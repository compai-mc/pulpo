# pulpo/tokens.py
import time
import httpx
import requests
import jwt
from contextvars import ContextVar

from pulpo.logueador import log
from pulpo.util.util import require_env

# ==========================
# 🔐 CONFIGURACIÓN GLOBAL
# ==========================
KEYCLOAK_URL = require_env("SEC_KEYCLOAK_URL")
REALM = require_env("SEC_REALM")
CLIENT_ID_FRONT = require_env("CLIENT_ID_FRONT")
CLIENT_SECRET_FRONT = require_env("CLIENT_SECRET_FRONT")
DEV_API_KEY = require_env("DEV_API_KEY")

# Token del usuario actual, necesario para no pasarlo por parámetros
_user_token_var = ContextVar("user_token", default=None)
_user_token_decoded = ContextVar("user_token_decoded", default=None)

# Funciones para manipular el token de usuario
def set_user_token(token: str, decoded = None):
    _user_token_var.set(token)
    _user_token_decoded.set(decoded)

def get_user_token() -> str:
    token = _user_token_var.get()
    if not token:
        log.warning("No hay token en el contexto de usuario")
        raise RuntimeError("No hay token en el contexto de usuario")
    return token


# Tokens inter-micro tras un exchange, evita pedir tokens cada vez
_micro_token_cache = {} 

# ==========================
# 🔄 TOKEN EXCHANGE + CACHÉ
# ==========================
def get_token_exchange(target_client_id, target_client_secret, subject_token):

    if subject_token == DEV_API_KEY:
        return DEV_API_KEY

    key = f"{target_client_id}:{target_client_secret}:{subject_token}"
    now = time.time()

    # ✔ Token cacheado
    cached = _micro_token_cache.get(key)
    if cached and now < cached["exp"] - 5:
        return cached["token"]

    # ✔ Token nuevo
    auth_origen = Auth(KEYCLOAK_URL, REALM, CLIENT_ID_FRONT, CLIENT_SECRET_FRONT)
    auth_origen.token = subject_token

    auth_destino = Auth(KEYCLOAK_URL, REALM, target_client_id, target_client_secret)
    result = auth_destino.exchange_token_from(auth_origen)

    new_token = result["access_token"]
    exp = now + result.get("expires_in", 60)

    _micro_token_cache[key] = {"token": new_token, "exp": exp}
    return new_token



class MicroTokenManager:
    def __init__(self, client_id, client_secret, get_user_token_func):
        self.client_id = client_id
        self.client_secret = client_secret
        self.get_user_token_func = get_user_token_func
        self.cached = None

    def _expired(self):
        return not self.cached or time.time() > self.cached["exp"] - 10

    def get_token(self):
        if self._expired():
            self.refresh()
        return self.cached["token"]

    def refresh(self):
        user_token = self.get_user_token_func()
        result = get_token_exchange(
            self.client_id,
            self.client_secret,
            user_token
        )
        self.cached = {
            "token": result,
            "exp": time.time() + 60  # o result.expires_in si lo devuelves
        }
        return self.cached["token"]
    

############################################
#   Cliente http genérico con reintentos
############################################

class MicroHttpClient:
    def __init__(self, token_manager):
        self.tm = token_manager

    # ----------------------------
    # 🔧 Core request (SIN CAMBIOS)
    # ----------------------------
    def _request(self, method, url, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.tm.get_token()}"

        resp = httpx.request(method, url, headers=headers, **kwargs)

        # Si el token caducó → refrescamos y reintentamos
        if resp.status_code == 401:
            headers["Authorization"] = f"Bearer {self.tm.refresh()}"
            resp = httpx.request(method, url, headers=headers, **kwargs)

        resp.raise_for_status()
        return resp

    # ----------------------------
    # 📌 Helpers amigables
    # ----------------------------
    def get(self, url, params=None, **kwargs):
        return self._request("GET", url, params=params, **kwargs).json()

    def post(self, url, json=None, **kwargs):
        return self._request("POST", url, json=json, **kwargs).json()

    def put(self, url, json=None, **kwargs):
        return self._request("PUT", url, json=json, **kwargs).json()

    def delete(self, url, **kwargs):
        r = self._request("DELETE", url, **kwargs)
        if r.status_code == 204:
            return None
        return r.json()

    def post_file(self, url, file_path, content_type="application/json"):
        with open(file_path, "rb") as f:
            files = {"file": (file_path, f, content_type)}
            return self.request("POST", url, files=files)
    

# ==========================
# 🔐 CLASE AUTH (COMPLETA, SIN CAMBIOS FUNCIONALES)
# ==========================
class Auth:
    def __init__(self, base_url, realm, client_id, client_secret):
        self.base_url = base_url
        self.realm = realm
        self.token_url = f"{base_url}/realms/{realm}/protocol/openid-connect/token"
        self.cert_url = f"{base_url}/realms/{realm}/protocol/openid-connect/certs"
        self.userinfo_url = f"{base_url}/realms/{realm}/protocol/openid-connect/userinfo"
        self.logout_url = f"{base_url}/realms/{realm}/protocol/openid-connect/logout"
        self.client_id = client_id
        self.client_secret = client_secret
        self.decoded_token = ""
        self.token = ""
        self.refresh_token = ""
        self.public_key = None
        self.otp = None

    # ==========================
    # LOGIN
    # ==========================
    def login(self, username, password, otp_code=None):
        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": username,
            "password": password,
            "scope": "openid profile email",
        }

        if otp_code:
            data["totp"] = otp_code
            self.otp = otp_code

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)

        if response.status_code != 200:
            log.error(f"Error {response.status_code}: {response.text}")
            raise Exception(f"Error {response.status_code}: {response.text}")

        token_data = response.json()

        self.token = token_data.get("access_token")
        self.refresh_token = token_data.get("refresh_token")
        self.decoded_token = self.__decode_jwt(self.token)

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
            "refresh_token": self.refresh_token,
            "expires_in": token_data.get("expires_in"),
            "token_type": token_data.get("token_type", "Bearer"),
        }

    # ==========================
    # REFRESH
    # ==========================
    def refresh_access_token(self):
        
        if not self.refresh_token:
            log.error("No hay refresh token disponible para refrescar el access token.")
            raise ValueError("Debe hacer login primero (no hay refresh token).")

        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)

        if response.status_code != 200:
            log.error(f"Error al refrescar token {response.status_code}: {response.text}")
            raise Exception(f"Error al refrescar token {response.status_code}: {response.text}")

        token_data = response.json()

        self.token = token_data.get("access_token")
        self.refresh_token = token_data.get("refresh_token")
        self.decoded_token = self.__decode_jwt(self.token)

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
            "refresh_token": self.refresh_token,
            "expires_in": token_data.get("expires_in"),
        }

    # ==========================
    # LOGOUT
    # ==========================
    def logout(self):

        if not self.refresh_token:
            self.token = ""
            self.decoded_token = ""
            return True

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.logout_url, data=data, headers=headers)

        self.token = ""
        self.refresh_token = ""
        self.decoded_token = ""
        self.otp = None

        return response.status_code == 204

    # ==========================
    # USERINFO
    # ==========================
    def get_userinfo(self):
        if not self.token:
            raise ValueError("No hay token disponible.")

        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(self.userinfo_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Error al obtener userinfo {response.status_code}: {response.text}")

        return response.json()

    # ==========================
    # TOKEN EXPIRADO
    # ==========================
    def is_token_expired(self, margin_seconds=30):
        if not self.decoded_token:
            return True

        exp = self.decoded_token.get("exp", 0)
        return exp < (time.time() + margin_seconds)

    # ==========================
    # DECODIFICAR JWT (sin firma)
    # ==========================
    def __decode_jwt(self, token):
        try:
            if isinstance(token, dict):
                token = token.get("access_token", "")
            return jwt.decode(str(token), options={"verify_signature": False})
        except Exception as e:
            raise Exception(f"Error al decodificar token: {str(e)}")

    # ==========================
    # TOKEN EXCHANGE
    # ==========================
    def exchange_token_from(self, source_auth):
        if not source_auth.token:
            log.error("Auth origen no tiene token para hacer exchange.")
            raise ValueError("Auth origen no tiene token.")

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": source_auth.token,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        }

        # Llamando a keycloak
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers)

        if response.status_code != 200:
            log.error(f"Error en token exchange {response.status_code}: {response.text}")
            raise RuntimeError(f"Error {response.status_code}: {response.text}")

        token_data = response.json()
        access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 0)

        if not access_token:
            log.error(f"No se recibió access_token en la respuesta de token exchange: {token_data}")
            raise ValueError("No se recibió access_token en la respuesta")

        self.token = access_token
        self.decoded_token = self.__decode_jwt(access_token)

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
            "expires_in": expires_in
        }

    def __repr__(self):
        return f"Auth(realm={self.realm}, client_id={self.client_id}, has_token={bool(self.token)})"
