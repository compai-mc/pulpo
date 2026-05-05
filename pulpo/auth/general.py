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

try:
    DEV_API_KEY = require_env("DEV_API_KEY")
except Exception:
    DEV_API_KEY = None

# Token del usuario actual, necesario para no pasarlo por parámetros
_user_token_var = ContextVar("user_token", default=None)

# Funciones para manipular el token de usuario
def set_user_token(token: str):
    _user_token_var.set(token)


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

    now = time.time()

    # Limpiando tokens caducados
    expired = [
        k for k, v in _micro_token_cache.items()
        if now >= v["exp"]
    ]

    for k in expired:
        del _micro_token_cache[k]

    if subject_token == DEV_API_KEY:
        return {
            "access_token": DEV_API_KEY,
            "expires_in": 365 * 24 * 3600,
            "exp": now + 365 * 24 * 3600
        }
    
    key = (target_client_id, subject_token)

    cached = _micro_token_cache.get(key)
    if cached and now < cached["exp"] - 10:
        return cached

    auth_destino = Auth(
        client_id=target_client_id,
        client_secret=target_client_secret
    )

    result = auth_destino.exchange_token_from(subject_token)

    expires_in = result.get("expires_in") or 300

    cached = {
        "access_token": result["access_token"],
        "expires_in": expires_in,
        "exp": now + expires_in
    }

    _micro_token_cache[key] = cached

    return cached



class MicroTokenManager:

    def __init__(self, client_id, client_secret):

        self.client_id = client_id
        self.client_secret = client_secret
        self.cached = None

    def _expired(self):

        return (
            not self.cached
            or time.time() > self.cached["exp"] - 10
        )
        

    def get_token(self):

        if self._expired():
            log.info("Token expirado o no existe, refrescando...")
            self.refresh()

        return self.cached["token"]

    def refresh(self):

        token = get_token_exchange(
            self.client_id,
            self.client_secret,
            get_user_token()
        )

        self.cached = {
            "token": token["access_token"],
            "exp": token["exp"]
        }

        return token["access_token"]


    

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

        headers = dict(kwargs.pop("headers", {}))
        headers["Authorization"] = f"Bearer {self.tm.get_token()}"
        resp = httpx.request(method, url, headers=headers, timeout=30, **kwargs)

        # Si el token caducó → refrescamos y reintentamos
        if resp.status_code == 401:
            self.tm.cached = None
            headers["Authorization"] = f"Bearer {self.tm.refresh()}"
            resp = httpx.request(method, url, headers=headers, timeout=30, **kwargs)

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
            return self._request(
                "POST",
                url,
                files=files
            )
                

# ==========================
# 🔐 CLASE AUTH (COMPLETA, SIN CAMBIOS FUNCIONALES)
# ==========================
class Auth:
    def __init__(self, client_id , client_secret , base_url = KEYCLOAK_URL, realm = REALM):
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
        response = requests.post(self.token_url, data=data, headers=headers, timeout=10)

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

    
    def login_service(self):

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        response = requests.post(
            self.token_url,
            data=data,
            headers=headers,
            timeout=10
        )

        if response.status_code != 200:
            log.error(
                f"Error {response.status_code}: {response.text}"
            )
            raise Exception(
                f"Error {response.status_code}: {response.text}"
            )

        token_data = response.json()

        self.token = token_data.get("access_token")
        self.decoded_token = self.__decode_jwt(self.token)

        # Aqui ponemos el token recien creado
        set_user_token(self.token)

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
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
        response = requests.post(self.token_url, data=data, headers=headers, timeout=10)

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
        response = requests.post(self.logout_url, data=data, headers=headers, timeout=10)

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
        response = requests.get(self.userinfo_url, headers=headers, timeout=10)

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
    def exchange_token_from(self, subject_token: str):

        if not subject_token:
            log.error("No hay subject_token para hacer exchange.")
            raise ValueError("No hay subject_token.")

        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": subject_token,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        }


        # Llamando a keycloak
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.token_url, data=data, headers=headers, timeout=10)

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
