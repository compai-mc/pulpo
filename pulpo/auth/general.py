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
except RuntimeError:
    DEV_API_KEY = None


def _token_hint(token: str | None) -> str:
    if not token:
        return "<empty>"
    token = str(token)
    if token == DEV_API_KEY:
        return "<DEV_API_KEY>"
    return f"{token[:12]}...{token[-8:]} len={len(token)}"


def _safe_claims(token: str | None) -> dict:
    if not token or token == DEV_API_KEY:
        return {}
    try:
        claims = jwt.decode(str(token), options={"verify_signature": False})
    except Exception as exc:
        return {"decode_error": str(exc)}

    return {
        "iss": claims.get("iss"),
        "azp": claims.get("azp"),
        "aud": claims.get("aud"),
        "sub": claims.get("sub"),
        "preferred_username": claims.get("preferred_username"),
        "exp": claims.get("exp"),
    }


_user_token_var = ContextVar(
    "user_token",
    default=None
)

def set_user_token(token: str):
    log.debug(f"[auth] set_user_token token={_token_hint(token)} claims={_safe_claims(token)}")
    _user_token_var.set(token)


def get_user_token() -> str | None:
    return _user_token_var.get()


_service_token = None

def set_service_token(token: str):
    global _service_token
    _service_token = token
    log.info(f"[auth] set_service_token token={_token_hint(token)} claims={_safe_claims(token)}")


def get_service_token() -> str:

    if not _service_token:
        log.error("[auth] get_service_token fallido: no hay token de servicio en memoria")
        raise RuntimeError(
            "No hay token de servicio"
        )

    log.debug(f"[auth] get_service_token ok token={_token_hint(_service_token)}")
    return _service_token


# Tokens inter-micro tras un exchange, evita pedir tokens cada vez
_micro_token_cache = {} 

# ==========================
# 🔄 TOKEN EXCHANGE + CACHÉ
# ==========================
def get_token_exchange(target_client_id, target_client_secret, subject_token):

    now = time.time()
    log.info(
        "[auth] token_exchange solicitado "
        f"target_client_id={target_client_id} "
        f"subject={_token_hint(subject_token)} "
        f"subject_claims={_safe_claims(subject_token)}"
    )

    # Limpiando tokens caducados
    expired = [
        k for k, v in _micro_token_cache.items()
        if now >= v["exp"]
    ]

    for k in expired:
        log.debug(f"[auth] token_exchange cache expirado key_client_id={k[0]}")
        del _micro_token_cache[k]

    if subject_token == DEV_API_KEY:
        log.warning(
            "[auth] token_exchange usando DEV_API_KEY como token inter-micro "
            f"target_client_id={target_client_id}"
        )
        return {
            "access_token": DEV_API_KEY,
            "expires_in": 365 * 24 * 3600,
            "exp": now + 365 * 24 * 3600
        }
    
    key = (target_client_id, subject_token)

    cached = _micro_token_cache.get(key)
    if cached and now < cached["exp"] - 10:
        log.debug(
            "[auth] token_exchange cache hit "
            f"target_client_id={target_client_id} exp={cached['exp']}"
        )
        return cached

    log.info(f"[auth] token_exchange cache miss target_client_id={target_client_id}")

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
    log.info(
        "[auth] token_exchange ok "
        f"target_client_id={target_client_id} expires_in={expires_in} "
        f"token={_token_hint(cached['access_token'])} "
        f"claims={_safe_claims(cached['access_token'])}"
    )

    return cached



class MicroTokenManager:

    def __init__(self, client_id, client_secret):

        self.client_id = client_id
        self.client_secret = client_secret
        self.cached = None
        log.debug(f"[auth] MicroTokenManager inicializado client_id={client_id}")

    def _expired(self):

        return (
            not self.cached
            or time.time() > self.cached["exp"] - 10
        )
        

    def get_token(self):

        if self._expired():
            log.info(f"[auth] token inter-micro expirado/no existe; refrescando client_id={self.client_id}")
            self.refresh()
        else:
            log.debug(f"[auth] token inter-micro cache ok client_id={self.client_id}")

        return self.cached["token"]


    def refresh(self):
        log.info(f"[auth] MicroTokenManager.refresh inicio client_id={self.client_id}")

        token = get_token_exchange(
            self.client_id,
            self.client_secret,
            get_service_token()
        )

        self.cached = {
            "token": token["access_token"],
            "exp": token["exp"]
        }

        log.info(
            "[auth] MicroTokenManager.refresh ok "
            f"client_id={self.client_id} exp={token['exp']} "
            f"token={_token_hint(token['access_token'])}"
        )
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

        user_token = get_user_token()

        if user_token:
            headers["X-User-Token"] = user_token


        timeout = kwargs.pop("timeout", 30)
        log.info(
            "[auth-http] request "
            f"method={method} url={url} timeout={timeout} "
            f"target_client_id={self.tm.client_id} "
            f"params={kwargs.get('params')} "
            f"json_keys={list(kwargs.get('json', {}).keys()) if isinstance(kwargs.get('json'), dict) else None} "
            f"has_user_token={bool(user_token)}"
        )
        resp = httpx.request(method, url, headers=headers, timeout=timeout, **kwargs)
        log.info(
            "[auth-http] response "
            f"method={method} url={url} status={resp.status_code} "
            f"target_client_id={self.tm.client_id}"
        )

        # Si el token caducó → refrescamos y reintentamos
        if resp.status_code == 401:
            log.warning(
                "[auth-http] 401 recibido; refrescando token y reintentando "
                f"method={method} url={url} target_client_id={self.tm.client_id} "
                f"body={resp.text[:500]}"
            )
            self.tm.cached = None
            headers["Authorization"] = f"Bearer {self.tm.refresh()}"
            resp = httpx.request(method, url, headers=headers, timeout=timeout, **kwargs)
            log.info(
                "[auth-http] response retry "
                f"method={method} url={url} status={resp.status_code} "
                f"target_client_id={self.tm.client_id}"
            )

        if resp.status_code >= 400:
            log.error(
                "[auth-http] error response "
                f"method={method} url={url} status={resp.status_code} "
                f"target_client_id={self.tm.client_id} body={resp.text[:1000]}"
            )
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

    def patch(self, url, json=None, **kwargs):
        return self._request("PATCH", url, json=json, **kwargs).json()

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
        log.debug(
            "[auth] Auth inicializado "
            f"client_id={client_id} realm={realm} base_url={base_url}"
        )

    # ==========================
    # LOGIN
    # ==========================
    def login(self, username, password, otp_code=None):
        log.info(
            "[auth] login usuario inicio "
            f"client_id={self.client_id} username={username} has_otp={bool(otp_code)}"
        )
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
        log.info(
            "[auth] login usuario respuesta "
            f"client_id={self.client_id} status={response.status_code}"
        )

        if response.status_code != 200:
            log.error(f"[auth] login usuario error {response.status_code}: {response.text[:1000]}")
            raise Exception(f"Error {response.status_code}: {response.text}")

        token_data = response.json()

        self.token = token_data.get("access_token")
        self.refresh_token = token_data.get("refresh_token")
        self.decoded_token = self.__decode_jwt(self.token)
        log.info(
            "[auth] login usuario ok "
            f"client_id={self.client_id} username={self.decoded_token.get('preferred_username')} "
            f"azp={self.decoded_token.get('azp')} aud={self.decoded_token.get('aud')} "
            f"exp={self.decoded_token.get('exp')}"
        )

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
            "refresh_token": self.refresh_token,
            "expires_in": token_data.get("expires_in"),
            "refresh_expires_in": token_data.get(
                "refresh_expires_in"
            ),
            "token_type": token_data.get(
                "token_type",
                "Bearer",
            ),
        }

    
    def login_service(self):
        log.info(f"[auth] login_service inicio client_id={self.client_id}")

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
        log.info(
            "[auth] login_service respuesta "
            f"client_id={self.client_id} status={response.status_code}"
        )

        if response.status_code != 200:
            log.error(
                f"[auth] login_service error client_id={self.client_id} "
                f"status={response.status_code}: {response.text[:1000]}"
            )
            raise Exception(
                f"Error {response.status_code}: {response.text}"
            )

        token_data = response.json()

        self.token = token_data.get("access_token")
        self.decoded_token = self.__decode_jwt(self.token)
        log.info(
            "[auth] login_service ok "
            f"client_id={self.client_id} azp={self.decoded_token.get('azp')} "
            f"aud={self.decoded_token.get('aud')} exp={self.decoded_token.get('exp')} "
            f"token={_token_hint(self.token)}"
        )

        # Aqui ponemos el token recien creado
        set_service_token(self.token)

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
        log.info(f"[auth] refresh_access_token inicio client_id={self.client_id}")

        if not self.refresh_token:
            log.error(
                "No hay refresh token disponible "
                "para refrescar el access token."
            )

            raise ValueError(
                "Debe hacer login primero "
                "(no hay refresh token)."
            )

        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        headers = {
            "Content-Type":
            "application/x-www-form-urlencoded"
        }

        response = requests.post(
            self.token_url,
            data=data,
            headers=headers,
            timeout=10,
        )
        log.info(
            "[auth] refresh_access_token respuesta "
            f"client_id={self.client_id} status={response.status_code}"
        )

        if response.status_code != 200:

            log.error(
                f"[auth] Error al refrescar token "
                f"{response.status_code}: "
                f"{response.text[:1000]}"
            )

            raise Exception(
                f"Error al refrescar token "
                f"{response.status_code}: "
                f"{response.text}"
            )

        token_data = response.json()

        self.token = token_data.get(
            "access_token"
        )

        self.refresh_token = token_data.get(
            "refresh_token"
        )

        self.decoded_token = (
            self.__decode_jwt(
                self.token
            )
        )
        log.info(
            "[auth] refresh_access_token ok "
            f"client_id={self.client_id} azp={self.decoded_token.get('azp')} "
            f"aud={self.decoded_token.get('aud')} exp={self.decoded_token.get('exp')}"
        )

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
            "refresh_token": self.refresh_token,
            "expires_in": token_data.get(
                "expires_in"
            ),
            "refresh_expires_in": token_data.get(
                "refresh_expires_in"
            ),
        }
    

    # ==========================
    # LOGOUT
    # ==========================
    def logout(self):
        log.info(f"[auth] logout inicio client_id={self.client_id} has_refresh={bool(self.refresh_token)}")

        if not self.refresh_token:
            self.token = ""
            self.decoded_token = ""
            log.info(f"[auth] logout local ok client_id={self.client_id}")
            return True

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.logout_url, data=data, headers=headers, timeout=10)
        log.info(f"[auth] logout respuesta client_id={self.client_id} status={response.status_code}")

        self.token = ""
        self.refresh_token = ""
        self.decoded_token = ""
        self.otp = None

        return response.status_code == 204

    # ==========================
    # USERINFO
    # ==========================
    def get_userinfo(self):
        log.info(f"[auth] get_userinfo inicio client_id={self.client_id} has_token={bool(self.token)}")
        if not self.token:
            raise ValueError("No hay token disponible.")

        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(self.userinfo_url, headers=headers, timeout=10)

        if response.status_code != 200:
            log.error(
                f"[auth] get_userinfo error client_id={self.client_id} "
                f"status={response.status_code}: {response.text[:1000]}"
            )
            raise Exception(f"Error al obtener userinfo {response.status_code}: {response.text}")

        log.info(f"[auth] get_userinfo ok client_id={self.client_id}")
        return response.json()

    # ==========================
    # TOKEN EXPIRADO
    # ==========================
    def is_token_expired(self, margin_seconds=30):
        if not self.decoded_token:
            log.debug(f"[auth] is_token_expired=True client_id={self.client_id} motivo=sin_decoded_token")
            return True

        exp = self.decoded_token.get("exp", 0)
        expired = exp < (time.time() + margin_seconds)
        log.debug(
            f"[auth] is_token_expired={expired} client_id={self.client_id} "
            f"exp={exp} margin_seconds={margin_seconds}"
        )
        return expired

    # ==========================
    # DECODIFICAR JWT (sin firma)
    # ==========================
    def __decode_jwt(self, token):
        try:
            if isinstance(token, dict):
                token = token.get("access_token", "")
            decoded = jwt.decode(str(token), options={"verify_signature": False})
            log.debug(
                "[auth] decode_jwt ok "
                f"client_id={self.client_id} azp={decoded.get('azp')} "
                f"aud={decoded.get('aud')} exp={decoded.get('exp')}"
            )
            return decoded
        except Exception as e:
            log.error(
                "[auth] decode_jwt error "
                f"client_id={self.client_id} token={_token_hint(token)} error={e}"
            )
            raise Exception(f"Error al decodificar token: {str(e)}")

    # ==========================
    # TOKEN EXCHANGE
    # ==========================
    def exchange_token_from(self, subject_token: str):
        log.info(
            "[auth] exchange_token_from inicio "
            f"client_id={self.client_id} subject={_token_hint(subject_token)} "
            f"subject_claims={_safe_claims(subject_token)}"
        )

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
        log.info(
            "[auth] exchange_token_from respuesta "
            f"client_id={self.client_id} status={response.status_code}"
        )

        if response.status_code != 200:
            log.error(
                f"[auth] Error en token exchange client_id={self.client_id} "
                f"status={response.status_code}: {response.text[:1000]}"
            )
            raise RuntimeError(f"Error {response.status_code}: {response.text}")

        token_data = response.json()
        access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 0)

        if not access_token:
            log.error(f"No se recibió access_token en la respuesta de token exchange: {token_data}")
            raise ValueError("No se recibió access_token en la respuesta")

        self.token = access_token
        self.decoded_token = self.__decode_jwt(access_token)
        log.info(
            "[auth] exchange_token_from ok "
            f"client_id={self.client_id} azp={self.decoded_token.get('azp')} "
            f"aud={self.decoded_token.get('aud')} exp={self.decoded_token.get('exp')} "
            f"token={_token_hint(access_token)}"
        )

        return {
            "access_token": self.token,
            "decoded_token": self.decoded_token,
            "expires_in": expires_in
        }

    def __repr__(self):
        return f"Auth(realm={self.realm}, client_id={self.client_id}, has_token={bool(self.token)})"
