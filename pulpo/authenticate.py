import requests
import jwt
from jwt import PyJWKClient
import time
from cryptography.x509 import load_pem_x509_certificate
from cryptography.hazmat.backends import default_backend
from functools import lru_cache
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime
from contextvars import ContextVar

from pulpo.util.util import require_env
from pulpo.logueador import log

# ==========================
# 🔧 LOG CONFIG
# ==========================
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
log.set_log_file(f"log/pulpo[{log_time}].log")
log.set_log_level(require_env("log_level"))

# ==========================
# 🔐 CONFIGURACIÓN GLOBAL
# ==========================
KEYCLOAK_URL = require_env("SEC_KEYCLOAK_URL")
REALM = require_env("SEC_REALM")
CLIENT_ID_FRONT = require_env("CLIENT_ID_FRONT")
CLIENT_SECRET_FRONT = require_env("CLIENT_SECRET_FRONT")

bearer_scheme = HTTPBearer(auto_error=True)



"""
Generacion de token

TOKEN=$(curl -s -X POST \
  "https://seguridad.merocomsolutions.com/realms/CompAI/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=Contabilidad" \
  -d "client_secret=ZTp7gW3N3krrlP9xzPVJz3k0e9ogYVmt" \
  -d "username=pepe" \
  -d "password=pepe" \
  -d "totp=474624" | jq -r '.access_token')

echo $TOKEN

Ejecuta esto en alcazar por cada uno de los micros para los que necesites un token de acceso 
    te dará un token que tiene 730 días de caducidad 
 
RESPONSE=$(curl -s -X POST \
  "https://seguridad.merocomsolutions.com/realms/CompAI/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=password" \
  -d "client_id=Contabilidad" \
  -d "client_secret=ZTp7gW3N3krrlP9xzPVJz3k0e9ogYVmt" \
  -d "username=pepe" \
  -d "password=pepe" \
  -d "totp=474624")

ACCESS=$(echo "$RESPONSE" | jq -r '.access_token')
REFRESH=$(echo "$RESPONSE" | jq -r '.refresh_token')
REFRESH_EXP=$(echo "$RESPONSE" | jq -r '.refresh_expires_in')

echo $ACCESS
echo
echo $REFRESH
echo
echo $REFRESH_EXP
    

 """



# ======================================
# CACHE GLOBAL TOKENS 
# =====================================

# Tokens inter-micro tras un exchange, evita pedir tokens cada vez
_micro_token_cache = {} 

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

# ==========================
# 🔑 JWKS CLIENT (CACHEADO)
# ==========================
@lru_cache(maxsize=1)
def _get_jwks_client() -> PyJWKClient:
    return PyJWKClient(
        f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/certs",
        cache_keys=True,
    )

# ==========================
# ✔ VALIDACIÓN DE TOKENS (PARA MICROS)
# ==========================
async def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
) -> dict:

    token = credentials.credentials
    expected_issuer = f"{KEYCLOAK_URL}/realms/{REALM}"

    try:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        decoded: dict = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=expected_issuer,
            options={"verify_exp": True, "verify_aud": False},
        )

    except jwt.ExpiredSignatureError:
        log.warning("Token expirado")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except jwt.InvalidTokenError as e:
        log.warning(f"Token invalido {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token inválido: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if decoded.get("azp") != CLIENT_ID_FRONT:
        log.warning(
            f"Token no destinado a este servicio (azp recibido: '{decoded.get('azp')}')"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token no destinado a este servicio (azp recibido: '{decoded.get('azp')}').",
        )

    log.info(
        f"Token válido para usuario '{decoded.get('preferred_username')}' "
        f"con roles: {decoded.get('realm_access', {}).get('roles', [])}"
    )

    set_user_token(token)

    return decoded

# ==========================
# 🔄 TOKEN EXCHANGE + CACHÉ
# ==========================
def get_token_exchange(target_client_id, target_client_secret, subject_token):

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


# ============================================================
# 📌 DEMO (Opción B: se mantiene dentro del fichero)
# ============================================================
if __name__ == "__main__":

    print("🔐 EJEMPLO DE USO DE LA CLASE AUTH")
    print("=" * 70)

    KEYCLOAK_URL = "https://seguridad.merocomsolutions.com"
    REALM = "master"
    CLIENT_ID = "informes"
    CLIENT_SECRET = "M5DaowmNZJR4t6MrFxX27Y7CNTyxR0bC"
    USERNAME = "dos"
    PASSWORD = "dos"

    try:
        print("\n📱 PASO 1: Login con OTP")
        print("-" * 70)
        auth = Auth(KEYCLOAK_URL, REALM, CLIENT_ID, CLIENT_SECRET)

        otp = input("🔢 Código OTP: ")
        token_response = auth.login(USERNAME, PASSWORD, otp)

        print("✅ Login exitoso")
        print(f"   Usuario: {token_response['decoded_token']['preferred_username']}")
        print(f"   Expira en: {token_response['expires_in']}s")
        print(f"   Token: {token_response['access_token'][:50]}...")

        print("\n🔄 PASO 4: Token Exchange (mismo cliente)")
        print("-" * 70)
        auth2 = Auth(KEYCLOAK_URL, REALM, CLIENT_ID, CLIENT_SECRET)
        token_response2 = auth2.exchange_token_from(auth)

        print("✅ Token exchange exitoso")
        print(f"   Nuevo token: {token_response2['access_token'][:50]}...")

        # etc...
    except Exception as e:
        print(f"\n❌ ERROR: {e}")