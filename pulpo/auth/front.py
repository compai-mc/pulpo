

"""
Así se llamaría desde un front:

from pulpo.auth.front import front_login, get_valid_access_token

# Login inicial
token = front_login("pepe", "pepe", otp_code="123456")



#  Instanciar el cliente de contabilidad como siempre
contabilidad = ContabilidadClient()

get_valid_access_token() # Antes de cada llamada:
respuesta = contabilidad.crear_cuenta_contable( ..... )

get_valid_access_token() # Antes de cada llamada:
respuesta = contabilidad.crear_balance_contable( ..... )

"""

import time

from pulpo.logueador import log
from pulpo.util.util import require_env
from pulpo.auth.general import set_user_token, Auth

# ==========================
# 🔐 CONFIGURACIÓN GLOBAL
# ==========================
KEYCLOAK_URL = require_env("SEC_KEYCLOAK_URL")
REALM = require_env("SEC_REALM")
CLIENT_ID_FRONT = require_env("CLIENT_ID_FRONT")
CLIENT_SECRET_FRONT = require_env("CLIENT_SECRET_FRONT")

# ===========================
# 🔐 Estado global del FRONT
# ===========================
_front_tokens = {
    "access_token": None,
    "refresh_token": None,
    "access_exp": 0,
    "refresh_exp": 0,
}

# ===========================
# 👤 LOGIN FRONT
# ===========================
def front_login(username: str, password: str, otp_code=None):
    global _front_tokens

    auth = Auth(KEYCLOAK_URL, REALM, CLIENT_ID_FRONT, CLIENT_SECRET_FRONT)
    result = auth.login(username, password, otp_code)

    # Guardamos todo
    _front_tokens["access_token"] = result["access_token"]
    _front_tokens["refresh_token"] = result["refresh_token"]
    _front_tokens["access_exp"] = time.time() + result["expires_in"]
    # Keycloak no envía refresh_expires_in en tu login. Lo obtenemos del token decodificado
    _front_tokens["refresh_exp"] = result["decoded_token"].get("exp", 0) + 1800


    return result


# ===========================
# 🔄 REFRESH FRONT
# ===========================
def front_refresh_tokens():
    global _front_tokens

    if not _front_tokens["refresh_token"]:
        log.error("No hay refresh token disponible para hacer refresh. Debes hacer login.")
        raise RuntimeError("No hay refresh token, debe hacer login.")

    auth = Auth(KEYCLOAK_URL, REALM, CLIENT_ID_FRONT, CLIENT_SECRET_FRONT)
    auth.refresh_token = _front_tokens["refresh_token"]

    result = auth.refresh_access_token()

    _front_tokens["access_token"] = result["access_token"]
    _front_tokens["refresh_token"] = result["refresh_token"]
    _front_tokens["access_exp"] = time.time() + result["expires_in"]

    return result


# ===========================
# ✔ TOKEN VÁLIDO
# ===========================
def get_valid_access_token(margin_seconds=30):
    global _front_tokens

    now = time.time()

    # ❌ No tiene nada → debe hacer login
    if not _front_tokens["access_token"]:
        log.info("No hay token de usuario. Debes hacer login.")
        raise RuntimeError("No hay token de usuario. Debe hacer login.")

    # ✔ Si está caducado el refresh → login obligatorio
    if now > _front_tokens["refresh_exp"]:
        log.info("Refresh token caducado. Debe hacer login otra vez.")
        raise RuntimeError("Refresh token caducado. Debe hacer login otra vez.")
    
    access_token = _front_tokens["access_token"]

    # ✔ Si el access va a caducar → intentamos refresh
    if now > (_front_tokens["access_exp"] - margin_seconds):

        nuevo_token = front_refresh_tokens()["access_token"]
        set_user_token(nuevo_token)
        return nuevo_token

    set_user_token(access_token)
    return access_token