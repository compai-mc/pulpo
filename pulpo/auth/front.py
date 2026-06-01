

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


def _token_hint(token: str | None) -> str:
    if not token:
        return "<empty>"
    token = str(token)
    return f"{token[:12]}...{token[-8:]} len={len(token)}"


# ===========================
# 👤 LOGIN FRONT
# ===========================
def front_login(
    username: str,
    password: str,
    otp_code=None,
):

    global _front_tokens
    log.info(
        "[auth-front] front_login inicio "
        f"client_id={CLIENT_ID_FRONT} username={username} has_otp={bool(otp_code)}"
    )

    auth = Auth(
        base_url=KEYCLOAK_URL,
        realm=REALM,
        client_id=CLIENT_ID_FRONT,
        client_secret=CLIENT_SECRET_FRONT,
    )

    result = auth.login(
        username,
        password,
        otp_code,
    )
    log.info(
        "[auth-front] front_login ok "
        f"client_id={CLIENT_ID_FRONT} username={username} "
        f"access_token={_token_hint(result.get('access_token'))} "
        f"expires_in={result.get('expires_in')} "
        f"refresh_expires_in={result.get('refresh_expires_in')}"
    )

    _front_tokens["access_token"] = (
        result["access_token"]
    )

    _front_tokens["refresh_token"] = (
        result["refresh_token"]
    )

    _front_tokens["access_exp"] = (
        time.time()
        + result["expires_in"]
    )

    _front_tokens["refresh_exp"] = (
        time.time()
        + result["refresh_expires_in"]
    )

    # dejamos el token disponible
    # para llamadas inmediatas
    set_user_token(
        result["access_token"]
    )

    return result


# ===========================
# 🔄 REFRESH FRONT
# ===========================
def front_refresh_tokens():

    global _front_tokens
    log.info(
        "[auth-front] front_refresh_tokens inicio "
        f"client_id={CLIENT_ID_FRONT} has_refresh={bool(_front_tokens['refresh_token'])}"
    )

    if not _front_tokens["refresh_token"]:

        log.error(
            "No hay refresh token disponible "
            "para hacer refresh. "
            "Debes hacer login."
        )

        raise RuntimeError(
            "No hay refresh token. "
            "Debe hacer login."
        )

    auth = Auth(
        base_url=KEYCLOAK_URL,
        realm=REALM,
        client_id=CLIENT_ID_FRONT,
        client_secret=CLIENT_SECRET_FRONT,
    )

    auth.refresh_token = (
        _front_tokens["refresh_token"]
    )

    result = auth.refresh_access_token()
    log.info(
        "[auth-front] front_refresh_tokens ok "
        f"client_id={CLIENT_ID_FRONT} "
        f"access_token={_token_hint(result.get('access_token'))} "
        f"expires_in={result.get('expires_in')} "
        f"refresh_expires_in={result.get('refresh_expires_in')}"
    )

    _front_tokens["access_token"] = (
        result["access_token"]
    )

    _front_tokens["refresh_token"] = (
        result["refresh_token"]
    )

    _front_tokens["access_exp"] = (
        time.time()
        + result["expires_in"]
    )

    _front_tokens["refresh_exp"] = (
        time.time()
        + result["refresh_expires_in"]
    )

    return result


# ===========================
# ✔ TOKEN VÁLIDO
# ===========================
def get_valid_access_token(margin_seconds=30):
    global _front_tokens

    now = time.time()
    log.debug(
        "[auth-front] get_valid_access_token inicio "
        f"has_access={bool(_front_tokens['access_token'])} "
        f"access_exp={_front_tokens['access_exp']} "
        f"refresh_exp={_front_tokens['refresh_exp']} "
        f"margin_seconds={margin_seconds}"
    )

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

        log.info("[auth-front] access token cerca de caducar; refrescando")
        nuevo_token = front_refresh_tokens()["access_token"]
        set_user_token(nuevo_token)
        return nuevo_token

    set_user_token(access_token)
    log.debug(f"[auth-front] access token reutilizado token={_token_hint(access_token)}")
    return access_token
