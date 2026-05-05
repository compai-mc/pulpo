"""
POner seguridad en un backend

from pulpo.auth.back import verify_token


app = FastAPI(
    title="Motor Contable",
    lifespan=lifespan,
    dependencies=[Depends(verify_token)] # para que se llame a verify_token en todos las funciones
    )


async def health(
    full: Optional[bool] = Query(default=False),
    token_data=Depends(verify_token)  # Hay que poner esto SOLO si se quiere usar el token (gestionar permisos)
):


"""

import jwt
from jwt import PyJWKClient
from functools import lru_cache
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime

from pulpo.util.util import require_env
from pulpo.logueador import log
from pulpo.auth.general import Auth, set_user_token, get_service_token

# ==========================
# 🔧 LOG CONFIG
# ==========================
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
log.set_log_file(f"log/pulpo[{log_time}].log")
log.set_log_level(require_env("log_level"))

KEYCLOAK_URL = require_env("SEC_KEYCLOAK_URL")
REALM = require_env("SEC_REALM")
CLIENT_ID_FRONT = require_env("CLIENT_ID_FRONT")
APP_ENV = require_env("APP_ENV")

try:
    DEV_API_KEY = require_env("DEV_API_KEY")
except Exception:
    DEV_API_KEY = None

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
    credentials: HTTPAuthorizationCredentials = Security(
        bearer_scheme
    ),
) -> dict:

    incoming_token = credentials.credentials
    expected_issuer = (
        f"{KEYCLOAK_URL}/realms/{REALM}"
    )

    # ==========================
    # 🚀 MODO DEV: API KEY
    # ==========================
    if (
        APP_ENV == "dev"
        and incoming_token == DEV_API_KEY
    ):

        log.warning(
            "🔓 Acceso con API KEY "
            "(modo desarrollo)"
        )

        set_user_token(DEV_API_KEY)

        return {
            "preferred_username": "dev_user",
            "realm_access": {
                "roles": ["admin"]
            },
            "azp": CLIENT_ID_FRONT,
            "dev_mode": True,
        }

    # ==========================
    # 🔐 VALIDAR TOKEN ENTRANTE
    # ==========================
    try:

        signing_key = (
            _get_jwks_client()
            .get_signing_key_from_jwt(
                incoming_token
            )
        )

        decoded: dict = jwt.decode(
            incoming_token,
            signing_key.key,
            algorithms=["RS256"],
            issuer=expected_issuer,
            options={
                "verify_exp": True,
                "verify_aud": False,
            },
        )

    except jwt.ExpiredSignatureError:

        log.warning("Token expirado")

        raise HTTPException(
            status_code=(
                status.HTTP_401_UNAUTHORIZED
            ),
            detail="Token expirado.",
            headers={
                "WWW-Authenticate":
                "Bearer"
            },
        )

    except jwt.InvalidTokenError as e:

        log.warning(
            f"Token invalido: {e}"
        )

        raise HTTPException(
            status_code=(
                status.HTTP_401_UNAUTHORIZED
            ),
            detail=(
                f"Token inválido: {e}"
            ),
            headers={
                "WWW-Authenticate":
                "Bearer"
            },
        )

    # ==========================
    # 🎯 VALIDAR DESTINO (AUD)
    # ==========================
    try:

        service_token = get_service_token()

        service_claims = jwt.decode(
            service_token,
            options={
                "verify_signature":
                False
            },
        )

        expected_client_id = (
            service_claims.get("azp")
        )

        if not expected_client_id:
            log.error(
                "El token de servicio no contiene 'azp'"
            )
            raise RuntimeError(
                "El token de servicio no contiene azp"
            )

    except Exception as e:

        log.error(
            "No se pudo obtener "
            "la identidad del servicio: "
            f"{e}"
        )

        raise HTTPException(
            status_code=(
                status.HTTP_500_INTERNAL_SERVER_ERROR
            ),
            detail=(
                "Servicio mal inicializado "
                "(falta login_service)."
            ),
        )

    aud = decoded.get("aud") or []

    if isinstance(aud, str):
        aud = [aud]

    if expected_client_id not in aud:

        log.warning(
            "Token no destinado "
            f"a este servicio "
            f"(esperado: "
            f"'{expected_client_id}', "
            f"aud recibido: {aud})"
        )

        raise HTTPException(
            status_code=(
                status.HTTP_403_FORBIDDEN
            ),
            detail=(
                "Token no destinado "
                f"a este servicio "
                f"(esperado: "
                f"'{expected_client_id}', "
                f"aud recibido: {aud})."
            ),
        )

    # ==========================
    # ✅ OK
    # ==========================
    log.info(
        f"Token válido para usuario "
        f"'{decoded.get('preferred_username')}' "
        f"con roles: "
        f"{decoded.get('realm_access', {}).get('roles', [])}"
    )

    # Guardamos el token entrante
    # para siguientes exchanges
    set_user_token(incoming_token)

    return decoded


# ============================================================
# 📌 Pruebas
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
        auth = Auth(base_url=KEYCLOAK_URL, realm=REALM, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

        otp = input("🔢 Código OTP: ")
        token_response = auth.login(USERNAME, PASSWORD, otp)

        print("✅ Login exitoso")
        print(f"   Usuario: {token_response['decoded_token']['preferred_username']}")
        print(f"   Expira en: {token_response['expires_in']}s")
        print(f"   Token: {token_response['access_token'][:50]}...")

        print("\n🔄 PASO 4: Token Exchange (mismo cliente)")
        print("-" * 70)
        auth2 = Auth(base_url=KEYCLOAK_URL, realm=REALM, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        token_response2 = auth2.exchange_token_from(auth)

        print("✅ Token exchange exitoso")
        print(f"   Nuevo token: {token_response2['access_token'][:50]}...")

        # etc...
    except Exception as e:
        print(f"\n❌ ERROR: {e}")