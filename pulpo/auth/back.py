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
from fastapi import HTTPException, Security, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime

from pulpo.util.util import require_env
from pulpo.logueador import log
from pulpo.auth.general import Auth, get_service_auth, set_user_token

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


def _token_hint(token: str | None) -> str:
    if not token:
        return "<empty>"
    token = str(token)
    if token == DEV_API_KEY:
        return "<DEV_API_KEY>"
    return f"{token[:12]}...{token[-8:]} len={len(token)}"


def _unsafe_claims(token: str | None) -> dict:
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
  -d "_id=Contabilidad" \
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
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(
        bearer_scheme
    ),
) -> dict:

    incoming_token = credentials.credentials
    expected_issuer = (
        f"{KEYCLOAK_URL}/realms/{REALM}"
    )
    user_token = request.headers.get(
        "X-User-Token"
    )
    incoming_claims_hint = _unsafe_claims(incoming_token)
    user_claims_hint = _unsafe_claims(user_token)

    log.info(
        "[auth-back] verify_token inicio "
        f"method={request.method} path={request.url.path} "
        f"query={request.url.query} app_env={APP_ENV} "
        f"incoming={_token_hint(incoming_token)} "
        f"incoming_claims={incoming_claims_hint} "
        f"has_user_token={bool(user_token)} "
        f"user_claims={user_claims_hint}"
    )

    # ==========================
    # 🚀 MODO DEV: API KEY
    # ==========================
    if (
        APP_ENV == "dev"
        and incoming_token == DEV_API_KEY
    ):

        log.warning(
            "[auth-back] DEV_API_KEY aceptada "
            f"method={request.method} path={request.url.path}"
        )
        
        set_user_token(DEV_API_KEY)
        
        return {
            "preferred_username": "dev_user",
            "realm_access": {"roles": ["admin"]},
            "azp": "dev",
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
        log.debug(
            "[auth-back] signing key obtenida "
            f"method={request.method} path={request.url.path}"
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
        log.info(
            "[auth-back] token entrante validado "
            f"method={request.method} path={request.url.path} "
            f"azp={decoded.get('azp')} aud={decoded.get('aud')} "
            f"sub={decoded.get('sub')} username={decoded.get('preferred_username')} "
            f"exp={decoded.get('exp')}"
        )

    except jwt.ExpiredSignatureError:

        log.warning(
            "[auth-back] Token expirado "
            f"method={request.method} path={request.url.path} "
            f"incoming_claims={incoming_claims_hint}"
        )

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
            "[auth-back] Token invalido "
            f"method={request.method} path={request.url.path} "
            f"error={e} incoming={_token_hint(incoming_token)} "
            f"incoming_claims={incoming_claims_hint}"
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

        service_id = get_service_auth().client_id  

    except Exception as e:

        log.error(
            "[auth-back] No se pudo obtener la identidad del servicio "
            f"method={request.method} path={request.url.path} error={e} "
            f"incoming_claims={incoming_claims_hint}"
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

    log.info(
        "[auth-back] validando audience "
        f"method={request.method} path={request.url.path} "
        f"expected={service_id} aud_recibido={aud}"
    )

    if service_id not in aud:
        log.warning(
            "[auth-back] Token no destinado a este servicio "
            f"method={request.method} path={request.url.path} "
            f"esperado={service_id} aud_recibido={aud} "
            f"azp={decoded.get('azp')} sub={decoded.get('sub')}"
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Token no destinado a este servicio (esperado: '{service_id}', aud recibido: {aud})")

    

    # ==========================
    # ✅ OK
    # ==========================
    log.info(
        f"Token válido para usuario "
        f"'{decoded.get('preferred_username')}' "
        f"con roles: "
        f"{decoded.get('realm_access', {}).get('roles', [])}"
    )

    log.info(
        "[auth-back] Token valido "
        f"method={request.method} path={request.url.path} "
        f"username={decoded.get('preferred_username')} "
        f"azp={decoded.get('azp')} aud={aud} "
        f"service_id={service_id} "
        f"roles={decoded.get('realm_access', {}).get('roles', [])}"
    )

    # ==========================
    # 🔄 USER TOKEN
    # ==========================
    if user_token:
        log.debug(
            "[auth-back] propagando X-User-Token existente "
            f"method={request.method} path={request.url.path}"
        )
        set_user_token(user_token)
    else:
        log.debug(
            "[auth-back] guardando token entrante como user token "
            f"method={request.method} path={request.url.path}"
        )
        set_user_token(incoming_token)


    # ==========================
    # ✅ OK
    # ==========================
    log.info(
        "[auth-back] verify_token ok "
        f"method={request.method} path={request.url.path}"
    )
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
        
        token_response2 = auth2.exchange_token_from(
            subject_token=token_response['access_token'], 
            target_client_id=CLIENT_ID)

        print("✅ Token exchange exitoso")
        print(f"   Nuevo token: {token_response2['access_token'][:50]}...")

        # etc...
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
