import jwt
from jwt import PyJWKClient
from functools import lru_cache
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime

from pulpo.util.util import require_env
from pulpo.logueador import log
from pulpo.auth.general import Auth, set_user_token


# ==========================
# 🔧 LOG CONFIG
# ==========================
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
log.set_log_file(f"log/pulpo[{log_time}].log")
log.set_log_level(require_env("log_level"))


CLIENT_ID_FRONT = require_env("CLIENT_ID_FRONT")

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