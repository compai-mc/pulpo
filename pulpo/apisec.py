import jwt
from jwt import PyJWKClient
import requests
from functools import lru_cache
from fastapi import Request, HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from pulpo.util.util import require_env

# ========== CONFIGURACIÓN ==========
KEYCLOAK_URL = require_env("SEC_KEYCLOAK_URL")              # https://seguridad.merocomsolutions.com"
REALM = require_env("SEC_REALM")                            # CompAI
KEYCLOAK_CLIENT_ID = require_env("KEYCLOAK_CLIENT_ID")      # "Forecast"  # client_id en Keycloak

bearer_scheme = HTTPBearer(auto_error=True)
_jwks_client: PyJWKClient | None = None

# ========== JWKS PARA VERIFICAR FIRMA ==========
@lru_cache(maxsize=1)
def _get_jwks_old():
    """Obtiene las claves públicas de Keycloak para verificar firma."""
    url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/certs"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def _verify_ms_token_old(raw_token: str) -> dict:
    """
    Valida el token:
    1. Verifica firma con JWKS de Keycloak
    2. Verifica exp
    3. Verifica iss
    4. Verifica aud == EXPECTED_AUD
    """
    if not raw_token:
        raise HTTPException(status_code=401, detail="Token vacío")

    try:
        # Obtener key id del header del token
        header = jwt.get_unverified_header(raw_token)
        kid = header.get("kid")

        # Buscar la clave correcta en JWKS
        jwks = _get_jwks_old()
        public_key = None
        for key in jwks.get("keys", []):
            if key.get("kid") == kid:
                public_key = jwt.algorithms.RSAAlgorithm.from_jwk(key)
                break

        if not public_key:
            raise HTTPException(status_code=401, detail="Clave pública no encontrada")

        # Decodificar y verificar firma
        decoded = jwt.decode(
            raw_token,
            key=public_key,
            algorithms=["RS256"],
            issuer=f"{KEYCLOAK_URL}/realms/{REALM}",
            options={
                "verify_exp": True,
                "verify_iss": True,
                "verify_aud": False  # la verificamos manualmente
            }
        )

        # Verificar audiencia manualmente
        aud = decoded.get("aud", [])
        if isinstance(aud, str):
            aud = [aud]
        if KEYCLOAK_CLIENT_ID not in aud:
            raise HTTPException(
                status_code=403,
                detail=f"No tienes permiso para acceder a Forecast (aud no contiene '{KEYCLOAK_CLIENT_ID}')"
            )

        return decoded

    except HTTPException:
        raise
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidIssuerError:
        raise HTTPException(status_code=401, detail="Issuer inválido")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token inválido: {str(e)}")

# ========== DEPENDENCY ==========
def get_current_user_old(request: Request) -> dict:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Falta Authorization: Bearer <token>")

    token = auth.split(" ", 1)[1]
    return _verify_ms_token_old(token)




"""
Generacion de token

curl -s -X POST "https://seguridad.merocomsolutions.com/realms/CompAI/protocol/openid-connect/token" \

  -d "grant_type=password" \

  -d "client_id=Front" \

  -d "client_secret=rT12EVq24Nm4pzcZDjyZKKQXER9O3PN1" \

  -d "username=pepe" \

  -d "password=pepe" \

  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 


ejecuta esta línea en alcazar por cada uno de los micros para los que necesites un token de acceso te dará un token que tiene 730 días de caducidad 
 
 """

@lru_cache(maxsize=1)
def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        _jwks_client = PyJWKClient(
            f"{KEYCLOAK_URL}/realms/{REALM}"
            "/protocol/openid-connect/certs",
            cache_keys=True,
        )
    return _jwks_client


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
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token inválido: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # El portal hace el exchange → el token llega con azp == client_id de este servicio
    if decoded.get("azp") != KEYCLOAK_CLIENT_ID:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                f"Token no destinado a este servicio "
                f"(azp recibido: '{decoded.get('azp')}')."
            ),
        )

    return decoded