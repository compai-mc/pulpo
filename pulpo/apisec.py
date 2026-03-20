import jwt
import requests
from functools import lru_cache
from fastapi import Request, HTTPException
from util.util import require_env

# ========== CONFIGURACIÓN ==========
KEYCLOAK_URL = require_env("SEC_KEYCLOAK_URL")  # https://seguridad.merocomsolutions.com"
REALM = require_env("SEC_REALM")                # CompAI
EXPECTED_AUD = require_env("SEC_AUDIENCE")      # "Forecast"  # client_id en Keycloak

# ========== JWKS PARA VERIFICAR FIRMA ==========
@lru_cache(maxsize=1)
def _get_jwks():
    """Obtiene las claves públicas de Keycloak para verificar firma."""
    url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/certs"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()

def _verify_ms_token(raw_token: str) -> dict:
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
        jwks = _get_jwks()
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
        if EXPECTED_AUD not in aud:
            raise HTTPException(
                status_code=403,
                detail=f"No tienes permiso para acceder a Forecast (aud no contiene '{EXPECTED_AUD}')"
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
def get_current_user(request: Request) -> dict:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Falta Authorization: Bearer <token>")

    token = auth.split(" ", 1)[1]
    return _verify_ms_token(token)
 