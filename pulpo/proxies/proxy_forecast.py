import httpx
from typing import Optional
from pulpo.util.util import require_env

FORECAST_URL = require_env("FORECAST_URL") 

def _float_env(var_name: str) -> float | None:
    """
    Devuelve el valor float de una variable de entorno, o None si no existe o no es válida.
    """
    value = require_env(var_name)
    if value is None:
        print(f"ℹ️ {var_name} no definida.")
        return None
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ Valor inválido para {var_name}: '{value}'.")
        return None

FORECAST_TIMEOUT_TOTAL = _float_env("FORECAST_TIMEOUT_TOTAL")
FORECAST_TIMEOUT_READ = _float_env("FORECAST_TIMEOUT_READ")

async def generar_forecast_minio(
    fecha: str,
    horizonte: Optional[int] = None,
    include_dashboard: Optional[bool] = None,
    include_alerts: Optional[bool] = None,
):
    """
    Llama al endpoint /generate-forecast-minio para generar el forecast en MinIO.

    Args:
        fecha (str): Fecha en formato DD/MM/YYYY o YYYY-MM-DD (obligatorio)
        horizonte (int | None): Meses de horizonte (opcional)
        include_dashboard (bool | None): Incluir dashboard (opcional)
        include_alerts (bool | None): Incluir alertas (opcional)

    Returns:
        dict: Respuesta JSON del servicio
    """
    url = f"{FORECAST_URL}/generate-forecast-minio"
    params = {"fecha": fecha}

    if horizonte is not None:
        params["horizonte"] = horizonte
    if include_dashboard is not None:
        params["include_dashboard"] = str(include_dashboard).lower()
    if include_alerts is not None:
        params["include_alerts"] = str(include_alerts).lower()

    timeout = httpx.Timeout(FORECAST_TIMEOUT_TOTAL, read=FORECAST_TIMEOUT_READ)


    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()