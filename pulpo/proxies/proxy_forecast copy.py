import asyncio
from typing import Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

FORECAST_URL = require_env("FORECAST_URL")
CLIENT_ID = require_env("CLIENT_ID_FORECAST")
CLIENT_SECRET = require_env("CLIENT_SECRET_FORECAST")


def _float_env(var_name: str) -> float | None:
    """
    Devuelve el valor float de una variable de entorno, o None si no existe o no es valida.
    """
    value = require_env(var_name)
    try:
        return float(value)
    except ValueError:
        print(f"Valor invalido para {var_name}: '{value}'.")
        return None


FORECAST_TIMEOUT_TOTAL = _float_env("FORECAST_TIMEOUT_TOTAL")
FORECAST_TIMEOUT_READ = _float_env("FORECAST_TIMEOUT_READ")


class ForecastProxy:
    def __init__(self, base_url: str = FORECAST_URL):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }
        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.client = MicroHttpClient(self.tm)

    def _get(self, path: str, **kwargs):
        return self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            timeout=FORECAST_TIMEOUT_TOTAL or FORECAST_TIMEOUT_READ or 30,
            **kwargs
        )

    def generar_forecast_minio(
        self,
        fecha: str,
        horizonte: Optional[int] = None,
        include_dashboard: Optional[bool] = None,
        include_alerts: Optional[bool] = None,
    ):
        params = {"fecha": fecha}

        if horizonte is not None:
            params["horizonte"] = horizonte
        if include_dashboard is not None:
            params["include_dashboard"] = str(include_dashboard).lower()
        if include_alerts is not None:
            params["include_alerts"] = str(include_alerts).lower()

        return self._get("/generate-forecast-minio", params=params)


_default_proxy: ForecastProxy | None = None


def _proxy() -> ForecastProxy:
    global _default_proxy
    if _default_proxy is None:
        _default_proxy = ForecastProxy()
    return _default_proxy


async def generar_forecast_minio(
    fecha: str,
    horizonte: Optional[int] = None,
    include_dashboard: Optional[bool] = None,
    include_alerts: Optional[bool] = None,
):
    """
    Llama al endpoint /generate-forecast-minio para generar el forecast en MinIO.
    """
    return await asyncio.to_thread(
        _proxy().generar_forecast_minio,
        fecha,
        horizonte,
        include_dashboard,
        include_alerts
    )
