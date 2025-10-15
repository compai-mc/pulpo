from langroid.agent.tools import tool
from datetime import date

from proxy_proccess_controler import ProccessControlerProxy 
from ejecucion_forecast import ejecucion_forecast

class ForecastTool:
    @tool
    async def get_forecast(self, fecha: str = None) -> dict:
        """Forecast energético, el resultado va al workflow."""
        if not fecha:
            fecha = date.today().isoformat()

        forecast = await ejecucion_forecast(fecha)
        return {
            "fecha": fecha,
            "resultado": forecast,
            "mode": "workflow"  # 👈 indica que no se muestra al usuario directamente
        }


class ProductosTool:
    @tool
    async def find_similar_products(self, productos: list[str]) -> dict:
        """Busca productos similares y envía el resultado al workflow."""
        if not productos:
            return {
                "productos": [],
                "mensaje": "No encuentro nada relacionado con ese producto.",
                "mode": "workflow"
            }

        pc = ProccessControlerProxy()
        similitudes = pc.find_similar_producto(
            productos, numero_resultados=7, min_score=0.1
        )["results"]

        productos_filtrados = [
            {k: p[k] for k in ("ref", "label") if k in p}
            for p in similitudes
        ]

        return {
            "productos": productos_filtrados,
            "mode": "online"  # 👈 indica que se muestra al usuario directamente
        }