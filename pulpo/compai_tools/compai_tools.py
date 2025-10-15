from datetime import date
from typing import List, Optional

from langroid.pydantic_v1 import BaseModel, Field
import langroid as lr
from langroid.agent.tools.orchestration import FinalResultTool

from .proxy_proccess_controler import ProccessControlerProxy
from .ejecucion_forecast import ejecucion_forecast


# --------------------------------------------------------------------
# 🔮 TOOL 1: Forecast energético
# --------------------------------------------------------------------

class ForecastRequest(BaseModel):
    fecha: Optional[str] = Field(None, description="Fecha en formato YYYY-MM-DD, opcional.")


class ForecastTool(lr.agent.ToolMessage):
    request: str = "get_forecast"
    purpose: str = "Para obtener el forecast energético en una fecha concreta."
    params: ForecastRequest

    @classmethod
    def examples(cls):
        return [
            cls(params=ForecastRequest(fecha="2025-10-15")),
            (
                "Quiero el forecast energético para hoy",
                cls(params=ForecastRequest(fecha=None)),
            ),
        ]

    async def handle(self) -> FinalResultTool:
        fecha = self.params.fecha or date.today().isoformat()
        forecast = await ejecucion_forecast(fecha)
        return FinalResultTool(
            info={
                "fecha": fecha,
                "resultado": forecast,
                "mode": "workflow"
            }
        )


# --------------------------------------------------------------------
# 🛒 TOOL 2: Búsqueda de productos similares
# --------------------------------------------------------------------

class ProductosRequest(BaseModel):
    productos: List[str] = Field(..., description="Lista de productos a buscar similares.")


class ProductosTool(lr.agent.ToolMessage):
    request: str = "find_similar_products"
    purpose: str = "Busca productos similares según la lista proporcionada."
    params: ProductosRequest

    @classmethod
    def examples(cls):
        return [
            cls(params=ProductosRequest(productos=["cable de red", "router"])),
            (
                "Encuentra productos parecidos a 'batería solar'",
                cls(params=ProductosRequest(productos=["batería solar"])),
            ),
        ]

    def handle(self) -> FinalResultTool:
        productos = self.params.productos
        if not productos:
            return FinalResultTool(
                info={
                    "productos": [],
                    "mensaje": "No encuentro nada relacionado con ese producto.",
                    "mode": "workflow"
                }
            )

        pc = ProccessControlerProxy()
        similitudes = pc.find_similar_producto(
            productos,
            numero_resultados=7,
            min_score=0.1
        )["results"]

        productos_filtrados = [
            {k: p[k] for k in ("ref", "label") if k in p}
            for p in similitudes
        ]

        return FinalResultTool(
            info={
                "productos": productos_filtrados,
                "mode": "online"
            }
        )


# --------------------------------------------------------------------
# 🔧 Registro en el agente
# --------------------------------------------------------------------

def register_tools(agent):
    """Activa las herramientas en un agente Langroid."""
    agent.enable_message(ForecastTool)
    agent.enable_message(ProductosTool)
