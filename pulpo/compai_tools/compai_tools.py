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
            (
                "Usuario pidió 'forecast para hoy' (palabra clave: forecast), ejecuto ejecucion_forecast UNA VEZ",
                cls(params=ForecastRequest(fecha=None)),
            ),
            (
                "Cliente quiere 'informe de previsión del mes pasado' (sinónimo de forecast), ejecuto herramienta UNA VEZ",
                cls(params=ForecastRequest(fecha="2025-01-16")),
            ),
            (
                "Usuario mencionó 'previsión mañana' (palabras clave: previsión + fecha), ejecuto automáticamente UNA VEZ",
                cls(params=ForecastRequest(fecha="2025-01-16")),
            ),
        ]

    def handle(self) -> FinalResultTool:
        fecha = self.params.fecha or date.today().isoformat()
        forecast = ejecucion_forecast(fecha)
        return FinalResultTool(
            info={
                "fecha": fecha,
                "respuesta": "Te envio una tarea al workflow con el documento solicitado.",
                "mode": "workflow",
                "status": "completado",
                "reejecutar": False 
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
            (
                "Usuario mencionó 'ONT' (producto tecnológico), ejecuto find_similar_products",
                cls(params=ProductosRequest(productos=["ONT"])),
            ),
            (
                "Cliente preguntó por 'router Cisco' (palabra clave: router), ejecuto herramienta",
                cls(params=ProductosRequest(productos=["router Cisco"])),
            ),
            (
                "Usuario dijo 'necesito cables KP' (producto tecnológico), ejecuto automáticamente",
                cls(params=ProductosRequest(productos=["baterías solares"])),
            ),
            (
                "Usuario pidió 'alternativas a switch' (sinónimo de productos similares), ejecuto tool",
                cls(params=ProductosRequest(productos=["switch"])),
            ),
        ]

    def handle(self) -> FinalResultTool:
        productos = self.params.productos
        if not productos:
            return FinalResultTool(
                info={
                    "respuesta": [],
                    "mensaje": "No encuentro nada relacionado con ese producto.",
                    "mode": "online",
                    "status": "completado",
                    "reejecutar": False 
                }
            )

        pc = ProccessControlerProxy()
        similitudes = pc.similarity_product(
            query = productos,
            codigo = "",
            numero_resultados=7,
            min_score=0.1
        )["results"]

        productos_filtrados = [
            {k: p[k] for k in ("ref", "label") if k in p}
            for p in similitudes
        ]

        return FinalResultTool(
            info={
                "respuesta": productos_filtrados,
                "mode": "online"
            }
        )


# --------------------------------------------------------------------
# 🔧 Registro en el agente
# --------------------------------------------------------------------

def register_all_tools(agent):
    """Activa las herramientas en un agente Langroid."""
    agent.enable_message(ForecastTool)
    agent.enable_message(ProductosTool)

def register_tool(agent, tool):
    agent.enable_message(tool)
