from pydantic import BaseModel, Field
from datetime import date
from proxy_proccess_controler import ProccessControlerProxy
from ejecucion_forecast import ejecucion_forecast

from langroid.agent.tool_message import tool
from typing import List, Optional  # 👈 Añadir Optional aquí

class ForecastTool:
    
    @tool
    async def get_forecast(self, fecha: Optional[str] = None) -> dict:  # 👈 Ahora Optional está definido
        """
        Forecast energético, el resultado va al workflow.
        
        Args:
            fecha: Fecha para el forecast en formato YYYY-MM-DD.
                   Si es None, usa la fecha actual.
        """
        if not fecha:
            fecha = date.today().isoformat()
            
        forecast = await ejecucion_forecast(fecha)
        return {
            "fecha": fecha,
            "resultado": forecast,
            "mode": "workflow"
        }
    

    @classmethod
    def register(cls, agent):
        """Registra esta herramienta en un agente."""
        agent.enable_message(cls().get_forecast)
                             

class ProductosTool:
    
    @tool
    async def find_similar_products(self, productos: List[str]) -> dict:
        """Busca productos similares y envía el resultado al workflow."""
        if not productos:
            return {
                "productos": [],
                "mensaje": "No encuentro nada relacionado con ese producto.",
                "mode": "workflow"
            }

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

        return {
            "productos": productos_filtrados,
            "mode": "online"  # 👈 indica que se muestra al usuario directamente
        }


    @classmethod
    def register(cls, agent):
        """Registra esta herramienta en un agente."""
        agent.enable_message(cls().find_similar_products)