from datetime import date
from typing import List, Optional, Dict, Any, Type
import os

from langroid.pydantic_v1 import BaseModel, Field
import langroid as lr
from langroid.agent.tools.orchestration import FinalResultTool
from langroid.agent import ToolMessage

from .proxy_proccess_controler import ProccessControlerProxy
from .ejecucion_forecast import ejecucion_forecast
from .proposal import ProposalMicroserviceClient
from .proxy_correo import CorreoClient
from .manager_historia import HistoriaManager
from .proxy_erpdolibarr_sincrono import ERPProxySincrono

from pulpo.util.util import require_env
from pulpo.util.esquema import CompaiMessage, Attachments, Attachment

URL_ERP = require_env("ERPDOLIBARR_URL")
URL_PROPOSAL = require_env("URL_PROPOSAL")
URL_CORREO = require_env("URL_CORREO")

class BrownDispatcher():
    def __init__(self, tools: dict[str, Type[lr.agent.ToolMessage]]):
        super().__init__()
        self.tools = tools

    def handle_message(self, mensaje):
        estados = mensaje.estado or []
        print(f"📦 Estados detectados: {estados}")

        # Buscar la primera tool aplicable
        for estado in estados:
            if estado in self.tools:
                ToolClass = self.tools[estado]
                print(f"⚙️ Ejecutando Tool: {ToolClass.__name__} para estado {estado}")
                tool = ToolClass(params=mensaje)
                return tool.handle()

        print("❌ Ninguna Tool aplicable a los estados recibidos.")
        return None

# --------------------------------------------------------------------
# 🔮 TOOL 1: Forecast 
# --------------------------------------------------------------------
class ForecastRequest(BaseModel):
    fecha: Optional[str] = Field(None, description="Fecha en formato YYYY-MM-DD, opcional.")


class ForecastTool(lr.agent.ToolMessage):
    request: str = "get_forecast"
    purpose: str = "Para obtener el forecast de los productos en una fecha concreta."
    params: ForecastRequest

    @classmethod
    def examples(cls):
        hoy = date.today().isoformat()

        return [
            (
                f"Hoy es {hoy}. El usuario dice 'dame el forecast para hoy'. "
                "Reconozco la palabra clave 'forecast' y entiendo que la fecha es la actual. "
                "Ejecuto la herramienta una vez para el día de hoy.",
                cls(params=ForecastRequest(fecha=None)),
            ),
            (
                f"Hoy es {hoy}. El cliente solicita 'el informe de previsión del mes pasado'. "
                "Interpreto 'mes pasado' como la fecha correspondiente al mes anterior al actual. "
                "Ejecuto la herramienta una vez con esa fecha.",
                cls(params=ForecastRequest(fecha=None)),
            ),
            (
                f"Hoy es {hoy}. El usuario pide 'la previsión de mañana'. "
                "Interpreto 'mañana' como un día después de la fecha actual y ejecuto la herramienta una vez con esa fecha.",
                cls(params=ForecastRequest(fecha=None)),
            ),
        ]

    def handle(self) -> FinalResultTool:
        fecha = self.params.fecha or date.today().isoformat()
        forecast = ejecucion_forecast(fecha)
        print(f"✅ Forecast generado para {fecha}: {forecast}")
        return FinalResultTool(
            info={
                "fecha": fecha,
                "respuesta": "Te envio una tarea al workflow con el documento solicitado.",
                "mode": "workflow",
                "status": "hecho_forecast",
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
# 🔮 TOOL 3: Propuesta
# --------------------------------------------------------------------
class PropuestaERPTool(lr.agent.ToolMessage):
    request: str = "create_erp_proposal"
    purpose: str = "Genera una propuesta en el ERP a partir de la historia de un mensaje."
    params: CompaiMessage

    @classmethod
    def examples(cls):
        return [
            (
                "Necesito hacer una propuesta para un cliente.",
                cls(params=CompaiMessage(
                    message="Solicitud de presupuesto paneles solares",
                    from_address="cliente@empresa.com",
                    history={
                        "subject": "Solicitud de presupuesto paneles solares",
                        "from_address": "cliente@empresa.com",
                        "categorias": ["energía", "paneles solares"],
                        "productos": ["panel solar 400W", "inversor 5kW"],
                        "comentarios": ["Cliente pide presupuesto para instalación doméstica."]
                    }
                )),
            ),
            (
                "Mensaje con intención de compra detectada, se genera automáticamente propuesta ERP.",
                cls(params=CompaiMessage(
                    message="Cotización routers Cisco",
                    from_address="it@empresa.com",
                    history={
                        "subject": "Cotización routers Cisco",
                        "from_address": "it@empresa.com",
                        "categorias": ["redes", "hardware"],
                        "productos": ["router Cisco 9200", "switch PoE 24p"],
                        "comentarios": ["Cliente busca alternativas de red para oficina."]
                    }
                )),
            ),
        ]

    def handle(self) -> FinalResultTool:
        historia = self.params.history
        if not historia:
            return FinalResultTool(
                info={
                    "respuesta": None,
                    "mensaje": "No se proporcionó historia para crear la propuesta.",
                    "mode": "offline",
                    "status": "error",
                    "reejecutar": False
                }
            )

        # 🧠 Procesa la historia
        manager = HistoriaManager(historia)
        interpretacion_procesada = manager.crear_json_humano()

        # 🧩 Llama al microservicio ERP
        prop = ProposalMicroserviceClient()
        resultado = prop.crear_proposal_desde_historia(interpretacion_procesada)

        print(f"✅ Propuesta creada con ID {resultado['proposal_id']}")

        # 📄 Generar y recuperar el PDF
        proposal = f"(PROV{resultado['proposal_id']})"
        erp = ERPProxySincrono(URL_ERP)
        response = erp.proposal_create_document(proposal)
        pdf_base64 = response["content_base64"]

        print(f"✅ Documento creado: {pdf_base64[:100]}...")

        if not resultado.get("success", False):
            return FinalResultTool(
                info={
                    "respuesta": resultado,
                    "mensaje": "No se pudo crear la propuesta en el ERP.",
                    "mode": "online",
                    "status": "error",
                    "reejecutar": False
                }
            )

        return FinalResultTool(
            info={
                "respuesta": resultado,
                "adjunto": pdf_base64,
                "mensaje": "Propuesta creada correctamente en el ERP.",
                "mode": "online",
                "status": "completado",
                "reejecutar": False
            }
        )


# --------------------------------------------------------------------
# 🔮 TOOL 4: Envio de correos
# --------------------------------------------------------------------
class EnviarCorreoTool(ToolMessage):
    request: str = "send_email"
    purpose: str = "Envía un correo electrónico con el asunto, cuerpo y adjuntos especificados."
    params: CompaiMessage

    @classmethod
    def examples(cls):
        return [
            (
                "Cliente identificado, se envía correo con propuesta adjunta.",
                cls(params=CompaiMessage(
                    from_address="agente.ia",
                    to_address="peperedondorubio@gmail.com",
                    subject="Propuesta para empresa",
                    message="Estimado cliente, adjuntamos la propuesta solicitada.",
                    attachments=[
                        Attachment(
                            name="propuesta.pdf",
                            content_base64="<base64_del_pdf>",
                            mimetype="application/pdf"
                        )
                    ],
                    accion=["enviar_correo"]
                ))
            ),
            (
                "Se envía correo sin adjuntos confirmando recepción de solicitud.",
                cls(params=CompaiMessage(
                    from_address="agente.ia",
                    to_address="cliente@ejemplo.com",
                    subject="Confirmación de solicitud",
                    message="Hemos recibido su solicitud y la estamos procesando.",
                    action=["enviar_correo"]
                ))
            ),
        ]
    
    # 💬 Genera el cuerpo del correo con ayuda del LLM del agente
    def generar_body(self, interpretacion_procesada) -> str:
        if not self.agent:
            raise RuntimeError("El agente no está disponible en la tool.")
        
        data = self.params
        prompt = f"""
        Escribe un correo profesional y amable con los siguientes datos:
        - Asunto: {data.subject}
        - Contexto: {interpretacion_procesada.get("productos", "")}
        - Destinatario: {interpretacion_procesada.get("empresa", {}).get("name", "Cliente")}
        El correo debe tener tono formal y ser breve.
        *Solo* ponme el cuerpo del correo, sin contexto ni nada adicional
        La respueta debe ser en formato apto para ser enviado por correo electrónico.
        """
        respuesta = self.agent.llm_response(prompt)
        
        # Si Langroid devuelve un solo documento:
        if hasattr(respuesta, "content"):
            return respuesta.content.strip()
        
        # Si devuelve una lista de documentos:
        elif isinstance(respuesta, list) and len(respuesta) > 0 and hasattr(respuesta[0], "content"):
            return respuesta[0].content.strip()
        
        # Si no hay contenido válido
        else:
            raise ValueError(f"Respuesta inesperada del agente: {respuesta}")


    def handle(self) -> FinalResultTool:
        historia = self.params.history
        if not historia:
            return FinalResultTool(
                info={
                    "respuesta": None,
                    "mensaje": "No se proporcionó historia para enviar el correo.",
                    "mode": "offline",
                    "status": "error",
                    "reejecutar": False
                }
            )

        # 🧠 Procesa la historia
        manager = HistoriaManager(historia)
        interpretacion_procesada = manager.crear_json_humano()
        email = interpretacion_procesada.get("empresa", {}).get("email")
        if not email:
            email="peperedondorubio@gmail.com"
            """
            return FinalResultTool(
                info={
                    "respuesta": email,
                    "mensaje": "No existe la dirección del destinatario para enviar el correo",
                    "mode": "online",
                    "status": "error",
                    "reejecutar": False
                }
            )"""

        # Generar el cuerpo del correo
        body = self.generar_body(interpretacion_procesada)

        # Enviar correo
        correo_cli = CorreoClient(URL_CORREO)
        print("Clase de correo iniciada")
        resultado = correo_cli.enviar_correo(
            destinatario=email,
            asunto=self.params.subject,
            mensaje=body,
            adjuntos=self.params.attachments
        )

        print("Correo enviado")

        # Evaluar el resultado
        if resultado.get("status", False) != "success":
            return FinalResultTool(
                info={
                    "respuesta": resultado,
                    "mensaje": "No se ha podido enviar el correo.",
                    "mode": "online",
                    "status": "error",
                    "reejecutar": False
                }
            )

        return FinalResultTool(
            info={
                "respuesta": resultado,
                "mensaje": "Correo enviado correctamente.",
                "mode": "online",
                "status": "hecho_correo",
                "reejecutar": False
            }
        )

# --------------------------------------------------------------------
# 🔮 TOOL 5: Genera y envia Proposal por correo
# --------------------------------------------------------------------


class EnviarPropuestaTool(ToolMessage):
    request: str = "create_and_send_proposal"
    purpose: str = "Crea una propuesta en el ERP y la envía por correo electrónico al cliente."
    params: CompaiMessage

    @classmethod
    def examples(cls):
        return [
            (
                "Mensaje indica solicitud de presupuesto; se genera la propuesta ERP y se envía por correo.",
                cls(params=CompaiMessage(
                    job_id="cc099db97a3545668423874933ed6119",
                    from_address="agente.ia",
                    to_address="cliente@empresa.com",
                    subject="Propuesta instalación solar",
                    message="Estimado cliente, adjuntamos la propuesta solicitada. Un cordial saludo.",
                    history={
                        "subject": "Presupuesto instalación solar",
                        "sender": "cliente@empresa.com",
                        "productos": ["panel solar 400W", "batería 5kWh"],
                        "comentarios": ["Cliente solicita presupuesto completo con instalación."]
                    },
                    action=["create_and_send_proposal"]
                ))
            ),
        ]

    def handle(self) -> FinalResultTool:
        historia = self.params.history
        if not historia:
            return FinalResultTool(
                info={
                    "resultado": "error",
                    "respuesta": None,
                    "mensaje": "No se proporcionó historia para enviar el correo.",
                    "mode": "offline",
                    "status": {},
                    "reejecutar": False
                }
            )

        # 1️⃣ Crear la propuesta usando PropuestaERPTool
        propuesta_tool = PropuestaERPTool(
            params=self.params
        )
        propuesta_tool.agent = self.agent  # 🔥 Reutiliza el mismo agente LLM
        resultado_propuesta = propuesta_tool.handle().info

        if resultado_propuesta.get("status") == "error":
            return FinalResultTool(
                info={
                    "resultado": "error",
                    "respuesta": resultado_propuesta,
                    "mensaje": "❌ No se pudo crear la propuesta en el ERP.",
                    "mode": "online",
                    "status": {},
                    "reejecutar": False
                }
            )

        # Extraer el PDF generado
        pdf_base64 = resultado_propuesta.get("adjunto")
        self.params.attachments = [("propuesta.pdf", pdf_base64, "application/pdf")]
        # 2️⃣ Enviar correo con la propuesta adjunta
        correo_tool = EnviarCorreoTool(
            params=self.params
            )
        correo_tool.agent = self.agent
        resultado_correo = correo_tool.handle().info

        if resultado_correo.get("status") == "error":
            return FinalResultTool(
                info={
                    "resultado": "warning",
                    "respuesta": {
                        "propuesta": resultado_propuesta,
                        "correo": resultado_correo
                    },
                    "mensaje": "⚠️ Propuesta creada, pero el correo no se pudo enviar.",
                    "mode": "online",
                    "status": {},
                    "reejecutar": False
                }
            )

        # ✅ Resultado final combinado
        #
        # Actualizar el job_id con la informacion de proposal hecho

        return FinalResultTool(
            info={
                "resultado": "ok", 
                "job_id": self.params.job_id,
                "respuesta": {
                    "propuesta": resultado_propuesta,
                    "correo": resultado_correo
                },
                "mensaje": "✅ Propuesta creada y enviada correctamente.",
                "mode": "online",
                "status": {"proposal" : "done"},
                "reejecutar": False
            }
        )



class PruebaTool(ToolMessage):
    request: str = "create_test"
    purpose: str = "Crea una tool de prueba."
    params: CompaiMessage

    @classmethod
    def examples(cls):
        return [
            (
                "Mensaje indica solicitud de presupuesto; se genera la propuesta ERP y se envía por correo.",
                cls(params=CompaiMessage(
                    from_address="agente.ia",
                    to_address="cliente@empresa.com",
                    subject="Propuesta instalación solar",
                    message="Estimado cliente, adjuntamos la propuesta solicitada. Un cordial saludo.",
                    history={
                        "subject": "Presupuesto instalación solar",
                        "sender": "cliente@empresa.com",
                        "productos": ["panel solar 400W", "batería 5kWh"],
                        "comentarios": ["Cliente solicita presupuesto completo con instalación."]
                    },
                    action=["create_and_send_proposal"]
                ))
            ),
        ]

    def handle(self) -> FinalResultTool:
        
        params = self.params
        return FinalResultTool(
            info={
                "resultado": "error",
                "respuesta": params,
                "mensaje": "No se proporcionó historia para enviar el correo.",
                "mode": "offline",
                "status": {"test": "done"},
                "reejecutar": False
            }
        )


if __name__ == "__main__":

    params = CompaiMessage(
        job_id="cc099db97a3545668423874933ed6119",
        from_address="agente",
        to_address="cliente@test.com",
        subject="Propuesta",
        message="Hola",
        history={"subject": "presupuesto"},
    )

    tool = EnviarPropuestaTool(params=params)
    result = tool.handle()

    gestionar_resultado_tool = tool.gestionar_resultado_tool(result.info)

    print("RESULTADO:")
    print(result.info)