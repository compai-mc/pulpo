from datetime import date
from typing import List, Optional, Dict, Any
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
from .proxy_erpdolibarr import ERPProxy
from .proxy_erpdolibarr_sincrono import ERPProxySincrono
from .esquema import MensajeEntrada,FicheroAdjunto

from pulpo.util.util import require_env
import asyncio

URL_ERP = require_env("ERPDOLIBARR_URL")
URL_PROPOSAL = require_env("URL_PROPOSAL")
URL_CORREO = require_env("URL_CORREO")

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
# 🔮 TOOL 3: Propuesta
# --------------------------------------------------------------------
class PropuestaERPTool(lr.agent.ToolMessage):
    request: str = "create_erp_proposal"
    purpose: str = "Genera una propuesta en el ERP a partir de la historia de un mensaje."
    params: MensajeEntrada  

    @classmethod
    def examples(cls):
        return [
            (
                "Necesito hacer una propuesta para un cliente.",
                cls(params=MensajeEntrada(
                    mensaje="Solicitud de presupuesto paneles solares",
                    remitente="cliente@empresa.com",
                    historia={
                        "subject": "Solicitud de presupuesto paneles solares",
                        "sender": "cliente@empresa.com",
                        "categorias": ["energía", "paneles solares"],
                        "productos": ["panel solar 400W", "inversor 5kW"],
                        "comentarios": ["Cliente pide presupuesto para instalación doméstica."]
                    }
                )),
            ),
            (
                "Mensaje con intención de compra detectada, se genera automáticamente propuesta ERP.",
                cls(params=MensajeEntrada(
                    mensaje="Cotización routers Cisco",
                    remitente="it@empresa.com",
                    historia={
                        "subject": "Cotización routers Cisco",
                        "sender": "it@empresa.com",
                        "categorias": ["redes", "hardware"],
                        "productos": ["router Cisco 9200", "switch PoE 24p"],
                        "comentarios": ["Cliente busca alternativas de red para oficina."]
                    }
                )),
            ),
        ]

    def handle(self) -> FinalResultTool:
        historia = self.params.historia
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
    params: MensajeEntrada

    @classmethod
    def examples(cls):
        return [
            (
                "Cliente identificado, se envía correo con propuesta adjunta.",
                cls(params=MensajeEntrada(
                    remitente="agente.ia",
                    destino="peperedondorubio@gmail.com",
                    asunto="Propuesta para empresa",
                    mensaje="Estimado cliente, adjuntamos la propuesta solicitada.",
                    adjuntos=[
                        FicheroAdjunto(
                            nombre="propuesta.pdf",
                            contenido_base64="<base64_del_pdf>",
                            tipo_mime="application/pdf"
                        )
                    ],
                    accion=["enviar_correo"]
                ))
            ),
            (
                "Se envía correo sin adjuntos confirmando recepción de solicitud.",
                cls(params=MensajeEntrada(
                    remitente="agente.ia",
                    destino="cliente@ejemplo.com",
                    asunto="Confirmación de solicitud",
                    mensaje="Hemos recibido su solicitud y la estamos procesando.",
                    accion=["enviar_correo"]
                ))
            ),
        ]
    
    # 💬 Genera el cuerpo del correo con ayuda del LLM del agente
    def generar_body(self) -> str:
        if not self.agent:
            raise RuntimeError("El agente no está disponible en la tool.")
        
        data = self.params
        prompt = f"""
        Escribe un correo profesional y amable con los siguientes datos:
        - Asunto: {data.asunto}
        - Contexto: {data.mensaje}
        - Destinatario: {data.destino}
        El correo debe tener tono formal y ser breve.
        """
        respuesta = self.agent.llm_response(prompt)
        return respuesta.message.strip()

    def handle(self) -> FinalResultTool:
        historia = self.params.historia
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
            return FinalResultTool(
                info={
                    "respuesta": email,
                    "mensaje": "No existe la dirección del destinatario para enviar el correo",
                    "mode": "online",
                    "status": "error",
                    "reejecutar": False
                }
            )

        # Generar el cuerpo del correo
        body = self.generar_body()

        # Preparar adjuntos (si los hay)
        adjuntos = []
        if self.params.adjuntos:
            for adj in self.params.adjuntos:
                adjuntos.append((adj.nombre, adj.contenido_base64, adj.tipo_mime))

        # Enviar correo
        correo_cli = CorreoClient(URL_CORREO)
        resultado = correo_cli.enviar_correo(
            destinatario=email,
            asunto=self.params.asunto,
            mensaje=body,
            adjuntos=adjuntos
        )

        # Evaluar el resultado
        if not resultado.get("success", False):
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
                "status": "completado",
                "reejecutar": False
            }
        )

# --------------------------------------------------------------------
# 🔮 TOOL 5: Genera y envia Proposal por correo
# --------------------------------------------------------------------

class EnviarPropuestaRequest(BaseModel):
    historia: Dict[str, Any] = Field(..., description="Historia interpretativa del mensaje para generar la propuesta.")
    destinatario: str = Field(..., description="Correo del destinatario de la propuesta.")
    asunto: str = Field(..., description="Asunto del correo.")
    mensaje: str = Field(..., description="Cuerpo del correo.")
    enviar_correo: bool = Field(default=True, description="Si es True, envía automáticamente el correo con la propuesta adjunta.")


class EnviarPropuestaTool(ToolMessage):
    request: str = "create_and_send_proposal"
    purpose: str = "Crea una propuesta en el ERP y la envía por correo electrónico al cliente."
    params: EnviarPropuestaRequest

    @classmethod
    def examples(cls):
        return [
            (
                "Mensaje indica solicitud de presupuesto; se genera la propuesta ERP y se envía por correo.",
                cls(params=EnviarPropuestaRequest(
                    historia={
                        "subject": "Presupuesto instalación solar",
                        "sender": "cliente@empresa.com",
                        "productos": ["panel solar 400W", "batería 5kWh"],
                        "comentarios": ["Cliente solicita presupuesto completo con instalación."]
                    },
                    destinatario="cliente@empresa.com",
                    asunto="Propuesta instalación solar",
                    mensaje="Estimado cliente, adjuntamos la propuesta solicitada. Un cordial saludo.",
                    enviar_correo=True
                ))
            ),
        ]

    def handle(self) -> FinalResultTool:
        data = self.params

        # ======================================================
        # 1️⃣ Crear la propuesta usando la tool PropuestaERPTool
        # ======================================================
        propuesta_tool = PropuestaERPTool(
            params=PropuestaERPRequest(historia=data.historia)
        )
        propuesta_tool.agent = self.agent  # 🔥 Mismo LLM y contexto
        resultado_propuesta = propuesta_tool.handle().info

        if resultado_propuesta.get("status") == "error":
            return FinalResultTool(
                info={
                    "respuesta": resultado_propuesta,
                    "mensaje": "No se pudo crear la propuesta en el ERP.",
                    "mode": "online",
                    "status": "error",
                    "reejecutar": False
                }
            )

        # Aquí ya tienes el PDF en base64 dentro del resultado
        pdf_base64 = resultado_propuesta.get("adjunto")

        # ======================================================
        # 2️⃣ Enviar el correo usando la tool EnviarCorreoTool
        # ======================================================
        if data.enviar_correo:
            correo_tool = EnviarCorreoTool(
                params=EnviarCorreoRequest(
                    destinatario=data.destinatario,
                    asunto=data.asunto,
                    mensaje=data.mensaje,
                    adjuntos=[
                        CorreoAdjunto(
                            nombre="propuesta.pdf",
                            contenido_base64=pdf_base64,
                            tipo_mime="application/pdf"
                        )
                    ]
                )
            )
            correo_tool.agent = self.agent  # 🔥 Misma instancia de LLM
            resultado_correo = correo_tool.handle().info

            if resultado_correo.get("status") == "error":
                return FinalResultTool(
                    info={
                        "respuesta": {
                            "propuesta": resultado_propuesta,
                            "correo": resultado_correo
                        },
                        "mensaje": "Propuesta creada pero el correo no se pudo enviar.",
                        "mode": "online",
                        "status": "warning",
                        "reejecutar": False
                    }
                )

        # ======================================================
        # ✅ Resultado final combinado
        # ======================================================
        return FinalResultTool(
            info={
                "respuesta": {
                    "propuesta": resultado_propuesta,
                    "correo": "enviado" if data.enviar_correo else "no enviado"
                },
                "mensaje": "Propuesta creada y enviada correctamente.",
                "mode": "online",
                "status": "completado",
                "reejecutar": False
            }
        )
