from datetime import date
from typing import List, Optional, Dict, Any

from langroid.pydantic_v1 import BaseModel, Field
import langroid as lr
from langroid.agent.tools.orchestration import FinalResultTool
from langroid.agent import ToolMessage

from .proxy_proccess_controler import ProccessControlerProxy
from .ejecucion_forecast import ejecucion_forecast


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
# 🔮 TOOL 3: Proposal
# --------------------------------------------------------------------

class PropuestaERPRequest(BaseModel):
    historia: Dict[str, Any] = Field(..., description="Diccionario con la historia  del mensaje.")


class PropuestaERPTool(ToolMessage):
    request: str = "create_erp_proposal"
    purpose: str = "Genera una propuesta en el ERP a partir de la historia de interpretación de un mensaje."
    params: PropuestaERPRequest

    @classmethod
    def examples(cls):
        return [
            (
                "Mensaje indica interés en productos solares, se crea una propuesta en el ERP con los datos históricos.",
                cls(params=PropuestaERPRequest(historia={
                    "subject": "Solicitud de presupuesto paneles solares",
                    "sender": "cliente@empresa.com",
                    "categorias": ["energía", "paneles solares"],
                    "productos": ["panel solar 400W", "inversor 5kW"],
                    "comentarios": ["Cliente pide presupuesto para instalación doméstica."]
                })),
            ),
            (
                "Mensaje con intención de compra detectada, se genera automáticamente propuesta ERP.",
                cls(params=PropuestaERPRequest(historia={
                    "subject": "Cotización routers Cisco",
                    "sender": "it@empresa.com",
                    "categorias": ["redes", "hardware"],
                    "productos": ["router Cisco 9200", "switch PoE 24p"],
                    "comentarios": ["Cliente busca alternativas de red para oficina."]
                })),
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

        # Aquí iría la integración real con tu ERP
        # por ejemplo a través de un proxy o API.
        from integrations.erp_proxy import ERPProxy  # ejemplo de proxy

        erp = ERPProxy()
        resultado = erp.crear_propuesta(historia)

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
                "mensaje": "Propuesta creada correctamente en el ERP.",
                "mode": "online",
                "status": "completado",
                "reejecutar": False
            }
        )
    

# --------------------------------------------------------------------
# 🔮 TOOL 4: Envio de correos
# --------------------------------------------------------------------

class CorreoAdjunto(BaseModel):
    nombre: str = Field(..., description="Nombre del archivo adjunto, por ejemplo 'propuesta.pdf'.")
    contenido_base64: str = Field(..., description="Contenido del archivo codificado en Base64.")
    tipo_mime: str = Field(..., description="Tipo MIME del adjunto, por ejemplo 'application/pdf'.")


class EnviarCorreoRequest(BaseModel):
    destinatario: str = Field(..., description="Dirección de correo del destinatario.")
    asunto: str = Field(..., description="Asunto del correo.")
    mensaje: str = Field(..., description="Cuerpo del correo en texto o HTML.")
    adjuntos: List[CorreoAdjunto] = Field(default_factory=list, description="Lista de adjuntos en formato Base64.")


class EnviarCorreoTool(ToolMessage):
    request: str = "send_email"
    purpose: str = "Envía un correo electrónico con el asunto, cuerpo y adjuntos especificados."
    params: EnviarCorreoRequest

    @classmethod
    def examples(cls):
        return [
            (
                "Cliente identificado, se envía correo con propuesta adjunta.",
                cls(params=EnviarCorreoRequest(
                    destinatario="peperedondorubio@gmail.com",
                    asunto="Propuesta para empresa",
                    mensaje="Estimado cliente, adjuntamos la propuesta solicitada.",
                    adjuntos=[
                        CorreoAdjunto(
                            nombre="propuesta.pdf",
                            contenido_base64="<base64_del_pdf>",
                            tipo_mime="application/pdf"
                        )
                    ]
                ))
            ),
            (
                "Se envía correo sin adjuntos confirmando recepción de solicitud.",
                cls(params=EnviarCorreoRequest(
                    destinatario="cliente@ejemplo.com",
                    asunto="Confirmación de solicitud",
                    mensaje="Hemos recibido su solicitud y la estamos procesando.",
                    adjuntos=[]
                ))
            ),
        ]

    def handle(self) -> FinalResultTool:
        data = self.params

        if not data.destinatario or not data.asunto or not data.mensaje:
            return FinalResultTool(
                info={
                    "respuesta": None,
                    "mensaje": "Faltan campos obligatorios (destinatario, asunto o mensaje).",
                    "mode": "offline",
                    "status": "error",
                    "reejecutar": False
                }
            )

        # Aquí iría la lógica real de envío de correo
        # por ejemplo, utilizando un proxy o un servicio SMTP/API.
        from integrations.mail_proxy import MailProxy  # ejemplo de integración

        mailer = MailProxy()
        resultado = mailer.enviar_correo(
            destinatario=data.destinatario,
            asunto=data.asunto,
            mensaje=data.mensaje,
            adjuntos=[
                (a.nombre, a.contenido_base64, a.tipo_mime)
                for a in data.adjuntos
            ]
        )

        if not resultado.get("success", False):
            return FinalResultTool(
                info={
                    "respuesta": resultado,
                    "mensaje": "No se pudo enviar el correo.",
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


from tools.propuesta_erp_tool import PropuestaERPTool, PropuestaERPRequest
from tools.enviar_correo_tool import EnviarCorreoTool, EnviarCorreoRequest, CorreoAdjunto


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
        propuesta_tool = PropuestaERPTool(params=PropuestaERPRequest(historia=data.historia))
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

        # ======================================================
        # 2️⃣ Generar PDF de la propuesta (usando tu proxy o ya incluido en la propuesta)
        # ======================================================
        try:
            from integrations.pdf_proxy import PDFProxy
            pdf = PDFProxy()
            pdf_base64 = pdf.generar_pdf_propuesta(resultado_propuesta)
        except Exception as e:
            return FinalResultTool(
                info={
                    "respuesta": resultado_propuesta,
                    "mensaje": f"Error al generar PDF: {e}",
                    "mode": "online",
                    "status": "error",
                    "reejecutar": False
                }
            )

        # ======================================================
        # 3️⃣ Enviar el correo usando la tool EnviarCorreoTool
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
