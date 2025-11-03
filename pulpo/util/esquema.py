from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime
import uuid

class Attachment(BaseModel):
    """Representa un archivo adjunto dentro de un mensaje."""
    name: str = Field(..., description="Nombre del archivo adjunto, por ejemplo 'propuesta.pdf'.")
    content_base64: str = Field(..., description="Contenido del archivo codificado en Base64.")
    mimetype: str = Field(..., description="Tipo MIME del adjunto, por ejemplo 'application/pdf'.")


class Attachments(BaseModel):
    """Información agregada sobre los archivos adjuntos detectados en el mensaje."""
    total_count: int = Field(..., description="Número total de archivos adjuntos encontrados en el mensaje.")
    pdf_count: int = Field(..., description="Número de archivos PDF entre los adjuntos.")
    has_attachments: bool = Field(..., description="Indica si el mensaje contiene algún archivo adjunto.")
    has_pdfs: bool = Field(..., description="Indica si el mensaje contiene adjuntos en formato PDF.")
    pdf_filenames: Optional[List[str]] = Field(default=None, description="Lista de nombres de archivos PDF encontrados.")
    attachment_list: Optional[List[Attachment]] = Field(
        default=None,
        description="Lista detallada de los archivos adjuntos, incluyendo nombre, contenido y tipo MIME."
    )


class RelatedMessage(BaseModel):
    """Información sobre mensajes relacionados (respuestas, reenvíos o mensajes similares)."""
    message_key: str = Field(..., description="Identificador único del mensaje relacionado.")
    score: float = Field(..., description="Nivel de similitud o relación con el mensaje actual.")
    received_ts: int = Field(..., description="Timestamp UNIX del momento en que se recibió el mensaje relacionado.")
    subject: str = Field(..., description="Asunto del mensaje relacionado.")


class MessageHistory(BaseModel):
    """Estructura que representa el historial de conversación relacionado con este mensaje."""
    parent: Optional[RelatedMessage] = Field(
        default=None,
        description="Mensaje padre directo dentro del hilo de conversación."
    )
    related: Optional[List[RelatedMessage]] = Field(
        default=None,
        description="Lista de mensajes relacionados dentro del mismo hilo (respuestas o reenvíos)."
    )


class Analysis(BaseModel):
    """Resultados del análisis de sentimiento, categoría y lenguaje del mensaje."""
    polarity: Optional[float] = Field(default=None, description="Valor numérico que representa la polaridad del texto (-1 negativo, 1 positivo).")
    category: Optional[str] = Field(default=None, description="Categoría o tipo asignado al mensaje tras la clasificación automática.")
    sentiment: Optional[str] = Field(default=None, description="Sentimiento detectado (positivo, negativo o neutro).")
    language: Optional[str] = Field(default=None, description="Código del idioma detectado en el texto (por ejemplo, 'es' o 'en').")


class CompaiMessage(BaseModel):
    """Esquema principal que define la estructura de un mensaje procesado por el sistema."""

    message_key: str = Field(
        default_factory=lambda: uuid.uuid4().hex,
        description="Identificador único del mensaje dentro del sistema origen."
    )
    job_id: Optional[str] = Field(
        default=None,
        description="Identificador opcional del trabajo o pipeline asociado al procesamiento del mensaje."
    )
    subject: Optional[str] = Field(
        default=None, description="Asunto o título del mensaje."
    )
    from_address: Optional[str] = Field(
        default=None, description="Dirección de mensaje del remitente del mensaje."
    )
    from_name: Optional[str] = Field(
        default=None, description="Nombre visible del remitente, si está disponible."
    )
    to_address: Optional[str] = Field(
        default=None, description="Dirección del destinatario."
    )
    to_name: Optional[str] = Field(
        default=None, description="Nombre visible del destinatario, si está disponible."
    )
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="Marca temporal (ISO 8601) que indica cuándo se creó el objeto de mensaje."
    )
    received_ts: datetime = Field(
        default_factory=datetime.now,
        description="Marca temporal (ISO 8601) que indica cuándo se recibió el mensaje en el sistema."
    )
    processed_ts: Optional[datetime] = Field(
        default=None,
        description="Marca temporal (ISO 8601) que indica cuándo el mensaje fue procesado completamente."
    )

    thread_depth: Optional[int] = Field(
        default=None, description="Nivel de profundidad dentro del hilo de conversación (0 = mensaje raíz)."
    )
    decide_stop: Optional[bool] = Field(
        default=None, description="Indica si debe detenerse el procesamiento o la clasificación del mensaje."
    )
    message: str = Field(
        ..., description="Contenido textual completo y limpio del cuerpo del mensaje."
    )

    message_history: Optional[MessageHistory] = Field(
        default=None, description="Historial de conversación asociado con el mensaje actual."
    )
    related_messages_count: Optional[int] = Field(
        default=None, description="Número de mensajes relacionados dentro del hilo de conversación."
    )

    attachments_count: Optional[int] = Field(
        default=None, description="Número total de adjuntos asociados al mensaje."
    )
    attachments: Optional[Attachments] = Field(
        default=None, description="Información detallada sobre los archivos adjuntos del mensaje."
    )
    pdf_attachments: Optional[List[Dict[str, Any]]] = Field(
        default=None, description="Lista de archivos PDF extraídos o procesados del mensaje."
    )

    analysis: Optional[Analysis] = Field(
        default=None, description="Resultados del análisis semántico y emocional del mensaje."
    )

    status: Optional[List[str]] = Field(
        default=None, description="Lista de estados actuales del mensaje dentro del flujo de procesamiento."
    )
    action: Optional[List[str]] = Field(
        default=None, description="Acciones realizadas o pendientes asociadas a este mensaje."
    )
    history: Optional[Dict[str, Any]] = Field(
        default=None, description="Historia de la conversacion o interacciones previas relacionadas con el mensaje."
    )
    story: Optional[Dict[str, Any]] = Field(
        default=None, description="Contexto narrativo o historia generada en torno al mensaje."
    )


class EstadoTarea(str, Enum):
    """Enumeración de los posibles estados de una tarea dentro del flujo de procesamiento."""
    PENDIENTE = "pending"
    INTERPRETADO = "interpreted"
    EN_PROGRESO = "in_progress"
    COMPLETADA = "completed"
    FALLIDA = "failed"
    CANCELADA = "cancelled"
    CHAT = "chat"


# Diccionario que define la transición o acción asociada a cada estado de tarea.
TRANSICIONES = {
    EstadoTarea.PENDIENTE: "procesar_nueva_tarea",
    EstadoTarea.INTERPRETADO: "continuar_tarea",
    EstadoTarea.EN_PROGRESO: "continuar_tarea",
    EstadoTarea.COMPLETADA: "procesar_resultado",
    EstadoTarea.FALLIDA: "procesar_fallo",
    EstadoTarea.CANCELADA: "procesar_cancelacion",
    EstadoTarea.CHAT: "procesar_chat",
}
