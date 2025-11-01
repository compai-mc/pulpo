from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum
from datetime import datetime
import uuid

class Attachment(BaseModel):
    name: str = Field(..., description="Nombre del archivo adjunto, por ejemplo 'propuesta.pdf'.")
    content_base64: str = Field(..., description="Contenido del archivo codificado en Base64.")
    mimetype: str = Field(..., description="Tipo MIME del adjunto, por ejemplo 'application/pdf'.")

class Attachments(BaseModel):
    """Information about attachments found in the message."""
    total_count: int = Field(..., description="Total number of attachments found.")
    pdf_count: int = Field(..., description="Number of PDF attachments found.")
    has_attachments: bool = Field(..., description="Whether the message contains any attachments.")
    has_pdfs: bool = Field(..., description="Whether the message contains PDF attachments.")
    pdf_filenames: Optional[List[str]] = Field(default=None, description="List of PDF filenames found in the message.")
    atachment_list: Optional[List[Attachment]] = Field(
        default=None,
        description="List of attachment details including name, content in Base64, and MIME type."
    )

class CurrentMessage(BaseModel):
    """Basic information about the current processed message."""
    message_key: str = Field(..., description="Key of the message being currently processed.")
    sender: str = Field(..., description="Address of the sender of this message.")
    subject: str = Field(..., description="Subject of the current message.")
    received_ts: int = Field(..., description="UNIX timestamp when this message was received.")


class ParentMessage(BaseModel):
    """Information about the immediate parent message in the thread."""
    message_key: str
    score: float
    subject: str
    received_ts: int


class RelatedMessage(BaseModel):
    """Information about related messages (siblings, replies, forwards)."""
    message_key: str
    score: float
    received_ts: int
    subject: str


class MessageHistory(BaseModel):
    """Information about related messages in the same conversation thread."""
    parent: Optional[ParentMessage] = None
    related: Optional[List[RelatedMessage]] = None


class ProcessingInfo(BaseModel):
    """Internal information about message processing and enrichment."""
    embedding_stored: Optional[bool] = None
    similarity_count: Optional[int] = None
    enrichment_status: Optional[str] = None
    processing_version: Optional[str] = None


class Metadata(BaseModel):
    """Metadata about the storage and source of this message record."""
    source: Optional[str] = None
    database_key: Optional[str] = None
    vector_stored: Optional[bool] = None
    has_conversation: Optional[bool] = None


class Analysis(BaseModel):
    """Sentiment and classification analysis results for the message."""
    polarity: Optional[float] = None
    category: Optional[str] = None
    sentiment: Optional[str] = None
    language: Optional[str] = None


class CompaiMessage(BaseModel):
    """Schema defining the structure of a processed message."""

    message_key: str = Field(
        default_factory=lambda: uuid.uuid4().hex,
        description="Unique identifier of the message in the source system."
    )
    job_id: Optional[str] = Field(
        None,
        description="Optional job identifier used by processing pipelines or workflows."
    )
    subject: Optional[str] = Field(
        None, description="Subject line of the message."
    )
    from_address: Optional[str] = Field(
        None, description="Address of the sender."
    )
    from_name: Optional[str] = Field(
        None, description="Display name of the sender, if available."
    )
    to_address: Optional[str] = Field(
        None, description="Address of the recipient."
    )
    to_name: Optional[str] = Field(
        None, description="Display name of the recipient, if available."
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="Creation timestamp for the message record (ISO 8601 string)."
    )
    received_ts: str = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="Timestamp (ISO 8601 string) when the message was received."
    )
    processed_ts: Optional[str] = Field(
        default=None,
        description="Timestamp (ISO 8601) when the message was processed by the system."
    )
    parent_message: Optional[str] = Field(
        None, description="Key or ID of the parent message in the conversation thread."
    )
    thread_depth: Optional[int] = Field(
        None, description="Depth level within the conversation thread (0 = root message)."
    )
    decide_stop: Optional[bool] = Field(
        None, description="Indicates whether message processing or classification should be stopped."
    )
    message: str = Field(
        ..., description="Full cleaned text content of the message body."
    )
    items_count: Optional[int] = None
    items: Optional[List[Dict[str, Any]]] = None
    related_messages_count: Optional[int] = None
    attachments: Optional["Attachments"] = None
    pdf_attachments: Optional[List[Dict[str, Any]]] = None
    current_message: Optional["CurrentMessage"] = None
    message_history: Optional["MessageHistory"] = None
    processing_info: Optional["ProcessingInfo"] = None
    processing_complete: Optional[bool] = None
    metadata: Optional["Metadata"] = None
    analysis: Optional["Analysis"] = None
    status: Optional[List[str]] = None
    action: Optional[List[str]] = None
    history: Optional[Dict[str, Any]] = None
    story: Optional[Dict[str, Any]] = None

    def model_dump(self, **kwargs):
        """
        Sobrescribe el model_dump para convertir datetime → str (ISO 8601)
        automáticamente antes de serializar.
        """
        data = super().model_dump(**kwargs)
        for field in ["timestamp", "received_ts", "processed_ts"]:
            if isinstance(data.get(field), datetime):
                data[field] = data[field].isoformat()
        return data


class EstadoTarea(str, Enum):
    PENDIENTE = "pending"
    INTERPRETADO = "interpreted"
    EN_PROGRESO = "in_progress"
    COMPLETADA = "completed"
    FALLIDA = "failed"
    CANCELADA = "cancelled"
    CHAT = "chat"

# Transiciones asociadas a cada estado
TRANSICIONES = {
    EstadoTarea.PENDIENTE: "procesar_nueva_tarea",
    EstadoTarea.INTERPRETADO: "continuar_tarea",
    EstadoTarea.EN_PROGRESO: "continuar_tarea",
    EstadoTarea.COMPLETADA: "procesar_resultado",
    EstadoTarea.FALLIDA: "procesar_fallo",
    EstadoTarea.CANCELADA: "procesar_cancelacion",
    EstadoTarea.CHAT: "procesar_chat",
}


