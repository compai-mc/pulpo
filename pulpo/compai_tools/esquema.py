import os
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

# Esquema de datos de entrada de mensajes
class MensajeEntrada(BaseModel):
    job_id: Optional[str] = Field(None, description="ID único del proceso o conversación")
    remitente: Optional[str] = None
    destino: Optional[str] = None
    asunto: Optional[str] = None
    mensaje: str = Field(..., description="Contenido del mensaje")  # ← obligatorio
    estado: Optional[List[str]] = None
    accion: Optional[List[str]] = None
    historial: Optional[List[Dict[str, Any]]] = []       # Campo de historial de mensajes anteriores (Opcional) 
    historia: Optional[Dict[str, Any]] = None            # Campo de historia de un mensaje-job (Opcional)
    contexto: Optional[Dict[str, Any]] = Field(
        default_factory=lambda: {"timestamp": datetime.now().isoformat()}
    )

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