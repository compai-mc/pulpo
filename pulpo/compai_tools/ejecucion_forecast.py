import asyncio
import os
from datetime import datetime,date

from .proxy_forecast import generar_forecast_minio
from .proxy_orquestator_flow import create_card
from pulpo.util.util import require_env

TABLERO_OPERACIONES = require_env("TABLERO_OPERACIONES")  
LISTA_NOTIFICACIONES = require_env("LISTA_NOTIFICACIONES")  
SWIMLANE = require_env("SWIMLANE")  

def _crear_tarea_wekan(mensaje: str):
    """Crea una tarea en Wekan vía API REST (síncrono)."""
    print(f"📌 Creando tarea en Wekan: {mensaje}")
    
    json_respuesta = create_card(
                    board_id=TABLERO_OPERACIONES,
                    title=mensaje,
                    description="Informe de previsión de ventas",
                    list_name=LISTA_NOTIFICACIONES,
                    swimlane_name=SWIMLANE
                )
    
    print(json_respuesta)

    return True


async def _forecast_en_background(fecha: str):

    print(f"Iniciando procesamiento de forecast en background para {fecha}")

    # Normalizar fecha a formato ISO (YYYY-MM-DD)
    try:
        # Intentamos parsear directamente
        fecha_obj = datetime.fromisoformat(fecha)
    except ValueError:
        # Intentar con formatos comunes
        for fmt in ("%m-%Y", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                fecha_obj = datetime.strptime(fecha, fmt)
                break
            except ValueError:
                continue
        else:
            # Si no se reconoce, usar hoy como fallback
            fecha_obj = datetime.today()
    
    fecha_iso = fecha_obj.date().isoformat()

    # Ejecuta forecast async
    resultado = await generar_forecast_minio(fecha_iso) 

    # Parseamos la fecha del forecast (en ISO)
    try:
        fecha_dt = datetime.fromisoformat(fecha_iso)
        fecha_forecast = fecha_dt.strftime("%d de %B de %Y")
    except Exception:
        fecha_forecast = fecha_iso  # fallback si no se puede parsear

    # Tarea de WEKAN
    mensaje = f"""
    📊 **Forecast generado**  
    🗓 Fecha del forecast: **{fecha_forecast}**  
    ⏱ Generado el: {date.today().strftime("%d de %B de %Y")}  
    🔗 Descargar Excel: [Haz clic aquí]({resultado["download_url"]})
    """

    _crear_tarea_wekan(mensaje)

    print( f"Forecast completado para {fecha_iso} en {resultado}")

    
def ejecucion_forecast(fecha: str) -> str:
    """
    Función síncrona que ejecuta tarea asíncrona en un nuevo event loop
    """
    def _lanzar_background():
        """Lanza la tarea asíncrona en un nuevo event loop"""
        asyncio.run(_forecast_en_background(fecha))
    
    # Ejecutar en un hilo separado para no bloquear

    print("iniciando hilo de forecast...")

    import threading
    thread = threading.Thread(target=_lanzar_background)
    thread.daemon = True  # Para que no impida la salida del programa
    thread.start()
    
    return f"Procesamiento iniciado para {fecha}"





if __name__ == "__main__":
    fecha = "2025-09-07"

    async def main():
        # Ejecuta el forecast
        resultado = ejecucion_forecast(fecha)
        print(resultado)

        # Mantener el programa corriendo sin bloquear
        await asyncio.Event().wait()

    asyncio.run(main())
