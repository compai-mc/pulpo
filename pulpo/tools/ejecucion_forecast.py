import asyncio
import requests
import proxy_forecast
import os
from datetime import datetime,date

import proxy_orquestator_flow

TABLERO_OPERACIONES = os.getenv("TABLERO_OPERACIONES","EQyBuWaxdzT9HgwCH")  
LISTA_NOTIFICACIONES = os.getenv("LISTA_NOTIFICACIONES","Notificaciones")  
SWIMLANE = os.getenv("SWIMLANE","Virtual")  

def _crear_tarea_wekan(mensaje: str):
    """Crea una tarea en Wekan vía API REST (síncrono)."""
    print(f"📌 Creando tarea en Wekan: {mensaje}")
    
    json_respuesta = proxy_orquestator_flow.create_card(
                    board_id=TABLERO_OPERACIONES,
                    title=mensaje,
                    description="Informe de previsión de ventas",
                    list_name=LISTA_NOTIFICACIONES,
                    swimlane_name=SWIMLANE
                )
    
    print(json_respuesta)

    return True


async def _forecast_en_background(fecha: str):

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
    resultado = await proxy_forecast.generar_forecast_minio(fecha_iso) 

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

    #loop = asyncio.get_running_loop()
    #await loop.run_in_executor(None, _crear_tarea_wekan, mensaje)

    _crear_tarea_wekan(mensaje)

    print( f"Forecast completado para {fecha_iso} en {resultado}")

    
async def ejecucion_forecast(fecha: str):
    # Lanzar forecast en paralelo (no bloquea flujo principal)
    tarea = asyncio.create_task(_forecast_en_background(fecha))
    print("Generandose el forecast .....")

    #resultado = await tarea

    return (
        "Ejecutando la previsión de ventas que has pedido. "
        "Te llegará una tarea a tu workflow cuando haya terminado."
    )

if __name__ == "__main__":
    fecha = "2025-09-07"

    async def main():
        # Ejecuta el forecast
        resultado = await ejecucion_forecast(fecha)
        print(resultado)

        # Mantener el programa corriendo sin bloquear
        await asyncio.Event().wait()

    asyncio.run(main())
