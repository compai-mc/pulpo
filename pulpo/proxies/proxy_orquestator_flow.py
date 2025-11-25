import requests
import os
from pulpo.util.esquema import CompaiMessage
from pulpo.util.util import require_env

BASE_URL_ORQUESTATOR = require_env("BASE_URL_ORQUESTATOR")

def create_card(board_id: str, title: str, description: str = "", list_name: str = None, swimlane_name: str = None):
    url = f"{BASE_URL_ORQUESTATOR}/card/create"
    payload = {
        "board_id": board_id,
        "title": title,
        "description": description,
        "list_name": list_name,
        "swimlane_name": swimlane_name
    }
    resp = requests.post(url, json=payload)
    if resp.ok:
        return resp.json()
    else:
        raise Exception(f"Error creando tarjeta del Workflow: {resp.status_code} - {resp.text}")
    

    
def publish_complete_job(mensaje: CompaiMessage):
    """
    Llama al endpoint /job/publish/complete del orquestador para publicar un job completado.
    """
    url = f"{BASE_URL_ORQUESTATOR}/job/publish/complete"

    try:
        resp = requests.post(url, json=mensaje.model_dump(mode="json", exclude_none=True))
        resp.raise_for_status()
        return resp.json()

    except requests.RequestException as e:
        raise Exception(f"[publish_complete_job] Error publicando job completo: {e}")


# Ejemplo de uso
if __name__ == "__main__":

    # Crear una tarjeta
    try:
        print(create_card("mi_board_id", "Título de prueba", "Descripción de prueba"))
    except Exception as e:
        print(e)