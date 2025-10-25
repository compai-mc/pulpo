import requests
import os
from dotenv import load_dotenv
load_dotenv()

BASE_URL_ORQUESTATOR = os.getenv("BASE_URL_ORQUESTATOR")

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
        raise Exception(f"Error: {resp.status_code} - {resp.text}")


# Ejemplo de uso
if __name__ == "__main__":

    # Crear una tarjeta
    try:
        print(create_card("mi_board_id", "Título de prueba", "Descripción de prueba"))
    except Exception as e:
        print(e)