import requests
import os


BASE_URL = os.getenv("ERPDOLIBARR_URL")

def get_pedidos_url(fecha):
    response = requests.get(f"{BASE_URL}/orders/{fecha}/url")
    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error al obtener la URL de los pedidos para la fecha {fecha}:", response.text)


def get_pedidos_producto_cliente_mes_url(fecha):
    response = requests.get(f"{BASE_URL}/orders/group-by-product-client-month/{fecha}/url")
    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Error al obtener la URL de los pedidos por producto y cliente para la fecha {fecha}:", response.text)


def pedidos_sync():
    response = requests.post(f"{BASE_URL}/orders/sync", timeout=6000)
    if response.status_code == 200:
        print("Datos de pedidos sincronizados correctamente")
    else:
        print("Error en la sincronización de pedidos:", response.text)

def get_productos():
    response = requests.get(f"{BASE_URL}/products", timeout=6000)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener los productos", response.text)


def get_clientes():
    response = requests.get(f"{BASE_URL}/clients", timeout=6000)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener los clientes", response.text)

        
if __name__ == "__main__":

    #fecha = "2025-03"
    data = get_productos()
    from pprint import pprint

    filtrados = [
    {k: item[k] for k in ['ref', 'label', 'multilangs'] if k in item}
    for item in data
    ]

    pprint(filtrados[76])