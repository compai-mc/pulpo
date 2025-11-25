import httpx
from typing import Any, Dict, Optional, List
from pulpo.util.util import require_env

#from config import config, config_global

def _float_env(var_name: str) -> float | None:
    """Devuelve el valor float de una variable de entorno, o None si no existe o no es válido."""
    value = require_env(var_name)
    if value is None:
        print(f"ℹ️ {var_name} no definida.")
        return None
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ Valor inválido para {var_name}: '{value}'.")
        return None


ERP_TIMEOUT_CONNECT = _float_env("ERP_TIMEOUT_CONNECT")
ERP_TIMEOUT_READ =  _float_env("ERP_TIMEOUT_READ")
ERP_TIMEOUT_WRITE =  _float_env("ERP_TIMEOUT_WRITE")
ERP_TIMEOUT_POOL = _float_env("ERP_TIMEOUT_POOL")

URL_DOLIBARR = require_env("ERPDOLIBARR_URL")
API_KEY_DOLIBARR = require_env("API_KEY_DOLIBARR")

class ERPProxySincrono:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["DOLAPIKEY"] = api_key

        timeout = httpx.Timeout(
            connect=ERP_TIMEOUT_CONNECT,
            read=ERP_TIMEOUT_READ,
            write=ERP_TIMEOUT_WRITE,
            pool=ERP_TIMEOUT_POOL,
        )
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout, headers=headers)

    def close(self):
        self.client.close()

    # ---------------- Orders ----------------
    def pedidos(self, fecha: str) -> Dict[str, Any]:
        r = self.client.get(f"/orders/{fecha}/url")
        return r.json()

    def pedidos_producto_cliente_mes(self, fecha: str) -> Dict[str, Any]:
        r = self.client.get(f"/orders/group-by-product-client-month/{fecha}/url")
        return r.json()

    def pedidos_sync(self) -> Dict[str, Any]:
        r = self.client.post("/orders/sync")
        return r.json()

    # ---------------- Products ----------------
    def productos(self) -> List[Dict[str, Any]]:
        r = self.client.get("/products")
        return r.json()

    def obtener_stock_producto_codificado(self, product_ref: str) -> Dict[str, Any]:

        import urllib.parse
        ref_encoded = urllib.parse.quote(product_ref, safe="")
        r = self.client.get(f"/products/{ref_encoded}/stock")
        return r.json()
    
    
    def obtener_stock_producto(self, product_ref: str) -> Dict[str, Any]:
        r = self.client.get(f"/products/{product_ref}/stock")
        return r.json()


    # ---------------- Clients ----------------
    def clientes(self) -> List[Dict[str, Any]]:
        r = self.client.get("/clients")
        return r.json()

    # ---------------- Proposal ----------------
    def crear_presupuesto(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.client.post("/proposal", json=payload)
        return r.json()

    def proposal_create_document(self, name: str) -> Dict[str, Any]:
        """
        Devuelve JSON con metadata y base64 del PDF.
        {
            "filename": "...",
            "content_type": "...",
            "content_base64": "...",
            "download_url": "..."
        }
        """
        r = self.client.post(f"/proposal/{name}/create/document")
        return r.json()

    def download_proposal_document(self, name: str) -> bytes:
        """Descarga directa del PDF binario."""
        r = self.client.get(f"/proposal/{name}/document/download")
        return r.content if r.status_code == 200 else b""

    # ---------------- Health ----------------
    def health(self) -> Dict[str, Any]:
        r = self.client.get("/health")
        return r.json()


# ---------------- Ejemplo de uso ----------------
if __name__ == "__main__":
    

    """ doc_info = client.proposal_create_document("(PROV10384)")
        print("Info documento:", doc_info)

        pdf_bytes = client.download_proposal_document("(PROV10384)")
        if pdf_bytes:
            with open("propuesta.pdf", "wb") as f:
                f.write(pdf_bytes)
            print("PDF guardado en propuesta.pdf")

        client.close()
    """

    client = ERPProxySincrono(URL_DOLIBARR, API_KEY_DOLIBARR)
    unco = "(C-1730)_ME0000001-1-899"

    a = client.obtener_stock_producto(unco)
    print(a)

