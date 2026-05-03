import httpx
from typing import Any, Dict, Optional, List
import urllib.parse

from pulpo.util.util import require_env
from pulpo.auth.general import MicroTokenManager, MicroHttpClient

CLIENT_ID = require_env("CLIENT_ID_ERPDOLIBARR")
CLIENT_SECRET = require_env("CLIENT_SECRET_ERPDOLIBARR")


class ERPProxySincrono:

    def __init__(self, base_url: str, api_key: Optional[str] = None):

        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }

        if api_key:
            self.headers["DOLAPIKEY"] = api_key

        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )

        self.client = MicroHttpClient(self.tm)

    # ---------------- Orders ----------------
    def pedidos(self, fecha: str) -> Dict[str, Any]:
        return  self.client.get(
            f"{self.base_url}/orders/{fecha}/url",
            headers=self.headers
        )

    def pedidos_producto_cliente_mes(self, fecha: str) -> Dict[str, Any]:
        return self.client.get(f"{self.base_url}/orders/group-by-product-client-month/{fecha}/url",
                            headers=self.headers)

    def pedidos_sync(self) -> Dict[str, Any]:
        return self.client.post(f"{self.base_url}/orders/sync", headers=self.headers)

    # ---------------- Products ----------------
    def productos(self) -> List[Dict[str, Any]]:
        return self.client.get(
            f"{self.base_url}/products", 
            headers=self.headers
            )

    def obtener_stock_producto_codificado(self, product_ref: str) -> Dict[str, Any]:

        ref_encoded = urllib.parse.quote(product_ref, safe="")
        return self.client.get(f"{self.base_url}/products/{ref_encoded}/stock", headers=self.headers)    
    
    def obtener_stock_producto(self, product_ref: str) -> Dict[str, Any]:
        return self.client.get(f"{self.base_url}/products/{product_ref}/stock", headers=self.headers)

    # ---------------- Clients ----------------
    def clientes(self) -> List[Dict[str, Any]]:
        return self.client.get(f"{self.base_url}/clients", headers=self.headers)

    # ---------------- Proposal ----------------
    def crear_presupuesto(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.post(f"{self.base_url}/proposal", json=payload, headers=self.headers)

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
        return self.client.post(
            f"{self.base_url}/proposal/{name}/create/document",
            headers=self.headers
            )

    def confirmar_propuesta(self, proposal_id: int, validate_order: bool = False) -> Dict[str, Any]:
        """
        Confirma una propuesta en el ERP Dolibarr y (opcionalmente) genera el pedido.

        Endpoint: POST /proposal/{proposal_id}/confirm?validate_order={true|false}
        """
        params = {"validate_order": str(validate_order).lower()}

        try:
            return self.client.post(
                        f"{self.base_url}/proposal/{proposal_id}/confirm", 
                        params=params, 
                        headers=self.headers)
            
        except httpx.HTTPStatusError as e:
            return {
                "error": e.response.text,
                "status": "failed",
                "http_status": e.response.status_code,
                "url": str(e.request.url),
            }
        
        except httpx.RequestError as e:
            return {
                "error": str(e),
                "status": "failed",
                "http_status": None,
            }
    
    def validar_propuesta(self, proposal_ref: str) -> Dict[str, Any]:
        """
        Valida una propuesta en el ERP Dolibarr.

        Endpoint: POST /proposal/{proposal_ref}/validate
        """
        try:
            return self.client.post(
                f"{self.base_url}/proposal/{proposal_ref}/validate",
                headers=self.headers
            )
        
        except httpx.HTTPStatusError as e:
            return {
                "error": e.response.text,
                "status": "failed",
                "http_status": e.response.status_code,
                "url": str(e.request.url),
            }
        
        except httpx.RequestError as e:
            return {
                "error": str(e),
                "status": "failed",
                "http_status": None,
            }


    def download_proposal_document(self, name: str) -> bytes:

        r = self.client._request(
            "GET",
            f"{self.base_url}/proposal/{name}/document/download",
            headers=self.headers
        )

        return r.content

    # ---------------- Health ----------------
    def health(self) -> Dict[str, Any]:
        return self.client.get(f"{self.base_url}/health", headers=self.headers)


# ---------------- Ejemplo de uso ----------------
if __name__ == "__main__":
    

    pass

    """ doc_info = client.proposal_create_document("(PROV10384)")
        print("Info documento:", doc_info)

        pdf_bytes = client.download_proposal_document("(PROV10384)")
        if pdf_bytes:
            with open("propuesta.pdf", "wb") as f:
                f.write(pdf_bytes)
            print("PDF guardado en propuesta.pdf")

        client.close()


    client = ERPProxySincrono(URL_DOLIBARR, API_KEY_DOLIBARR)
    unco = "(C-1730)_ME0000001-1-899"

    a = client.obtener_stock_producto(unco)
    print(a)
    """
