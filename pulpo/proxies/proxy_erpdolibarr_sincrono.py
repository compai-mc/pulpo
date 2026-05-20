from typing import Any, Dict, Optional, List
import urllib.parse

from pulpo.util.util import require_env
from pulpo.auth.general import MicroTokenManager, MicroHttpClient


CLIENT_ID = require_env("CLIENT_ID_ERPDOLIBARR")
CLIENT_SECRET = require_env("CLIENT_SECRET_ERPDOLIBARR")
URL_DOLIBARR = require_env("URL_DOLIBARR")
API_KEY_DOLIBARR = require_env("API_KEY_DOLIBARR")


class ERPProxySincrono:

    def __init__(self, base_url: str = URL_DOLIBARR, api_key: Optional[str] = API_KEY_DOLIBARR):

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

    # ============================================================
    # Helpers
    # ============================================================

    def _get(self, path: str, **kwargs):
        return self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _post(self, path: str, **kwargs):
        return self.client.post(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )
    
    def _put(self, path: str, **kwargs):
        return self.client.put(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _patch(self, path: str, **kwargs):
        return self.client.patch(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    # ============================================================
    # Shipments
    # ============================================================

    def shipments(self):
        return self._get("/shipments")

    def shipment_parametro(
        self,
        shipment_id: int,
        parametro: str
    ):
        return self._get(
            f"/shipments/{shipment_id}/parametro/{parametro}"
        )

    def actualizar_shipment_parametro(
        self,
        shipment_id: int,
        parametro: str,
        valor: Any
    ):
        return self._patch(
            f"/shipments/{shipment_id}/parametro/{parametro}",
            json={"value": valor}
        )

    # ============================================================
    # Orders
    # ============================================================

    def pedidos(self, fecha: str):
        return self._get(f"/orders/{fecha}/url")

    def pedidos_producto_cliente_mes(
        self,
        fecha: str
    ):
        return self._get(
            f"/orders/group-by-product-client-month/{fecha}/url"
        )

    def pedidos_sync(self):
        return self._post("/orders/sync")

    # ============================================================
    # Products
    # ============================================================

    def productos(self):
        return self._get("/products")

    def producto(self, product_id: int):
        return self._get(f"/products/{product_id}")

    def producto_stock(self, product_ref: str):

        ref = urllib.parse.quote(
            product_ref,
            safe=""
        )

        return self._get(
            f"/products/{ref}/stock"
        )

    def producto_proveedores(
        self,
        product_id: int
    ):
        return self._get(
            f"/products/{product_id}/suppliers"
        )

    def producto_clientes(
        self,
        product_id: int
    ):
        return self._get(
            f"/products/{product_id}/orders/customers"
        )

    def producto_compras(
        self,
        product_id: int
    ):
        return self._get(
            f"/products/{product_id}/orders/suppliers"
        )

    # ============================================================
    # Clients
    # ============================================================

    def clientes(self):
        return self._get("/clients")

    def cliente_por_telefono(
        self,
        phone: str
    ):
        return self._get(
            f"/clients/by-phone/{phone}"
        )

    def guardar_telefono_cliente(
        self,
        payload: dict
    ):
        return self._post(
            "/client-phone",
            json=payload
        )

    # ============================================================
    # Contacts
    # ============================================================

    def contactos(self):
        return self._get("/contacts")

    # ============================================================
    # Suppliers
    # ============================================================

    def proveedores(self):
        return self._get("/suppliers")

    def proveedor(
        self,
        supplier_id: int
    ):
        return self._get(
            f"/suppliers/{supplier_id}"
        )

    # ============================================================
    # Banks
    # ============================================================

    def bancos(self):
        return self._get("/banks")

    def movimientos_contables(self):
        return self._get(
            "/banks/accounting-movements"
        )

    def gastos_bancarios(self):
        return self._get(
            "/banks/expenses"
        )

    def cuenta_bancaria(
        self,
        account_id: int
    ):
        return self._get(
            f"/banks/{account_id}"
        )

    def movimientos_cuenta(
        self,
        account_id: int
    ):
        return self._get(
            f"/banks/{account_id}/lines"
        )

    # ============================================================
    # Invoices
    # ============================================================

    def facturas_venta(self, limit: int = 100, page: int = 0, from_date: str = None, to_date: str = None, include_raw: bool = False):
        return self._get(
            "/invoices/sales",
            params={
                "limit": limit,
                "page": page,
                "from_date": from_date,
                "to_date": to_date,
                "include_raw": include_raw
            }
        )

    def facturas_compra(self, limit: int = 100, page: int = 0, from_date: str = None, to_date: str = None, include_raw: bool = False):
        return self._get(
            "/invoices/purchase",
            params={
                "limit": limit,
                "page": page,
                "from_date": from_date,
                "to_date": to_date,
                "include_raw": include_raw
            }
        )

    def facturas_periodo(
        self,
        start: str,
        end: str
    ):
        return self._get(
            "/invoices/by-date",
            params={
                "start": start,
                "end": end
            }
        )
    
    def actualizar_conciliacion_factura(
        self,
        invoice_id: int,
        estado=None,
        fecha_conciliacion=None,
        fecha_expiracion=None
    ):
        
        payload = {}

        if estado:
            payload["estado"] = estado

        if fecha_conciliacion:
            payload["fecha_conciliacion"] = fecha_conciliacion.strftime("%Y-%m-%d %H:%M:%S")

        if fecha_expiracion:
            payload["fecha_expiracion"] = fecha_expiracion.strftime("%Y-%m-%d")

        return self._put(
            f"/invoices/{invoice_id}/conciliacion",
            json=payload
        )


    # ============================================================
    # Proposal
    # ============================================================

    def crear_presupuesto(
        self,
        payload: Dict[str, Any]
    ):
        return self._post(
            "/proposal",
            json=payload
        )

    def validar_propuesta(
        self,
        proposal_id: int
    ):
        return self._post(
            f"/proposal/{proposal_id}/validate"
        )

    def confirmar_propuesta(
        self,
        proposal_id: int,
        validate_order: bool = False
    ):
        return self._post(
            f"/proposal/{proposal_id}/confirm",
            params={
                "validate_order": validate_order
            }
        )

    # ============================================================
    # Admin
    # ============================================================

    def health(self):
        return self._get("/health")