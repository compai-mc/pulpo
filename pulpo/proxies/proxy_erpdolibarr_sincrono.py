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

    def crear_shipment(
        self,
        payload: Dict[str, Any]
    ):
        return self._post(
            "/shipments",
            json=payload
        )

    def shipment_por_pedido(
        self,
        origin_id: int
    ):
        return self._get(
            f"/shipments/by-order/{origin_id}"
        )

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

    def validar_shipment(
        self,
        shipment_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/shipments/{shipment_id}/validate",
            json=payload or {}
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

    def producto_por_ref(self, ref: str):

        ref_encoded = urllib.parse.quote(
            ref,
            safe=""
        )

        return self._get(
            f"/products/ref/{ref_encoded}"
        )

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

    def clientes_con_contactos(self):
        return self._get("/clients/with-contacts")

    def cliente(
        self,
        client_id: int
    ):
        return self._get(
            f"/clients/{client_id}"
        )

    def contactos_cliente(
        self,
        client_id: int
    ):
        return self._get(
            f"/clients/{client_id}/contacts"
        )

    def crear_contacto_cliente(
        self,
        client_id: int,
        payload: Dict[str, Any]
    ):
        return self._post(
            f"/clients/{client_id}/contacts",
            json=payload
        )

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

    def gastos_comisiones_bancarias(self):
        return self._get(
            "/banks/expenses/bank-fees"
        )

    def gastos_intereses_financieros(self):
        return self._get(
            "/banks/expenses/financial-interest"
        )

    def gastos_seguros(self):
        return self._get(
            "/banks/expenses/insurance"
        )

    def cancelaciones_financieras(self):
        return self._get(
            "/banks/financial-cancellations"
        )

    def transferencias_bancarias(self):
        return self._get(
            "/banks/transfers"
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

    def facturas(self, period: Optional[str] = None):
        params = {}

        if period:
            params["period"] = period

        return self._get(
            "/invoices",
            params=params or None
        )

    def facturas_periodo_actual(self):
        return self.facturas(
            period="current_year"
        )

    def facturas_venta(self):
        return self._get(
            "/invoices/sales"
        )

    def facturas_compra(self):
        return self._get(
            "/invoices/purchase"
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

    def pagos_facturas_bancos(self):
        return self._get(
            "/invoices/payments/banks"
        )

    def pagos_factura(
        self,
        invoice_id: int
    ):
        return self._get(
            f"/invoices/{invoice_id}/payments"
        )

    def pagos_factura_bancos(
        self,
        invoice_id: int
    ):
        return self._get(
            f"/invoices/{invoice_id}/payments/banks"
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

    def propuesta(
        self,
        proposal_id: int
    ):
        return self._get(
            f"/proposal/{proposal_id}"
        )

    def crear_documento_propuesta(
        self,
        name: str,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/proposal/{name}/create/document",
            json=payload or {}
        )

    def descargar_documento_propuesta(
        self,
        name: str
    ):
        return self._get(
            f"/proposal/{name}/document/download"
        )

    def actualizar_extrafields_propuesta(
        self,
        proposal_id: int,
        payload: Dict[str, Any]
    ):
        return self._patch(
            f"/proposal/{proposal_id}/extrafields",
            json=payload
        )

    def lineas_propuesta(
        self,
        proposal_id: int
    ):
        return self._get(
            f"/proposal/{proposal_id}/lines"
        )

    def crear_linea_propuesta(
        self,
        proposal_id: int,
        payload: Dict[str, Any]
    ):
        return self._post(
            f"/proposal/{proposal_id}/lines",
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
