from typing import Any, Dict, Optional, List
import os
import urllib.parse

from pulpo.util.util import require_env
from pulpo.auth.general import MicroTokenManager, MicroHttpClient

CLIENT_ID = require_env("CLIENT_ID_ERPDOLIBARR")
CLIENT_SECRET = require_env("CLIENT_SECRET_ERPDOLIBARR")

## URL de la API del micro wrapper de Dolibarr, que a su vez se conecta con el ERP de Dolibarr
ERPDOLIBARR_URL = require_env("ERPDOLIBARR_URL")

## API Key para autenticación con DOLIBARR
API_KEY_DOLIBARR = require_env("API_KEY_DOLIBARR")


def _float_env(var_name: str, default: float) -> float:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


ERPDOLIBARR_TIMEOUT = _float_env("ERPDOLIBARR_TIMEOUT", 120.0)
ERPDOLIBARR_SLOW_TIMEOUT = _float_env("ERPDOLIBARR_SLOW_TIMEOUT", 300.0)
ERPDOLIBARR_CLIENTS_TIMEOUT = _float_env(
    "ERPDOLIBARR_CLIENTS_TIMEOUT",
    ERPDOLIBARR_SLOW_TIMEOUT
)

class ERPProxySincrono:

    def __init__(
        self,
        base_url: str = ERPDOLIBARR_URL,
        api_key: Optional[str] = API_KEY_DOLIBARR,
        timeout: float = ERPDOLIBARR_TIMEOUT,
        slow_timeout: float = ERPDOLIBARR_SLOW_TIMEOUT
    ):

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.slow_timeout = slow_timeout

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
        kwargs.setdefault("timeout", self.timeout)
        return self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _post(self, path: str, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return self.client.post(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )
    
    def _put(self, path: str, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return self.client.put(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _patch(self, path: str, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
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

    def crear_factura_shipment(
        self,
        shipment_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/shipments/{shipment_id}/invoice",
            json=payload or {}
        )

    def crear_documento_shipment(
        self,
        shipment_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/shipments/{shipment_id}/create/document",
            json=payload or {}
        )

    def descargar_documento_shipment(
        self,
        shipment_id: int
    ):
        return self._get(
            f"/shipments/{shipment_id}/document/download"
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
        return self._post(
            "/orders/sync",
            timeout=self.slow_timeout
        )

    def pedido_por_ref(self, order_ref: str):
        ref_encoded = urllib.parse.quote(
            order_ref,
            safe=""
        )

        return self._get(
            f"/orders/by-ref/{ref_encoded}"
        )

    def pedido(
        self,
        order_id: int
    ):
        return self._get(
            f"/orders/{order_id}"
        )

    def crear_factura_pedido(
        self,
        order_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/orders/{order_id}/invoice",
            json=payload or {}
        )

    def marcar_pedido_facturado(
        self,
        order_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/orders/{order_id}/mark-billed",
            json=payload or {}
        )

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
            f"/products/{product_id}/orders/customers",
            timeout=self.slow_timeout
        )

    def producto_compras(
        self,
        product_id: int
    ):
        return self._get(
            f"/products/{product_id}/orders/suppliers"
        )

    def actualizar_barcode_producto(
        self,
        product_id: int,
        barcode: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None
    ):
        body = payload if payload is not None else {"barcode": barcode}
        return self._patch(
            f"/products/{product_id}/barcode",
            json=body
        )

    # ============================================================
    # Projects
    # ============================================================

    def proyectos(self):
        return self._get("/projects")

    def buscar_proyectos(
        self,
        **params
    ):
        return self._get(
            "/projects/search",
            params=params or None
        )

    # ============================================================
    # Clients
    # ============================================================

    def clientes(self):
        return self._get(
            "/clients",
            timeout=ERPDOLIBARR_CLIENTS_TIMEOUT
        )

    def clientes_con_contactos(self):
        return self._get(
            "/clients/with-contacts",
            timeout=ERPDOLIBARR_CLIENTS_TIMEOUT
        )

    def crear_cliente(
        self,
        payload: Dict[str, Any]
    ):
        return self._post(
            "/clients",
            json=payload
        )

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

    def facturas(
        self,
        **params
    ):
        return self._get(
            "/invoices",
            params=params or None
        )

    def crear_factura_venta_personalizada(
        self,
        payload: Dict[str, Any]
    ):
        return self._post(
            "/invoices/sales/custom",
            json=payload
        )

    def facturas_por_ref_cliente(
        self,
        ref_client: str,
        **params
    ):
        ref_encoded = urllib.parse.quote(
            ref_client,
            safe=""
        )
        return self._get(
            f"/invoices/by-ref-client/{ref_encoded}",
            params=params or None
        )

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

    def crear_documento_factura(
        self,
        invoice_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/invoices/{invoice_id}/create/document",
            json=payload or {}
        )

    def descargar_documento_factura(
        self,
        invoice_id: int
    ):
        return self._get(
            f"/invoices/{invoice_id}/document/download"
        )


    # ============================================================
    # Proposal
    # ============================================================

    def propuestas_abiertas(
        self,
        **params
    ):
        return self._get(
            "/proposals/open",
            params=params or None
        )

    def propuesta_por_ref(
        self,
        proposal_ref: str
    ):
        ref_encoded = urllib.parse.quote(
            proposal_ref,
            safe=""
        )
        return self._get(
            f"/proposals/by-ref/{ref_encoded}"
        )

    def propuesta_por_ref_cliente(
        self,
        ref_client: str
    ):
        ref_encoded = urllib.parse.quote(
            ref_client,
            safe=""
        )
        return self._get(
            f"/proposals/by-ref-client/{ref_encoded}"
        )

    def crear_documento_propuesta_por_id(
        self,
        proposal_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/proposals/{proposal_id}/create/document",
            json=payload or {}
        )

    def descargar_documento_propuesta_por_id(
        self,
        proposal_id: int
    ):
        return self._get(
            f"/proposals/{proposal_id}/document/download"
        )

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

    def validar_propuesta_por_ref(
        self,
        proposal_ref: str,
        payload: Optional[Dict[str, Any]] = None
    ):
        ref_encoded = urllib.parse.quote(
            proposal_ref,
            safe=""
        )
        return self._post(
            f"/proposal/by-ref/{ref_encoded}/validate",
            json=payload or {}
        )

    def validar_propuesta_por_ref_cliente(
        self,
        ref_client: str,
        payload: Optional[Dict[str, Any]] = None
    ):
        ref_encoded = urllib.parse.quote(
            ref_client,
            safe=""
        )
        return self._post(
            f"/proposal/by-ref-client/{ref_encoded}/validate",
            json=payload or {}
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

    def contactos_propuesta(
        self,
        proposal_id: int,
        payload: Dict[str, Any]
    ):
        return self._post(
            f"/proposal/{proposal_id}/contacts",
            json=payload
        )

    def add_proposal_contact(
        self,
        proposal_id: int,
        payload: Dict[str, Any]
    ):
        return self.contactos_propuesta(
            proposal_id,
            payload
        )

    # ============================================================
    # Admin
    # ============================================================

    def health(self):
        return self._get("/health")
