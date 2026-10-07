from typing import Any, Dict, Optional, List
import base64
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

    def _delete(self, path: str, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return self.client.delete(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _download(self, path: str, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        response = self.client._request(
            "GET",
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type:
            return response.json()

        return {
            "content_type": content_type or None,
            "content_length": len(response.content),
            "content_base64": base64.b64encode(response.content).decode("ascii"),
        }

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

    def shipment_por_ref(
        self,
        shipment_ref: str
    ):
        ref_encoded = urllib.parse.quote(
            shipment_ref,
            safe=""
        )
        return self._get(
            f"/shipments/by-ref/{ref_encoded}"
        )

    def shipment_por_ref_full(
        self,
        shipment_ref: str
    ):
        ref_encoded = urllib.parse.quote(
            shipment_ref,
            safe=""
        )
        return self._get(
            f"/shipments/by-ref/{ref_encoded}/full"
        )

    def crear_shipment_parcial(
        self,
        payload: Dict[str, Any]
    ):
        return self._post(
            "/shipments/partial",
            json=payload
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
        return self._download(
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

    def pedido_por_ref_cliente(
        self,
        ref_client: str
    ):
        ref_encoded = urllib.parse.quote(
            ref_client,
            safe=""
        )
        return self._get(
            f"/orders/by-ref-client/{ref_encoded}"
        )

    def pedidos_por_fecha(
        self,
        fecha: str,
        **params
    ):
        return self._get(
            f"/orders/by-date/{fecha}",
            params=params or None
        )

    def actualizar_pedido_por_ref(
        self,
        order_ref: str,
        payload: Dict[str, Any]
    ):
        ref_encoded = urllib.parse.quote(
            order_ref,
            safe=""
        )
        return self._patch(
            f"/orders/by-ref/{ref_encoded}",
            json=payload
        )

    def validar_pedido_por_ref(
        self,
        order_ref: str,
        payload: Optional[Dict[str, Any]] = None
    ):
        ref_encoded = urllib.parse.quote(
            order_ref,
            safe=""
        )
        return self._post(
            f"/orders/by-ref/{ref_encoded}/validate",
            json=payload or {}
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

    def documentos_relacionados_pedido(self, order_id: int, **params):
        return self._get(
            f"/orders/{order_id}/related-documents",
            params=params or None
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

    def validar_pedido(
        self,
        order_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/orders/{order_id}/validate",
            json=payload or {}
        )

    def crear_documento_pedido(
        self,
        order_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/orders/{order_id}/create/document",
            json=payload or {}
        )

    def descargar_documento_pedido(
        self,
        order_id: int,
        generate_if_missing: bool = False
    ):
        return self._download(
            f"/orders/{order_id}/document/download",
            params={"generate_if_missing": generate_if_missing}
        )

    def actualizar_productos_pedido(
        self,
        order_id: int,
        payload: Dict[str, Any]
    ):
        return self._patch(
            f"/orders/{order_id}/products",
            json=payload
        )

    # ============================================================
    # Products
    # ============================================================

    def productos(self):
        return self._get("/products")

    def productos_padre_con_variantes(self, **params):
        return self._get("/products/parents-with-variants", params=params or None)

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
    # Geography / Warehouses
    # ============================================================

    def paises(self, **params):
        return self._get("/countries", params=params or None)

    def provincias_pais(self, country_id: int, **params):
        return self._get(f"/countries/{country_id}/states", params=params or None)

    def provincias(self, **params):
        return self._get("/states", params=params or None)

    def almacenes(self):
        return self._get("/warehouses")

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

    def pedidos_proveedor(
        self,
        **params
    ):
        return self._get(
            "/supplier-orders",
            params=params or None
        )

    def pedido_proveedor(
        self,
        supplier_order_id: int
    ):
        return self._get(
            f"/supplier-orders/{supplier_order_id}"
        )

    def pedido_proveedor_por_ref(
        self,
        supplier_order_ref: str
    ):
        ref_encoded = urllib.parse.quote(
            supplier_order_ref,
            safe=""
        )
        return self._get(
            f"/supplier-orders/by-ref/{ref_encoded}"
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

    def pagos_factura_compra(self, invoice_id: int, **params):
        return self._get(
            f"/invoices/purchase/{invoice_id}/payments",
            params=params or None
        )

    def datos_verifactu_factura(self, invoice_id: int):
        return self._get(f"/invoices/{invoice_id}/verifactu-data")

    def pagos_factura_bancos(
        self,
        invoice_id: int
    ):
        return self._get(
            f"/invoices/{invoice_id}/payments/banks"
        )

    def validar_factura(
        self,
        invoice_id: int,
        payload: Optional[Dict[str, Any]] = None
    ):
        return self._post(
            f"/invoices/{invoice_id}/validate",
            json=payload or {}
        )

    def validar_factura_por_ref(
        self,
        invoice_ref: str,
        payload: Optional[Dict[str, Any]] = None
    ):
        ref_encoded = urllib.parse.quote(
            invoice_ref,
            safe=""
        )
        return self._post(
            f"/invoices/by-ref/{ref_encoded}/validate",
            json=payload or {}
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
        return self._download(
            f"/invoices/{invoice_id}/document/download"
        )

    def preview_documento_factura(
        self,
        invoice_id: int
    ):
        return self._download(
            f"/invoices/{invoice_id}/document/preview"
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

    def actualizar_propuesta_por_ref(
        self,
        proposal_ref: str,
        payload: Dict[str, Any]
    ):
        ref_encoded = urllib.parse.quote(
            proposal_ref,
            safe=""
        )
        return self._patch(
            f"/proposals/by-ref/{ref_encoded}",
            json=payload
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

    def adjuntar_documentos_propuesta(self, proposal_id: int, payload: Dict[str, Any]):
        """Payload: documentos con nombre_fichero y contenido_base64."""
        return self._post(f"/proposals/{proposal_id}/documents", json=payload)

    def descargar_documento_propuesta_por_id(
        self,
        proposal_id: int
    ):
        return self._download(
            f"/proposals/{proposal_id}/document/download"
        )

    def actualizar_productos_propuesta(
        self,
        proposal_id: int,
        payload: Dict[str, Any]
    ):
        return self._patch(
            f"/proposals/{proposal_id}/products",
            json=payload
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
        return self._download(
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

    def firmar_propuesta(
        self, proposal_id: int, payload: Optional[Dict[str, Any]] = None, **params
    ):
        return self._post(
            f"/proposal/{proposal_id}/sign",
            json=payload or {},
            params=params or None
        )

    def firmar_propuesta_por_ref(
        self, proposal_ref: str, payload: Optional[Dict[str, Any]] = None, **params
    ):
        ref_encoded = urllib.parse.quote(proposal_ref, safe="")
        return self._post(
            f"/proposal/by-ref/{ref_encoded}/sign",
            json=payload or {},
            params=params or None
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

    def obtener_contactos_propuesta(
        self,
        proposal_id: int
    ):
        return self._get(
            f"/proposal/{proposal_id}/contacts"
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

    def reemplazar_contactos_propuesta(
        self,
        proposal_id: int,
        payload: Dict[str, Any]
    ):
        return self._put(
            f"/proposal/{proposal_id}/contacts/replace",
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
    # Native purchase queries (distinct from /supplier-orders)
    # ============================================================

    def terceros(self, **params):
        return self._get("/thirdparties", params=params or None)

    def tercero(self, thirdparty_id: int):
        return self._get(f"/thirdparties/{thirdparty_id}")

    def pedidos_proveedor_nativos(self, **params):
        return self._get("/supplierorders", params=params or None)

    def pedido_proveedor_nativo(self, supplier_order_id: int):
        return self._get(f"/supplierorders/{supplier_order_id}")

    def recepciones(self, **params):
        return self._get("/receptions", params=params or None)

    def recepcion(self, reception_id: int):
        return self._get(f"/receptions/{reception_id}")

    def facturas_proveedor_nativas(self, **params):
        return self._get("/supplierinvoices", params=params or None)

    def factura_proveedor_nativa(self, invoice_id: int):
        return self._get(f"/supplierinvoices/{invoice_id}")

    def crear_factura_proveedor(self, payload: Dict[str, Any]):
        """Crea un borrador con socid y ref_supplier; no valida ni reintenta."""
        return self._post("/supplierinvoices", json=payload)

    def crear_linea_factura_proveedor(self, invoice_id: int, payload: Dict[str, Any]):
        """Envia description, pu_ht, qty, tva_tx y los campos adicionales de la linea."""
        return self._post(f"/supplierinvoices/{invoice_id}/lines", json=payload)

    def validar_factura_proveedor(
        self, invoice_id: int, payload: Optional[Dict[str, Any]] = None
    ):
        """Valida con idwarehouse y notrigger opcionales en el cuerpo JSON."""
        return self._post(f"/supplierinvoices/{invoice_id}/validate", json=payload or {})

    def actualizar_factura_proveedor(self, invoice_id: int, payload: Dict[str, Any]):
        """Modifica la cabecera; el micro no permite cambiar ID, estado ni lineas."""
        return self._put(f"/supplierinvoices/{invoice_id}", json=payload)

    def actualizar_linea_factura_proveedor(
        self, invoice_id: int, line_id: int, payload: Dict[str, Any]
    ):
        """PUT de la linea completa, incluidos los valores que se quieran conservar."""
        return self._put(f"/supplierinvoices/{invoice_id}/lines/{line_id}", json=payload)

    def eliminar_linea_factura_proveedor(self, invoice_id: int, line_id: int):
        return self._delete(f"/supplierinvoices/{invoice_id}/lines/{line_id}")

    def subir_documento(self, payload: Dict[str, Any]):
        """Envia filename, modulepart, ref y filecontent (base64) como JSON.

        Para facturas de proveedor: modulepart=supplier_invoice y ref del ERP,
        no ref_supplier. El micro sobrescribe el mismo nombre sin subdirectorios.
        """
        return self._post("/documents/upload", json=payload)

    def condiciones_pago(self, **params):
        """Filtros: limit, page, sortorder, sqlfilters, active y sortfield."""
        return self._get("/setup/dictionary/payment_terms", params=params or None)

    def formas_pago(self, **params):
        """Filtros: limit, page, sortorder, sqlfilters, active y sortfield."""
        return self._get("/setup/dictionary/payment_types", params=params or None)

    def tipos_iva(self, **params):
        """Filtros de diccionario y fk_country (-1 empresa, 0 todos o ID de pais)."""
        return self._get("/setup/dictionary/vat", params=params or None)

    def monedas(self, **params):
        """Filtros de diccionario y multicurrency (0 monedas, 1/2 cambios)."""
        return self._get("/setup/dictionary/currencies", params=params or None)

    def documentos(self, modulepart: str, **params):
        """Lista metadatos; indicar id o ref y los filtros de consulta."""
        return self._get("/documents", params={"modulepart": modulepart, **params})

    # ============================================================
    # MCP business queries
    # Optional filters and pagination (limit, next_cursor) go in **params.
    # ============================================================

    def mcp_terceros(self, **params):
        return self._get("/mcp/thirdparties", params=params or None)

    def mcp_contactos(self, company: str, **params):
        return self._get("/mcp/contacts", params={"company": company, **params})

    def mcp_productos(self, **params):
        return self._get("/mcp/products", params=params or None)

    def mcp_stock_producto(self, product: str, **params):
        return self._get("/mcp/products/stock", params={"product": product, **params})

    def mcp_precio_producto(self, product: str, customer: str, quantity, **params):
        return self._get(
            "/mcp/products/price",
            params={"product": product, "customer": customer, "quantity": quantity, **params}
        )

    def mcp_proveedores_producto(self, product: str, **params):
        return self._get("/mcp/products/suppliers", params={"product": product, **params})

    def mcp_pedidos(self, **params):
        return self._get("/mcp/orders", params=params or None)

    def mcp_detalle_pedido(self, **params):
        return self._get("/mcp/orders/detail", params=params or None)

    def mcp_propuestas(self, **params):
        return self._get("/mcp/proposals", params=params or None)

    def mcp_detalle_propuesta(self, **params):
        return self._get("/mcp/proposals/detail", params=params or None)

    def mcp_facturas(self, type: str, **params):
        """type: sale o purchase."""
        return self._get("/mcp/invoices", params={"type": type, **params})

    def mcp_detalle_factura(self, type: str, **params):
        """type: sale o purchase."""
        return self._get("/mcp/invoices/detail", params={"type": type, **params})

    def mcp_cobros_clientes(self, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/customer-payments",
            params={"from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_pagos_proveedores(self, supplier: str, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/supplier-payments",
            params={"supplier": supplier, "from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_cuentas_bancarias(self, **params):
        return self._get("/mcp/bank-accounts", params=params or None)

    def mcp_movimientos_bancarios(self, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/bank-movements",
            params={"from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_gastos_bancarios(self, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/bank-expenses",
            params={"from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_transferencias_internas(self, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/internal-transfers",
            params={"from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_informe_ventas(self, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/reports/sales",
            params={"from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_informe_ventas_producto(self, from_date: str, to_date: str, **params):
        return self._get(
            "/mcp/reports/product-sales",
            params={"from_date": from_date, "to_date": to_date, **params}
        )

    def mcp_informe_pagos(self, direction: str, from_date: str, to_date: str, **params):
        """direction: incoming u outgoing."""
        return self._get(
            "/mcp/reports/payments",
            params={"direction": direction, "from_date": from_date, "to_date": to_date, **params}
        )

    # ============================================================
    # Admin
    # ============================================================

    def health(self):
        return self._get("/health")

    def openapi(self):
        return self._get("/openapi.json")

    def docs(self):
        return self.client._request(
            "GET", f"{self.base_url}/docs", headers=self.headers, timeout=self.timeout
        ).text

    def docs_oauth2_redirect(self):
        return self.client._request(
            "GET", f"{self.base_url}/docs/oauth2-redirect",
            headers=self.headers, timeout=self.timeout
        ).text

    def redoc(self):
        return self.client._request(
            "GET", f"{self.base_url}/redoc", headers=self.headers, timeout=self.timeout
        ).text
