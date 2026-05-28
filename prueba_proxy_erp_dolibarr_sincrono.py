import os
import sys
from datetime import date
from typing import Any, Callable

from prueba_proxies_vault import (
    apply_dev_defaults,
    apply_env_aliases,
    assert_bootstrap_env,
    bootstrap_service_token,
    load_config_service_direct,
    load_vault_recursive,
)


SKIPPED = object()


def summarize(value: Any) -> str:
    text = repr(value)
    if len(text) > 500:
        return text[:500] + "..."
    return text


def as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value

    if isinstance(value, dict):
        for key in (
            "data",
            "items",
            "results",
            "rows",
            "banks",
            "clients",
            "products",
            "invoices",
            "shipments",
            "suppliers",
            "contacts",
            "lines",
            "payments",
        ):
            nested = value.get(key)
            if isinstance(nested, list):
                return nested

    return []


def find_first(items: Any) -> dict[str, Any] | None:
    for item in as_list(items):
        if isinstance(item, dict):
            return item
    return None


def pick(item: dict[str, Any] | None, keys: tuple[str, ...]) -> Any:
    if not item:
        return None

    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return value

    return None


def skip(reason: str) -> object:
    print(f"[SKIP] {reason}")
    return SKIPPED


def require_value(value: Any, reason: str) -> Any:
    if value in (None, ""):
        return skip(reason)
    return value


def run_case(name: str, fn: Callable[[], Any]) -> str:
    print(f"\n[TEST] {name}")

    try:
        result = fn()
    except Exception as exc:
        print("[FAIL]", type(exc).__name__, exc)
        return "FAIL"

    if result is SKIPPED:
        return "SKIP"

    print("[OK]", summarize(result))
    return "OK"


def maybe_mutating(name: str, fn: Callable[[], Any]) -> tuple[str, Callable[[], Any]]:
    if os.getenv("ERP_TEST_MUTATING") == "1":
        return name, fn

    return name, lambda: skip(
        "operacion con efectos secundarios; activa ERP_TEST_MUTATING=1"
    )


def main() -> int:
    load_vault_recursive()
    load_config_service_direct()
    apply_env_aliases()
    apply_dev_defaults()
    assert_bootstrap_env()
    bootstrap_service_token()

    from pulpo.proxies.proxy_erpdolibarr_sincrono import ERPProxySincrono

    base_url = os.getenv(
        "ERP_TEST_BASE_URL",
        "http://alcazar:7404"
    )
    os.environ["ERPDOLIBARR_URL"] = base_url
    print(f"ERP Dolibarr base URL: {base_url}")

    proxy = ERPProxySincrono(base_url)

    fecha = os.getenv("ERP_TEST_FECHA", date.today().strftime("%Y-%m"))
    start = os.getenv("ERP_TEST_START", f"{date.today().year}-01-01")
    end = os.getenv("ERP_TEST_END", date.today().isoformat())

    cache: dict[str, Any] = {}

    def cached(name: str, fn: Callable[[], Any]) -> Any:
        if name not in cache:
            cache[name] = fn()
        return cache[name]

    def banks():
        return cached("banks", proxy.bancos)

    def clients():
        return cached("clients", proxy.clientes)

    def products():
        return cached("products", proxy.productos)

    def invoices():
        return cached("invoices", proxy.facturas)

    def shipments():
        return cached("shipments", proxy.shipments)

    def suppliers():
        return cached("suppliers", proxy.proveedores)

    def bank_id():
        return (
            os.getenv("ERP_TEST_BANK_ACCOUNT_ID")
            or pick(find_first(banks()), ("id", "rowid", "account_id", "bank_id"))
        )

    def client_id():
        return (
            os.getenv("ERP_TEST_CLIENT_ID")
            or pick(find_first(clients()), ("id", "rowid", "client_id", "socid"))
        )

    def client_phone():
        return (
            os.getenv("ERP_TEST_CLIENT_PHONE")
            or pick(find_first(clients()), ("phone", "phone_mobile", "tel", "telefono"))
        )

    def product_id():
        return (
            os.getenv("ERP_TEST_PRODUCT_ID")
            or pick(find_first(products()), ("id", "rowid", "product_id"))
        )

    def product_ref():
        return (
            os.getenv("ERP_TEST_PRODUCT_REF")
            or pick(find_first(products()), ("ref", "reference", "product_ref"))
        )

    def invoice_id():
        return (
            os.getenv("ERP_TEST_INVOICE_ID")
            or pick(find_first(invoices()), ("id", "rowid", "invoice_id", "facid"))
        )

    def proposal_id():
        return os.getenv("ERP_TEST_PROPOSAL_ID")

    def proposal_name():
        return os.getenv("ERP_TEST_PROPOSAL_NAME")

    def shipment_id():
        return (
            os.getenv("ERP_TEST_SHIPMENT_ID")
            or pick(find_first(shipments()), ("id", "rowid", "shipment_id"))
        )

    def shipment_origin_id():
        return (
            os.getenv("ERP_TEST_SHIPMENT_ORIGIN_ID")
            or pick(find_first(shipments()), ("origin_id", "fk_origin", "order_id"))
        )

    def supplier_id():
        return (
            os.getenv("ERP_TEST_SUPPLIER_ID")
            or pick(find_first(suppliers()), ("id", "rowid", "supplier_id", "socid"))
        )

    def shipment_parametro():
        return os.getenv("ERP_TEST_SHIPMENT_PARAMETRO")

    tests: list[tuple[str, Callable[[], Any]]] = [
        ("GET /health", proxy.health),
        ("GET /banks", banks),
        ("GET /banks/accounting-movements", proxy.movimientos_contables),
        ("GET /banks/expenses", proxy.gastos_bancarios),
        ("GET /banks/expenses/bank-fees", proxy.gastos_comisiones_bancarias),
        ("GET /banks/expenses/financial-interest", proxy.gastos_intereses_financieros),
        ("GET /banks/expenses/insurance", proxy.gastos_seguros),
        ("GET /banks/financial-cancellations", proxy.cancelaciones_financieras),
        ("GET /banks/transfers", proxy.transferencias_bancarias),
        (
            "GET /banks/{account_id}",
            lambda: (
                proxy.cuenta_bancaria(bank_id())
                if require_value(bank_id(), "falta ERP_TEST_BANK_ACCOUNT_ID o banco con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /banks/{account_id}/lines",
            lambda: (
                proxy.movimientos_cuenta(bank_id())
                if require_value(bank_id(), "falta ERP_TEST_BANK_ACCOUNT_ID o banco con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        ("GET /clients", clients),
        ("GET /clients/with-contacts", proxy.clientes_con_contactos),
        (
            "GET /clients/by-phone/{phone}",
            lambda: (
                proxy.cliente_por_telefono(client_phone())
                if require_value(client_phone(), "falta ERP_TEST_CLIENT_PHONE o cliente con telefono") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /clients/{client_id}",
            lambda: (
                proxy.cliente(client_id())
                if require_value(client_id(), "falta ERP_TEST_CLIENT_ID o cliente con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /clients/{client_id}/contacts",
            lambda: (
                proxy.contactos_cliente(client_id())
                if require_value(client_id(), "falta ERP_TEST_CLIENT_ID o cliente con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        ("GET /contacts", proxy.contactos),
        ("GET /invoices", invoices),
        ("GET /invoices?period=current_year", proxy.facturas_periodo_actual),
        ("GET /invoices/by-date", lambda: proxy.facturas_periodo(start, end)),
        ("GET /invoices/payments/banks", proxy.pagos_facturas_bancos),
        ("GET /invoices/purchase", proxy.facturas_compra),
        ("GET /invoices/sales", proxy.facturas_venta),
        (
            "GET /invoices/{invoice_id}/payments",
            lambda: (
                proxy.pagos_factura(invoice_id())
                if require_value(invoice_id(), "falta ERP_TEST_INVOICE_ID o factura con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /invoices/{invoice_id}/payments/banks",
            lambda: (
                proxy.pagos_factura_bancos(invoice_id())
                if require_value(invoice_id(), "falta ERP_TEST_INVOICE_ID o factura con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        ("GET /orders/group-by-product-client-month/{fecha}/url", lambda: proxy.pedidos_producto_cliente_mes(fecha)),
        ("GET /orders/{fecha}/url", lambda: proxy.pedidos(fecha)),
        ("GET /products", products),
        (
            "GET /products/ref/{ref:path}",
            lambda: (
                proxy.producto_por_ref(product_ref())
                if require_value(product_ref(), "falta ERP_TEST_PRODUCT_REF o producto con ref") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /products/{product_id}",
            lambda: (
                proxy.producto(product_id())
                if require_value(product_id(), "falta ERP_TEST_PRODUCT_ID o producto con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /products/{product_id}/orders/customers",
            lambda: (
                proxy.producto_clientes(product_id())
                if require_value(product_id(), "falta ERP_TEST_PRODUCT_ID o producto con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /products/{product_id}/orders/suppliers",
            lambda: (
                proxy.producto_compras(product_id())
                if require_value(product_id(), "falta ERP_TEST_PRODUCT_ID o producto con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /products/{product_id}/suppliers",
            lambda: (
                proxy.producto_proveedores(product_id())
                if require_value(product_id(), "falta ERP_TEST_PRODUCT_ID o producto con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /products/{product_ref}/stock",
            lambda: (
                proxy.producto_stock(product_ref())
                if require_value(product_ref(), "falta ERP_TEST_PRODUCT_REF o producto con ref") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /proposal/{proposal_id}",
            lambda: (
                proxy.propuesta(proposal_id())
                if require_value(proposal_id(), "falta ERP_TEST_PROPOSAL_ID") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /proposal/{name}/document/download",
            lambda: (
                proxy.descargar_documento_propuesta(proposal_name())
                if require_value(proposal_name(), "falta ERP_TEST_PROPOSAL_NAME") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /proposal/{proposal_id}/lines",
            lambda: (
                proxy.lineas_propuesta(proposal_id())
                if require_value(proposal_id(), "falta ERP_TEST_PROPOSAL_ID") is not SKIPPED
                else SKIPPED
            ),
        ),
        ("GET /shipments", shipments),
        (
            "GET /shipments/by-order/{origin_id}",
            lambda: (
                proxy.shipment_por_pedido(shipment_origin_id())
                if require_value(shipment_origin_id(), "falta ERP_TEST_SHIPMENT_ORIGIN_ID o shipment con origin_id") is not SKIPPED
                else SKIPPED
            ),
        ),
        (
            "GET /shipments/{shipment_id}/parametro/{parametro:path}",
            lambda: (
                proxy.shipment_parametro(shipment_id(), shipment_parametro())
                if (
                    require_value(shipment_id(), "falta ERP_TEST_SHIPMENT_ID o shipment con id") is not SKIPPED
                    and require_value(shipment_parametro(), "falta ERP_TEST_SHIPMENT_PARAMETRO") is not SKIPPED
                )
                else SKIPPED
            ),
        ),
        ("GET /suppliers", suppliers),
        (
            "GET /suppliers/{supplier_id}",
            lambda: (
                proxy.proveedor(supplier_id())
                if require_value(supplier_id(), "falta ERP_TEST_SUPPLIER_ID o proveedor con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating("POST /client-phone", lambda: proxy.guardar_telefono_cliente({})),
        maybe_mutating(
            "POST /clients/{client_id}/contacts",
            lambda: (
                proxy.crear_contacto_cliente(client_id(), {})
                if require_value(client_id(), "falta ERP_TEST_CLIENT_ID o cliente con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating("POST /orders/sync", proxy.pedidos_sync),
        maybe_mutating(
            "PUT /invoices/{invoice_id}/conciliacion",
            lambda: (
                proxy.actualizar_conciliacion_factura(invoice_id(), estado="test")
                if require_value(invoice_id(), "falta ERP_TEST_INVOICE_ID o factura con id") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating("POST /proposal", lambda: proxy.crear_presupuesto({})),
        maybe_mutating(
            "POST /proposal/{name}/create/document",
            lambda: (
                proxy.crear_documento_propuesta(proposal_name())
                if require_value(proposal_name(), "falta ERP_TEST_PROPOSAL_NAME") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating(
            "PATCH /proposal/{proposal_id}/extrafields",
            lambda: (
                proxy.actualizar_extrafields_propuesta(proposal_id(), {})
                if require_value(proposal_id(), "falta ERP_TEST_PROPOSAL_ID") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating(
            "POST /proposal/{proposal_id}/lines",
            lambda: (
                proxy.crear_linea_propuesta(proposal_id(), {})
                if require_value(proposal_id(), "falta ERP_TEST_PROPOSAL_ID") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating(
            "POST /proposal/{proposal_id}/confirm",
            lambda: (
                proxy.confirmar_propuesta(proposal_id())
                if require_value(proposal_id(), "falta ERP_TEST_PROPOSAL_ID") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating(
            "POST /proposal/{proposal_id}/validate",
            lambda: (
                proxy.validar_propuesta(proposal_id())
                if require_value(proposal_id(), "falta ERP_TEST_PROPOSAL_ID") is not SKIPPED
                else SKIPPED
            ),
        ),
        maybe_mutating("POST /shipments", lambda: proxy.crear_shipment({})),
        maybe_mutating(
            "PATCH /shipments/{shipment_id}/parametro/{parametro:path}",
            lambda: (
                proxy.actualizar_shipment_parametro(shipment_id(), shipment_parametro(), "")
                if (
                    require_value(shipment_id(), "falta ERP_TEST_SHIPMENT_ID o shipment con id") is not SKIPPED
                    and require_value(shipment_parametro(), "falta ERP_TEST_SHIPMENT_PARAMETRO") is not SKIPPED
                )
                else SKIPPED
            ),
        ),
        maybe_mutating(
            "POST /shipments/{shipment_id}/validate",
            lambda: (
                proxy.validar_shipment(shipment_id())
                if require_value(shipment_id(), "falta ERP_TEST_SHIPMENT_ID o shipment con id") is not SKIPPED
                else SKIPPED
            ),
        ),
    ]

    counts = {
        "OK": 0,
        "FAIL": 0,
        "SKIP": 0,
    }

    for name, fn in tests:
        counts[run_case(name, fn)] += 1

    print(
        "\nResultado: "
        f"{counts['OK']} OK, "
        f"{counts['FAIL']} FAIL, "
        f"{counts['SKIP']} SKIP"
    )

    return 0 if counts["FAIL"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
