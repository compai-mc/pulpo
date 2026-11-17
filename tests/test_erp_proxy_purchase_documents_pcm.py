import base64
import os
from pathlib import Path
import runpy
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


def load_proxy():
    # Import only the proxy: tests must not load Vault or contact Keycloak.
    util = types.ModuleType("pulpo.util.util")
    util.require_env = lambda name: {
        "CLIENT_ID_ERPDOLIBARR": "erpdolibarr",
        "CLIENT_SECRET_ERPDOLIBARR": "test-secret",
        "ERPDOLIBARR_URL": "http://erp.test",
        "API_KEY_DOLIBARR": "test-key",
    }[name]
    auth = types.ModuleType("pulpo.auth.general")
    auth.MicroTokenManager = Mock()
    auth.MicroHttpClient = Mock()
    path = Path(__file__).resolve().parents[1] / "pulpo/proxies/proxy_erpdolibarr_sincrono.py"
    with patch.dict("sys.modules", {"pulpo.util.util": util, "pulpo.auth.general": auth}):
        return runpy.run_path(str(path))["ERPProxySincrono"]


class ERPProxyEndpointsTest(unittest.TestCase):
    def setUp(self):
        self.proxy = load_proxy()(base_url="http://erp.test/", timeout=42)
        self.client = self.proxy.client

    def test_json_routes_and_parameters(self):
        company = {"name": "Proveedor", "fournisseur": 1}
        contact = {"lastname": "Contacto", "email": "test@example.com"}
        order = {"socid": 12, "lines": [{"fk_product": 1, "qty": 2, "subprice": 3, "tva_tx": 21}]}
        pcm = {"mode": "preview", "from_date": "2026-01-01", "to_date": "2026-02-01",
               "date_basis": "validation", "group_by_parent": False}
        cases = [
            ("crear_proveedor", (company,), {}, "post", "/suppliers", {"json": company}),
            ("proveedores_con_contactos", (), {"limit": 10, "page": 0}, "get",
             "/suppliers/with-contacts", {"params": {"limit": 10, "page": 0}}),
            ("proveedor_por_telefono", ("+34 123#456",), {}, "get",
             "/suppliers/by-phone/%2B34%20123%23456", {}),
            ("contactos_proveedor", (12,), {}, "get", "/suppliers/12/contacts", {}),
            ("crear_contacto_proveedor", (12, contact), {}, "post",
             "/suppliers/12/contacts", {"json": contact}),
            ("crear_pedido_proveedor", (order,), {}, "post", "/supplier-orders", {"json": order}),
            ("recepciones_pedido_proveedor", (9,), {"limit": 10, "page": 0}, "get",
             "/supplierorders/9/receptions", {"params": {"limit": 10, "page": 0}}),
            ("vincular_factura_proveedor", (8, {"supplier_order_id": 9, "reception_id": 4}), {},
             "post", "/supplierinvoices/8/links", {"json": {"supplier_order_id": 9, "reception_id": 4}}),
            ("documentos_manuales", ("Compras/2026",), {"limit": 10, "offset": 0, "checksum": False},
             "get", "/documents/manual/files",
             {"params": {"directory_path": "Compras/2026", "limit": 10, "offset": 0, "checksum": False}}),
            ("directorios_manuales", (), {"limit": 10, "offset": 0}, "get",
             "/documents/manual/directories", {"params": {"parent_path": "", "limit": 10, "offset": 0}}),
            ("pedidos_sync_pcm", (pcm,), {}, "post", "/orders/sync/pcm", {"json": pcm}),
            ("crear_trabajo_pcm", (pcm,), {}, "post", "/pcm/jobs", {"json": pcm}),
            ("trabajo_pcm", ("job ?#",), {}, "get", "/pcm/jobs/job%20%3F%23", {}),
            ("descargar_trabajo_pcm", ("job ?#",), {}, "get", "/pcm/jobs/job%20%3F%23/download", {}),
            ("reglas_pcm", (), {}, "get", "/pcm/rules", {}),
        ]
        for name, args, params, verb, route, expected in cases:
            with self.subTest(method=name):
                method = getattr(self.client, verb)
                method.reset_mock()
                result = getattr(self.proxy, name)(*args, **params)
                method.assert_called_once_with("http://erp.test" + route,
                                               headers=self.proxy.headers, timeout=42, **expected)
                self.assertIs(result, method.return_value)

    def test_invoice_records_and_purchase_documents(self):
        payload = {"socid": 12, "ref_supplier": "FAC A/B", "lines": [], "validate": False}
        header = {"note_public": None, "fk_project": 0, "array_options": {"options_test": "value"}}
        validation = {"idwarehouse": 0, "notrigger": 1}
        document = {"supplier_order_ids": [9, 10], "reception_ids": [4], "validate": False}
        params = {"doctemplate": "test_model", "langcode": "es_ES", "supplier_order_id": 9,
                  "reception_id": 4, "validate": False}
        cases = [
            ("validar_pedido_proveedor", (9, validation), {}, "post", "/supplier-orders/9/validate", {"json": validation}),
            ("validar_pedido_proveedor", (9,), {}, "post", "/supplier-orders/9/validate", {"json": {}}),
            ("crear_documento_pedido_proveedor", (9,), {"doctemplate": "test_model", "langcode": "es_ES"},
             "post", "/supplier-orders/9/document", {"params": {"doctemplate": "test_model", "langcode": "es_ES"}}),
            ("factura_venta", (8,), {}, "get", "/invoices/sales/8", {}),
            ("factura", (8,), {}, "get", "/invoices/8", {}),
            ("factura_compra", (8,), {}, "get", "/invoices/purchase/8", {}),
            ("crear_factura_venta", (payload,), {}, "post", "/invoices/sales", {"json": payload}),
            ("crear_factura", (payload,), {}, "post", "/invoices", {"json": payload}),
            ("crear_factura_compra", (payload,), {"validate": False, "generate_pdf": False}, "post",
             "/invoices/purchase", {"json": payload, "params": {"validate": False, "generate_pdf": False}}),
            ("crear_factura_compra", (payload,), {}, "post", "/invoices/purchase", {"json": payload, "params": None}),
            ("actualizar_factura_venta", (8, header), {}, "patch", "/invoices/sales/8", {"json": header}),
            ("actualizar_factura", (8, header), {}, "patch", "/invoices/8", {"json": header}),
            ("actualizar_factura_compra", (8, header), {}, "patch", "/invoices/purchase/8", {"json": header}),
            ("actualizar_cabecera_factura_proveedor", (8, header), {}, "patch", "/supplierinvoices/8", {"json": header}),
            ("actualizar_factura_proveedor", (8, header), {}, "put", "/supplierinvoices/8", {"json": header}),
        ]
        for name, route in (
            ("crear_documento_factura_compra", "/invoices/purchase/8/create/document"),
            ("crear_documento_factura_proveedor", "/supplierinvoices/8/create/document"),
            ("generar_documento_factura_proveedor", "/supplierinvoices/8/document"),
        ):
            cases.append((name, (8, document), params, "post", route, {"json": document, "params": params}))
            cases.append((name, (8,), {}, "post", route, {"json": {}, "params": None}))
        for name, args, kwargs, verb, route, expected in cases:
            with self.subTest(method=name, kwargs=kwargs):
                method = getattr(self.client, verb)
                method.reset_mock()
                result = getattr(self.proxy, name)(*args, **kwargs)
                method.assert_called_once_with("http://erp.test" + route,
                                               headers=self.proxy.headers, timeout=42, **expected)
                self.assertIs(result, method.return_value)

    def test_invoice_errors_propagate_without_proxy_retry(self):
        self.client.post.side_effect = RuntimeError("invoice created, PDF failed")
        with self.assertRaisesRegex(RuntimeError, "invoice created"):
            self.proxy.crear_factura_compra({"socid": 12, "ref_supplier": "FAC 1"}, generate_pdf=True)
        self.client.post.assert_called_once()

    def test_pcm_defaults(self):
        for method, route in (("crear_trabajo_pcm", "/pcm/jobs"), ("pedidos_sync_pcm", "/orders/sync/pcm")):
            getattr(self.proxy, method)()
            self.client.post.assert_called_with("http://erp.test" + route, json={},
                                                headers=self.proxy.headers, timeout=42)

    def test_binary_downloads(self):
        content = b"%PDF-1.7\n\xe2\xe3\xcf\xd3"
        self.client._request.return_value = Mock(headers={"content-type": "application/pdf"}, content=content)
        for name, identifier, route in (
            ("descargar_documento_pedido_proveedor", 9, "/supplier-orders/9/document/download"),
            ("descargar_documento_factura_compra", 8, "/invoices/purchase/8/document/download"),
            ("descargar_documento_factura_proveedor", 8, "/supplierinvoices/8/document/download"),
            ("descargar_documento_manual", "opaque?=#", "/documents/manual/files/opaque%3F%3D%23/download"),
        ):
            result = getattr(self.proxy, name)(identifier)
            self.client._request.assert_called_with("GET", "http://erp.test" + route,
                                                    headers=self.proxy.headers, timeout=42)
            self.assertEqual(base64.b64decode(result["content_base64"]), content)
            self.assertEqual(result["content_length"], len(content))

    def test_multipart_upload_and_file_cleanup(self):
        content = b"%PDF-1.7\n\xe2"
        handles = []

        def receive(method, url, **kwargs):
            self.assertEqual((method, url), ("POST", "http://erp.test/documents/manual/upload"))
            self.assertNotIn("content-type", {k.lower() for k in kwargs["headers"]})
            self.assertEqual(kwargs["headers"]["DOLAPIKEY"], "test-key")
            self.assertEqual(kwargs["data"], {"directory_path": "Compras/2026", "overwrite": "false"})
            self.assertEqual(kwargs["timeout"], 42)
            filename, handle, mime = kwargs["files"]["file"]
            handles.append(handle)
            self.assertEqual(filename, "factura.pdf")
            self.assertEqual(mime, "application/pdf")
            self.assertEqual(handle.read(), content)
            return Mock(json=lambda: {"status": "ok"})

        self.client._request.side_effect = receive
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "factura.pdf")
            with open(path, "wb") as file:
                file.write(content)
            result = self.proxy.subir_documento_manual(path, "Compras/2026", content_type="application/pdf")
            self.assertEqual(result, {"status": "ok"})
            self.assertTrue(handles[-1].closed)
            self.assertEqual(self.proxy.headers["Content-Type"], "application/json")

            def fail(*args, **kwargs):
                handles.append(kwargs["files"]["file"][1])
                self.assertEqual(kwargs["data"]["overwrite"], "true")
                raise RuntimeError("upload failed")

            self.client._request.side_effect = fail
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                self.proxy.subir_documento_manual(path, "Compras/2026", overwrite=True)
            self.assertTrue(handles[-1].closed)


if __name__ == "__main__":
    unittest.main()
