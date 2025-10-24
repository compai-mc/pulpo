import httpx
import asyncio
import os
from typing import Any, Dict, Optional, List, Union
from dotenv import load_dotenv
load_dotenv()

def _float_env(var_name: str, default: float) -> float:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ Valor inválido para {var_name}: {value}. Usando {default}.")
        return default


ERP_TIMEOUT_CONNECT = _float_env("ERP_TIMEOUT_CONNECT", 30.0)
ERP_TIMEOUT_READ = _float_env("ERP_TIMEOUT_READ", 300.0)
ERP_TIMEOUT_WRITE = _float_env("ERP_TIMEOUT_WRITE", 60.0)
ERP_TIMEOUT_POOL = _float_env("ERP_TIMEOUT_POOL", 90.0)


class ERPProxy:
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
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, headers=headers)

    async def close(self):
        await self.client.aclose()

    # ---------------- Orders ----------------
    async def pedidos(self, fecha: str) -> Dict[str, Any]:
        r = await self.client.get(f"/orders/{fecha}/url")
        return r.json()

    async def pedidos_producto_cliente_mes(self, fecha: str) -> Dict[str, Any]:
        r = await self.client.get(f"/orders/group-by-product-client-month/{fecha}/url")
        return r.json()

    async def pedidos_sync(self) -> Dict[str, Any]:
        r = await self.client.post("/orders/sync")
        return r.json()

    # ---------------- Products ----------------
    async def productos(self) -> List[Dict[str, Any]]:
        r = await self.client.get("/products")
        return r.json()

    async def obtener_stock_producto(self, product_ref: str) -> Dict[str, Any]:
        r = await self.client.get(f"/products/{product_ref}/stock")
        return r.json()

    # ---------------- Clients ----------------
    async def clientes(self) -> List[Dict[str, Any]]:
        r = await self.client.get("/clients")
        return r.json()

    # ---------------- Proposal ----------------
    async def crear_presupuesto(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = await self.client.post("/proposal", json=payload)
        return r.json()

    async def proposal_create_document(self, name: str) -> Dict[str, Any]:
        """
        Devuelve JSON con metadata y base64 del PDF.
        {
            "filename": "...",
            "content_type": "...",
            "content_base64": "...",
            "download_url": "..."
        }
        """
        r = await self.client.post(f"/proposal/{name}/create/document")
        return r.json()

    async def download_proposal_document(self, name: str) -> bytes:
        """
        Descarga directa del PDF binario.
        """
        r = await self.client.get(f"/proposal/{name}/document/download")
        if r.status_code == 200:
            return r.content
        return b""  # en caso de error devolvemos vacío

    # ---------------- Health / utilidades ----------------
    async def health(self) -> Dict[str, Any]:
        r = await self.client.get("/health")
        return r.json()


# ---------------- Ejemplo de uso ----------------
async def main():
    client = ERPProxy("http://localhost:7404", api_key="299620633106460d8c1d03bf89fb2006fec66ccc")

    # Listados
    #print("Pedidos:", await client.pedidos("2025-09"))
    #print("Clientes:", await client.clientes())
    #print("Productos:", await client.productos())

    # Crear doc -> devuelve JSON con base64
    doc_info = await client.proposal_create_document("(PROV10384)")
    print("Info documento:", doc_info)

    # Descargar binario directamente
    pdf_bytes = await client.download_proposal_document("(PROV10384)")
    if pdf_bytes:
        with open("propuesta.pdf", "wb") as f:
            f.write(pdf_bytes)
        print("PDF guardado en propuesta.pdf")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())