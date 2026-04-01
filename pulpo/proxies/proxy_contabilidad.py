"""

Cliente de contabilidad: La llamada es así:

    from pulpo.proxies.proxy_contabilidad import ContabilidadClient
    cliente = ContabilidadClient()
    balance = cliente.la_funcion_que_sea()
    print(balance)
    
"""

import httpx
from typing import Optional, List
from datetime import date

from pulpo.authenticate import get_token_exchange, get_user_token
from pulpo.util.util import require_env

CONTABILIDAD_URL = require_env("CONTABILIDAD_URL")
CLIENT_ID_CONTABILIDAD = require_env("CLIENT_ID_CONTABILIDAD")
CLIENT_SECRET_CONTABILIDAD = require_env("CLIENT_SECRET_CONTABILIDAD")

class ContabilidadClient:
    def __init__(self):

        self.user_token = get_user_token()
        self.base_url = CONTABILIDAD_URL
        self.client_id = CLIENT_ID_CONTABILIDAD
        self.client_secret = CLIENT_SECRET_CONTABILIDAD

    def _auth_headers(self):
        token =  get_token_exchange(
            self.client_id,
            self.client_secret,
            self.user_token
        )
        return {"Authorization": f"Bearer {token}"}

    def _post(self, path: str, payload: dict) -> dict:
        with httpx.Client() as client:
            resp = client.post(
                f"{self.base_url}{path}",
                json=payload,
                headers=self._auth_headers()   
            )
            resp.raise_for_status()
            return resp.json()

    def _put(self, path: str, payload: dict = None) -> dict:
        with httpx.Client() as client:
            resp = client.put(
                f"{self.base_url}{path}",
                json=payload,
                headers=self._auth_headers()   
            )
            resp.raise_for_status()
            return resp.json()

    def _get(self, path: str, params: dict = None) -> dict:
        with httpx.Client() as client:
            resp = client.get(
                f"{self.base_url}{path}",
                params=params,
                headers=self._auth_headers()   
            )
            resp.raise_for_status()
            return resp.json()

    def _delete(self, path: str) -> Optional[dict]:
        with httpx.Client() as client:
            resp = client.delete(
                f"{self.base_url}{path}",
                headers=self._auth_headers()  
            )
            if resp.status_code == 204:
                return None
            resp.raise_for_status()
            return resp.json()

    # ======================================================
    # PGC
    # ======================================================

    def crear_cuenta_contable(
        self,
        code: str,
        name: str,
        account_type: Optional[str] = None,
        grupo_codigo: Optional[int] = None,
        grupo_nombre: Optional[str] = None,
        parent_code: Optional[str] = None,
        categoria: Optional[str] = "",
        subcategoria: Optional[str] = "",

    ):
        payload = {
            "code": code,
            "name": name,
            "account_type": account_type,
            "grupo_codigo": grupo_codigo,
            "grupo_nombre": grupo_nombre,
            "parent_code": parent_code,
            "categoria": categoria,
            "subcategoria": subcategoria
        }

        return self._post("/cuentascontables", payload)

    def listar_cuentas_contables(self, nivel: str = "todas"):
        return self._get("/cuentascontables", {"nivel": nivel})

    def obtener_cuenta(self, code: str):
        return self._get(f"/cuentascontables/{code}")

    def movimientos_cuenta(self, code: str):
        return self._get(f"/cuentascontables/{code}/movimientos")

    def balance_cuenta_fecha(self, code: str, at: date):
        return self._get(
            f"/cuentascontables/{code}/balance_en",
            {"at": at.isoformat()}
        )

    def balance_cuenta_actual(self, code: str, consolidado: bool = False):
        return self._get(
            f"/cuentascontables/{code}/balance",
            {"consolidado": consolidado}
        )

    def importar_cuentas(self, json_file_path: str):
        with open(json_file_path, "rb") as f:
            with httpx.Client() as client:
                files = {"file": (json_file_path, f, "application/json")}
                resp = client.post(
                    f"{self.base_url}/cuentascontables/importar",
                    files=files,
                    headers=self._auth_headers()
                )
                resp.raise_for_status()
                return resp.json()

    # ======================================================
    # PERIODOS
    # ======================================================

    def crear_periodo(self, code: str, start: date, end: date):
        return self._post("/periodos", {
            "code": code,
            "start_date": start.isoformat(),
            "end_date": end.isoformat()
        })

    def crear_periodo_anual(self, year: int):
        return self._post("/periodos/year", {"year": year})

    def abrir_periodo(self, code: str):
        return self._put(f"/periodos/{code}/abrir")

    def cerrar_periodo(self, code: str):
        return self._post(f"/periodos/{code}/cerrar", {})

    def borrar_periodo_dev(self, code: str):
        return self._delete(f"/des/periodos/{code}")

    # ======================================================
    # ASIENTOS
    # ======================================================

    def crear_asiento(
        self,
        date_: date,
        description: str,
        lines: List[dict]
    ):
        """
        lines = [
            {"account_code": "...", "debe": "0.00", "haber": "10.00", "description": "..."},
            ...
        ]
        """
        payload = {
            "date": date_.isoformat(),
            "description": description,
            "lines": lines
        }
        return self._post("/asientos", payload)

    # ======================================================
    # BALANCES
    # ======================================================

    def balance_comprobacion(self):
        return self._get("/balances/trial")

    def balance_de_cuenta(self, code: str):
        return self._get(f"/balances/{code}")

    # ======================================================
    # IVA
    # ======================================================

    def calcular_iva(self, base: float, rate: float):
        return self._post("/iva/calcular", {
            "base_amount": str(base),
            "rate": str(rate)
        })

    # ======================================================
    # CIERRE DE EJERCICIO
    # ======================================================

    def cerrar_ejercicio(self, year: int):
        return self._post("/cierre", {"year": year})

    # ======================================================
    # INFORMES
    # ======================================================

    def balances_por_tipo(self, acc_type: str):
        return self._get(f"/informes/balances/por_tipo/{acc_type}")

    def perdidas_y_ganancias(self):
        return self._get("/informes/PyG")

    def informe_financiero_periodo(self, period_code: str):
        return self._get(f"/informes/{period_code}")

    def resumen_financiero(
        self, fecha_referencia: str, scope: str = "periodo"
    ):
        return self._get("/informes/resumen_financiero", {
            "fecha_referencia": fecha_referencia,
            "scope": scope
        })

    # ======================================================
    # AUTOMATION RULES
    # ======================================================

    def crear_regla(
        self,
        automation_id: str,
        nombre: str,
        expresion: str,
        acciones: List[dict],
        prioridad: int = 10,
        sobreescribir: bool = False
    ):
        payload = {
            "automation_id": automation_id,
            "nombre": nombre,
            "expresion": expresion,
            "acciones": acciones,
            "prioridad": prioridad
        }

        return self._post(
            f"/automation/rule?sobreescribir={'true' if sobreescribir else 'false'}",
            payload
        )

    def ejecutar_automatizacion(
        self,
        automation_id: str,
        period_start: str,
        period_end: str,
        scope: str = "ejercicio"
    ):
        return self._post(
            "/automation/execution",
            {
                "automation_id": automation_id,
                "period_start": period_start,
                "period_end": period_end,
                "scope": scope
            }
        )

    # ======================================================
    # DEV ONLY
    # ======================================================

    def reset_all(self):
        return self._delete("/des/delete")

    def reset_asientos(self):
        return self._delete("/des/delete/asientos")
