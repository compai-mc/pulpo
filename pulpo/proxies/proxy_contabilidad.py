"""

Cliente de contabilidad: La llamada es así:

    from pulpo.proxies.proxy_contabilidad import ContabilidadClient
    cliente = ContabilidadClient()
    balance = cliente.la_funcion_que_sea()
    print(balance)
    
"""

from typing import Optional, List
from datetime import date

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

CONTABILIDAD_URL = require_env("CONTABILIDAD_URL")
CLIENT_ID = require_env("CLIENT_ID_CONTABILIDAD")
CLIENT_SECRET = require_env("CLIENT_SECRET_CONTABILIDAD")

class ContabilidadClient:

    def __init__(self):

        tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.http = MicroHttpClient(tm)
        self.base_url = CONTABILIDAD_URL

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

        return self.http.post("/cuentascontables", payload)

    def listar_cuentas_contables(self, nivel: str = "todas"):
        return self.http.get("/cuentascontables", {"nivel": nivel})

    def obtener_cuenta(self, code: str):
        return self.http.get(f"/cuentascontables/{code}")

    def movimientos_cuenta(self, code: str):
        return self.http.get(f"/cuentascontables/{code}/movimientos")

    def balance_cuenta_fecha(self, code: str, at: date):
        return self.http.get(
            f"/cuentascontables/{code}/balance_en",
            {"at": at.isoformat()}
        )

    def balance_cuenta_actual(self, code: str, consolidado: bool = False):
        return self.http.get(
            f"/cuentascontables/{code}/balance",
            {"consolidado": consolidado}
        )

    def importar_cuentas(self, json_file_path: str):
        resp = self.http.post_file(
            f"{self.base_url}/cuentascontables/importar",
            json_file_path
        )
        return resp.json()

    # ======================================================
    # PERIODOS
    # ======================================================

    def crear_periodo(self, code: str, start: date, end: date):
        return self.http.post("/periodos", {
            "code": code,
            "start_date": start.isoformat(),
            "end_date": end.isoformat()
        })

    def crear_periodo_anual(self, year: int):
        return self.http.post("/periodos/year", {"year": year})

    def abrir_periodo(self, code: str):
        return self.http.put(f"/periodos/{code}/abrir")

    def cerrar_periodo(self, code: str):
        return self.http.post(f"/periodos/{code}/cerrar", {})

    def borrar_periodo_dev(self, code: str):
        return self.http.delete(f"/des/periodos/{code}")

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
        return self.http.post("/asientos", payload)

    # ======================================================
    # BALANCES
    # ======================================================

    def balance_comprobacion(self):
        return self.http.get("/balances/trial")

    def balance_de_cuenta(self, code: str):
        return self.http.get(f"/balances/{code}")

    # ======================================================
    # IVA
    # ======================================================

    def calcular_iva(self, base: float, rate: float):
        return self.http.post("/iva/calcular", {
            "base_amount": str(base),
            "rate": str(rate)
        })

    # ======================================================
    # CIERRE DE EJERCICIO
    # ======================================================

    def cerrar_ejercicio(self, year: int):
        return self.http.post("/cierre", {"year": year})

    # ======================================================
    # INFORMES
    # ======================================================

    def balances_por_tipo(self, acc_type: str):
        return self.http.get(f"/informes/balances/por_tipo/{acc_type}")

    def perdidas_y_ganancias(self):
        return self.http.get("/informes/PyG")

    def informe_financiero_periodo(self, period_code: str):
        return self.http.get(f"/informes/{period_code}")

    def resumen_financiero(
        self, fecha_referencia: str, scope: str = "periodo"
    ):
        return self.http.get("/informes/resumen_financiero", {
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

        return self.http.post(
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
        return self.http.post(
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
        return self.http.delete("/des/delete")

    def reset_asientos(self):
        return self.http.delete("/des/delete/asientos")
