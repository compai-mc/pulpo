import json
import json5
import re
import ast
from json.decoder import scanstring
import importlib
from pathlib import Path
import os
from dotenv import load_dotenv
import hvac
import requests
from datetime import datetime
from importlib.metadata import distributions
from typing import Union, List

from time import time
START_TIME = time()


from pulpo.logueador import log
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
#log.set_log_file(f"log/util[{log_time}].log")
log.set_log_level("DEBUG")

def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None:
        raise RuntimeError(f"❌ Variable de entorno obligatoria no definida: {var_name}")
    return value


def load_env(
        vault_addr:str = "http://alcazar:8200",
        vault_token:str = "root",
        vault_path: Union[str, List[str]] = "des/compai",
        env_path: str = "../../compai/deploy/desarrollo/desarrollo-compai/.env",
        config_especifico_id: str = ""
    ):
    """Carga variables desde Vault → .env → config-service"""

    # Permite que los micros usen el compose como fuente de bootstrap.
    # Estas variables deben cargarse antes de importar pulpo.auth.
    vault_addr = os.getenv("VAULT_URL", vault_addr)
    vault_token = os.getenv("VAULT_TOKEN", vault_token)
    config_especifico_id = config_especifico_id or os.getenv("CONFIG_ID", "")

    env_vault_path = os.getenv("VAULT_PATH")
    env_vault_clients = os.getenv("VAULT_CLIENTS")

    if env_vault_path and env_vault_clients:
        vault_path = [env_vault_path, env_vault_clients]
    elif env_vault_path:
        vault_path = env_vault_path
    elif env_vault_clients:
        vault_path = env_vault_clients

    def load_all_secrets(client, base_path):
        """
        Carga todas las claves del path base y de todos los subdirectorios recursivamente.
        Compatible con Vault KV v2.
        """
        # 1️⃣ Intentar leer secretos DIRECTAMENTE del base_path
        try:
            secret = client.secrets.kv.v2.read_secret_version(path=base_path)
            secret_data = secret["data"]["data"]
            if secret_data:
                os.environ.update(secret_data)
        except hvac.exceptions.InvalidPath:
            # Es válido que el root no tenga datos
            pass
        except Exception as e:
            log.error(f"⚠️ Error leyendo secretos en {base_path}: {e}")

        # 2️⃣ Listar subpaths (si existen)
        try:
            response = client.secrets.kv.v2.list_secrets(path=base_path)
            keys = response["data"]["keys"]

            for key in keys:
                if key.endswith("/"):
                    full_path = f"{base_path.rstrip('/')}/{key.rstrip('/')}"
                    load_all_secrets(client, full_path)

        except hvac.exceptions.InvalidPath:
            # Es válido que no existan subdirectorios
            pass
        except Exception as e:
            log.error(f"⚠️ Error listando secretos en {base_path}: {e}")


    # --- 1. Vault ---
    vault_loaded = False
    if vault_addr and vault_token:
        try:
            client = hvac.Client(url=vault_addr, token=vault_token)
            if client.is_authenticated():
                log.info("✅ Vault conectado correctamente")

                paths = (
                    vault_path
                    if isinstance(vault_path, (list, tuple))
                    else [vault_path]
                )

                for path in paths:
                    load_all_secrets(client, path)

                vault_loaded = True
            else:
                log.warning("⚠️ Token inválido, usando .env local")
        except Exception as e:
            log.error(f"⚠️ Error accediendo a Vault: {e}")

    # --- 2. .env local si Vault no funcionó ---
    if not vault_loaded:
        log.info("💡 Usando variables locales del .env")
        load_dotenv(
            dotenv_path=Path(env_path),
            override=True
        )

    def get_config_raw(service: str) -> dict:
        """
        Lee config-service sin importar clientes autenticados de Pulpo.
        Se usa en bootstrap, antes de que existan SEC_KEYCLOAK_URL/SEC_REALM.
        """
        config_service_url = os.getenv("URL_CONFIG_SERVICE")

        if not config_service_url:
            raise RuntimeError(
                "Variable de entorno obligatoria no definida: URL_CONFIG_SERVICE"
            )

        url = f"{config_service_url.rstrip('/')}/config/{service}"
        response = requests.get(url, timeout=10)

        if response.status_code == 404:
            log.warning(
                f"No se encontro configuracion para el servicio '{service}'"
            )
            return {}

        response.raise_for_status()
        return response.json() or {}

    def extract_config(payload: dict) -> dict:
        """
        Normaliza respuestas del config-service:
        {"config": {...}} o directamente {...}.
        """
        if not isinstance(payload, dict):
            return {}

        config = payload.get("config", payload)
        return config if isinstance(config, dict) else {}

    # --- 3. Config-service ---
    # Inicializar variables para evitar UnboundLocalError
    config_global = {}
    config_especifico = {}

    try:
        config_global = extract_config(
            get_config_raw("compai_global")
        )
        config_global_static = config_global.get("static", {})

        if config_especifico_id:
            config_especifico = extract_config(
                get_config_raw(config_especifico_id)
            )
        else:
            config_especifico = {}

        # Añadir al entorno
        for key, value in config_global_static.items():
            os.environ[str(key)] = str(value)

        for key, value in config_especifico.items():
            str_key = str(key)
            
            # Verificamos si la clave ya existe en el entorno antes de escribir
            if str_key in os.environ:
                log.warning(
                    f"Configuración específica sobrescribiendo variable existente: "
                    f"Key='{str_key}' | Valor anterior='{os.environ[str_key]}' | Nuevo valor='{value}'"
                )
            os.environ[str_key] = str(value)

        log.info("🔧 Config-service cargado correctamente")

    except Exception as e:
        log.error(f"⚠️ No se pudo cargar config-service: {e}")

    return { "global": config_global, "especifico": config_especifico }


def extraer_json_del_texto(texto) -> dict:
    """
    Extrae y arregla un bloque JSON de un texto que puede venir malformado,
    con backticks, comillas simples, comas colgantes, etc.
    Si el argumento ya es un dict, se devuelve tal cual.
    """

    # 🧩 0. Si ya es un dict o lista, devolver directamente
    if isinstance(texto, (dict, list)):
        return texto

    # 🧩 1. Si no es str, convertir a str
    if not isinstance(texto, str):
        return {"raw": str(texto)}

    # 1. Buscar bloque con ```json ... ```
    match = re.search(r"```json\s*(\{.*?\})\s*```", texto, re.DOTALL)
    if not match:
        match = re.search(r"(\{.*\})", texto, re.DOTALL)

    if not match:
        return {"raw": texto.strip()}

    json_str = match.group(1).strip()

    # 2. Limpiar ```json ... ``` o ```
    if json_str.startswith("```json"):
        json_str = json_str[len("```json"):].strip()
    elif json_str.startswith("```"):
        json_str = json_str[len("```"):].strip()
    if json_str.endswith("```"):
        json_str = json_str[:-len("```")].strip()

    # 3. Intentar parsear directo
    try:
        return json.loads(json_str)
    except Exception:
        pass

    # 4. Parches comunes
    fixed = json_str
    fixed = fixed.replace("'", '"')
    fixed = fixed.replace("None", "null")
    fixed = fixed.replace("True", "true")
    fixed = fixed.replace("False", "false")
    fixed = fixed.replace("\\n", " ")
    fixed = re.sub(r",\s*([}\]])", r"\1", fixed)

    # 5. Escapar comillas internas dentro de strings
    def escape_strings(s):
        result = []
        i = 0
        while i < len(s):
            if s[i] == '"':  # inicio de string
                try:
                    val, end = scanstring(s, i + 1)
                    safe_val = val.replace('"', '\\"')
                    result.append(f'"{safe_val}"')
                    i = end + 1
                except Exception:
                    result.append(s[i])
                    i += 1
            else:
                result.append(s[i])
                i += 1
        return ''.join(result)

    fixed = escape_strings(fixed)

    # 6. Intentar parsear con JSON estándar
    try:
        return json.loads(fixed)
    except Exception:
        pass

    # 7. Probar con json5 si está disponible
    try:
        
        return json5.loads(fixed)
    except Exception:
        pass

    # 8. Último recurso: ast.literal_eval
    try:
        data = ast.literal_eval(fixed)
    except Exception:
        return {"raw": texto.strip()}

    # ✅ 9. Reparar JSONs anidados en strings (como "interpretacion")
    if isinstance(data, dict):
        for k, v in list(data.items()):
            if isinstance(v, str):
                v_str = v.strip()
                if v_str.startswith("{") and v_str.endswith("}"):
                    try:
                        data[k] = json.loads(v_str)
                    except Exception:
                        pass  # si no parsea, lo dejamos como está

    return data


def cargar_clases_tools(config: dict) -> dict:
    """
        Carga clases desde cadenas en la configuración bajo la clave "string-classes".
        Recorre recursivamente el diccionario y busca listas bajo la clave "string-
        classes", importando las clases y reemplazándolas en la clave "classes".
    """

    def _importar_clase(nombre_clase: str):
        modulo, clase = nombre_clase.rsplit(".", 1)
        mod = importlib.import_module(modulo)
        clase_obj = getattr(mod, clase)
        return clase_obj  # 👈 devuelve la clase, no la instancia

    def _procesar_nodo(nodo):
        if isinstance(nodo, dict):

            if "classes" in nodo:
                return config

            if "string-classes" in nodo:
                clases_importadas = []
                for nombre_clase in nodo["string-classes"]:
                    try:
                        clases_importadas.append(_importar_clase(nombre_clase))
                    except Exception as e:
                        raise ImportError(f"No se pudo importar {nombre_clase}: {e}") from e

                nodo["classes"] = clases_importadas

            for k, v in nodo.items():
                _procesar_nodo(v)
        elif isinstance(nodo, list):
            for item in nodo:
                _procesar_nodo(item)
    
    _procesar_nodo(config)

    return config


def get_version_info(version = "0.0.0"):

    commit = os.getenv("APP_COMMIT","n/a")

    log.info(f"version: {version}.{commit}")

    return {
        "version": version,
        "commit": commit,
        "full_version": f"{version}.{commit}",
        "uptime_seconds": int(time() - START_TIME)
    }


def get_installed_packages():
    """
    Genera una lista de los modulos con los que esta creado este micro
    """

    packages = []
    for dist in distributions():
        packages.append({
            "name": dist.metadata["Name"],
            "version": dist.version
        })
    return packages


#### Pruebas ##########################
if __name__ == "__main__":

    # -----------------
    # Ejemplos
    bad_jsons = [
        "{'a': 1, 'b': True, 'c': None,}",     # errores comunes
        '{"a":1, "b":2,}',                     # coma extra
        "{unquoted: 'value'}"                  # json5 style
    ]

    for s in bad_jsons:
        print("Entrada:", s)
        try:
            #data = arreglar_json(s)
            #print("Arreglado:", data)
            pass
        except Exception as e:
            print("Error:", e)
        print("-"*40)
