import json
import re
import ast
from json.decoder import scanstring
import importlib
from pathlib import Path
import os
from dotenv import load_dotenv
import hvac

def require_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None:
        raise RuntimeError(f"❌ Variable de entorno obligatoria no definida: {var_name}")
    return value


def load_env(
        vault_addr:str = "http://alcazar:8200",
        vault_token:str = "root",
        vault_path:str = "des/compai",
        env_path: str = "../../compai/deploy/desarrollo/desarrollo-compai/.env"
        ):
    """Carga las variables de entorno desde HashiCorp Vault si es posible,"""

    def load_all_secrets(client, base_path):
        """Carga todas las claves de todos los subdirectorios recursivamente."""
        try:
            # Intenta listar los subpaths del directorio actual
            response = client.secrets.kv.v2.list_secrets(path=base_path)
            keys = response["data"]["keys"]

            for key in keys:
                full_path = f"{base_path.rstrip('/')}/{key}"
                if key.endswith("/"):
                    # Es un subdirectorio: recurse
                    load_all_secrets(client, full_path)
                else:
                    # Es una clave: leer sus valores
                    secret_data = client.secrets.kv.v2.read_secret_version(path=full_path)["data"]["data"]
                    os.environ.update(secret_data)
        except hvac.exceptions.InvalidPath:
            # Si el path no es una carpeta KV válida, lo ignoramos
            pass

    if vault_addr and vault_token:
        try:
            client = hvac.Client(url=vault_addr, token=vault_token)
            if client.is_authenticated():
                print("✅ Vault conectado correctamente")
                load_all_secrets(client, vault_path)
                return
            else:
                print("⚠️ Token inválido, usando .env local")
        except Exception as e:
            print(f"⚠️ Error accediendo a Vault: {e}")

    print("💡 Usando variables locales del .env")
    load_dotenv(
        dotenv_path=Path(env_path),
        override=True
    )


def extraer_json_del_texto(texto: str) -> dict:
    """
    Extrae y arregla un bloque JSON de un texto que puede venir malformado,
    con backticks, comillas simples, comas colgantes, etc.
    """

    # 1. Buscar bloque con ```json ... ```
    match = re.search(r"```json\s*(\{.*?\})\s*```", texto, re.DOTALL)
    if not match:
        match = re.search(r"(\{.*\})", texto, re.DOTALL)

    if not match:
        raise ValueError("No se encontró bloque JSON en el texto")

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
    fixed = fixed.replace("'", '"')         # comillas simples → dobles
    fixed = fixed.replace("None", "null")   # None → null
    fixed = fixed.replace("True", "true")   # True → true
    fixed = fixed.replace("False", "false") # False → false
    fixed = fixed.replace("\\n", " ")       # eliminar saltos literales
    fixed = re.sub(r",\s*([}\]])", r"\1", fixed)  # eliminar comas colgantes

    # 5. Escapar comillas internas dentro de strings
    def escape_strings(s):
        result = []
        i = 0
        while i < len(s):
            if s[i] == '"':  # inicio de string
                try:
                    val, end = scanstring(s, i + 1)
                    # escapamos comillas internas en el valor
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
        import json5
        return json5.loads(fixed)
    except Exception:
        pass

    # 8. Último recurso: ast.literal_eval
    try:
        data = ast.literal_eval(fixed)
    except Exception as e:
        raise ValueError(f"No se pudo reparar el JSON: {e}\nTexto:\n{fixed}")


    # ✅ 9. Reparar JSONs anidados en strings (como "interpretacion")
    for k, v in list(data.items()):
        if isinstance(v, str):
            v_str = v.strip()
            if v_str.startswith("{") and v_str.endswith("}"):
                try:
                    data[k] = json.loads(v_str)
                except Exception:
                    pass  # si no parsea, lo dejamos como está

    return data


def cargar_config(ruta_config: str | Path, cargar_clases: bool = True) -> dict:
    ruta = Path(ruta_config)
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {ruta}")

    with open(ruta, "r", encoding="utf-8") as f:
        config = json.load(f)

    def _importar_clase(nombre_clase: str):
        modulo, clase = nombre_clase.rsplit(".", 1)
        mod = importlib.import_module(modulo)
        clase_obj = getattr(mod, clase)
        return clase_obj  # 👈 devuelve la clase, no la instancia

    def _procesar_nodo(nodo):
        if isinstance(nodo, dict):
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

    # 🔁 Solo procesar si se pidió
    if cargar_clases:
        _procesar_nodo(config)

    return config


if __name__ == "__main__":

    cfg = cargar_config("app/config.json")

    print(cfg["agents"]["agente_herramientas"]["tools"]["classes"])





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
            data = arreglar_json(s)
            print("Arreglado:", data)
        except Exception as e:
            print("Error:", e)
        print("-"*40)
