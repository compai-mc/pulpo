import json
import re
import ast
from json.decoder import scanstring
import importlib
from pathlib import Path


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



def cargar_config(ruta_config: str | Path) -> dict:
    """
    Carga un JSON desde disco y reemplaza las listas bajo la clave 'string-classes'
    por la clave 'classes' con las clases reales importadas dinámicamente.
    """
    ruta = Path(ruta_config)
    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo de configuración: {ruta}")

    # Leer el JSON
    with open(ruta, "r", encoding="utf-8") as f:
        config = json.load(f)

    # Función auxiliar para importar una clase desde su nombre completo
    def _importar_clase(nombre_clase: str):
        modulo, clase = nombre_clase.rsplit(".", 1)
        mod = importlib.import_module(modulo)
        return getattr(mod, clase)

    # Función recursiva que reemplaza todas las apariciones de "string-classes"
    def _procesar_nodo(nodo):
        if isinstance(nodo, dict):
            # Si el nodo tiene la clave "string-classes"
            if "string-classes" in nodo:
                clases_importadas = []
                for nombre_clase in nodo["string-classes"]:
                    try:
                        clases_importadas.append(_importar_clase(nombre_clase))
                    except Exception as e:
                        raise ImportError(f"No se pudo importar {nombre_clase}: {e}") from e

                nodo["classes"] = clases_importadas
                del nodo["string-classes"]

            # Recorrer los valores del diccionario
            for k, v in nodo.items():
                _procesar_nodo(v)

        elif isinstance(nodo, list):
            for item in nodo:
                _procesar_nodo(item)

    # Procesar todo el JSON
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
