import json
import re
import ast
from json.decoder import scanstring

def old_arreglar_json(json_str: str):
    # 1. Primer intento: cargar tal cual
    try:
        return json.loads(json_str)
    except Exception:
        pass

    fixed = json_str

    # 2. Parches comunes
    fixed = fixed.replace("'", '"')             # comillas simples → dobles
    fixed = fixed.replace("None", "null")       # None → null
    fixed = fixed.replace("True", "true")       # True → true
    fixed = fixed.replace("False", "false")     # False → false
    fixed = re.sub(r",\s*([}\]])", r"\1", fixed)  # eliminar coma final antes de ] o }

    # 3. Intentar de nuevo con json estándar
    try:
        return json.loads(fixed)
    except Exception:
        pass

    # 4. Intentar con json5 si está instalado
    try:
        import json5
        return json5.loads(json_str)
    except Exception:
        pass

    # 5. Último recurso: ast.literal_eval (convierte a dict de Python)
    try:
        return ast.literal_eval(json_str)
    except Exception as e:
        raise ValueError(f"No se pudo reparar el JSON: {e}")


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
            return ast.literal_eval(fixed)
        except Exception as e:
            raise ValueError(f"No se pudo reparar el JSON: {e}\nTexto:\n{fixed}")




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
