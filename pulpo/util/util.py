import json
import re
import ast

def arreglar_json(json_str: str):
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
            data = fix_json(s)
            print("Arreglado:", data)
        except Exception as e:
            print("Error:", e)
        print("-"*40)
