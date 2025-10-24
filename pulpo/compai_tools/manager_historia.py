import json
import re
from typing import Dict, List, Any, Optional

class HistoriaManager:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self.veredicto = data.get('veredicto', {})
        self.interpretacion = self.veredicto.get('interpretacion', {})
    
    def _extraer_json_desde_markdown(self, texto: str) -> Dict[str, Any]:
        """Extrae JSON del formato markdown con ```json"""
        try:
            # Buscar contenido entre ```json y ```
            match = re.search(r'```json\s*(.*?)\s*```', texto, re.DOTALL)
            if match:
                json_str = match.group(1)
                return json.loads(json_str)
            else:
                # Intentar parsear directamente si no hay markdown
                return json.loads(texto)
        except (json.JSONDecodeError, AttributeError):
            return {}
    
    def _obtener_seleccion_ia(self, tipo: str, index: int = 0) -> Optional[Dict[str, Any]]:
        """Obtiene la selección IA para empresa o producto"""
        if tipo == "empresa":
            empresa_data = self.interpretacion.get('empresa', {})
            return empresa_data.get('seleccion_ia')
        elif tipo == "producto":
            productos = self.interpretacion.get('productos', [])
            if index < len(productos):
                return productos[index].get('seleccion_ia')
        return None
    
    def _obtener_seleccion_humano(self, tipo: str, index: int = 0) -> Optional[Dict[str, Any]]:
        """Obtiene la selección humana para empresa o producto"""
        if tipo == "empresa":
            empresa_data = self.interpretacion.get('empresa', {})
            return empresa_data.get('seleccion_humano')
        elif tipo == "producto":
            productos = self.interpretacion.get('productos', [])
            if index < len(productos):
                return productos[index].get('seleccion_humano')
        return None
    
    def crear_json_ia(self) -> Dict[str, Any]:
        """Crea JSON con productos y empresas generados por IA"""
        resultado = {
            "empresa": None,
            "productos": []
        }
        
        # Obtener empresa IA
        empresa_ia = self._obtener_seleccion_ia("empresa")
        if empresa_ia:
            resultado["empresa"] = empresa_ia
        
        # Obtener productos IA
        productos_ia = []
        for i in range(len(self.interpretacion.get('productos', []))):
            producto_ia = self._obtener_seleccion_ia("producto", i)
            if producto_ia:
                productos_ia.append(producto_ia)
        
        resultado["productos"] = productos_ia
        
        return resultado
    
    def crear_json_humano(self) -> Dict[str, Any]:
        """Crea JSON con productos y empresas generados por humano, usando IA si no hay humano"""
        resultado = {
            "empresa": None,
            "productos": []
        }
        
        # Obtener empresa (humano si existe, sino IA)
        empresa_humano = self._obtener_seleccion_humano("empresa")
        empresa_ia = self._obtener_seleccion_ia("empresa")
        resultado["empresa"] = empresa_humano if empresa_humano is not None else empresa_ia
        
        # Obtener productos (humano si existe, sino IA)
        productos = []
        for i in range(len(self.interpretacion.get('productos', []))):
            producto_humano = self._obtener_seleccion_humano("producto", i)
            producto_ia = self._obtener_seleccion_ia("producto", i)
            
            producto_final = producto_humano if producto_humano is not None else producto_ia
            if producto_final:

                cantidad = producto_ia.get("cantidad")
                if cantidad is not None:
                    producto_final["cantidad"] = cantidad

                productos.append(producto_final)
        
        resultado["productos"] = productos
        
        return resultado
    
    def obtener_analistas_json(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extrae todos los JSON de los analistas"""
        analistas_json = {}
        
        for analista, mensajes in self.data.items():
            if analista != 'veredicto':
                json_mensajes = []
                for mensaje in mensajes:
                    json_data = self._extraer_json_desde_markdown(mensaje)
                    if json_data:
                        json_mensajes.append(json_data)
                analistas_json[analista] = json_mensajes
        
        return analistas_json

# Función de utilidad para crear la clase desde un archivo JSON
def crear_historia_manager_desde_archivo(archivo_path: str) -> HistoriaManager:
    """Crea un HistoriaManager desde un archivo JSON"""
    with open(archivo_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return HistoriaManager(data)



def procesar_estructura_completa(data: Dict[str, Any]) -> Dict[str, Any]:
    """Procesa toda la estructura y devuelve ambos JSONs"""
    manager = HistoriaManager(data)
    
    return {
        "seleccion_ia": manager.crear_json_ia(),
        "seleccion_humano": manager.crear_json_humano(),
        "analistas_json": manager.obtener_analistas_json()
    }

def guardar_json(data: Dict[str, Any], archivo_path: str):
    """Guarda un JSON en un archivo"""
    with open(archivo_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# Ejemplo de uso completo
def ejemplo_completo(data_json):
    manager = HistoriaManager(data_json)
    
    # Obtener JSONs
    json_ia = manager.crear_json_ia()
    json_humano = manager.crear_json_humano()
    
    # Guardar en archivos
    guardar_json(json_ia, "seleccion_ia.json")
    guardar_json(json_humano, "seleccion_humano.json")
    
    print("Procesamiento completado. Archivos guardados.")
    return json_ia, json_humano


# Ejemplo de uso
if __name__ == "__main__":
    # Suponiendo que tienes el JSON en una variable llamada 'data'
    # manager = HistoriaManager(data)
    
    # O desde archivo:
    manager = crear_historia_manager_desde_archivo("/home/pepe/Desarrollo/compai-agentes/MessageControler/app/pruebas/historia.json")
    
    json_ia = manager.crear_json_ia()
    json_humano = manager.crear_json_humano()
    
    print("JSON IA:", json.dumps(json_ia, indent=2, ensure_ascii=False))
    print("JSON Humano:", json.dumps(json_humano, indent=2, ensure_ascii=False))
    pass