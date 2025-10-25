import json
import re
import requests
import os
from datetime import datetime, timedelta
from arango import ArangoClient
from typing import Dict, List, Any, Optional

def _float_env(var_name: str, default: float) -> float:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        print(f"⚠️ Valor inválido para {var_name}: {value}. Usando {default}.")
        return default


PROPOSAL_HEALTH_TIMEOUT = _float_env("PROPOSAL_HEALTH_TIMEOUT")
PROPOSAL_REQUEST_TIMEOUT = _float_env("PROPOSAL_REQUEST_TIMEOUT")

URL_DOLIBARR = os.getenv('URL_DOLIBARR')
API_KEY_DOLIBARR= os.getenv('API_KEY_DOLIBARR')

# ==========================================
# PARTE 1: HistoriaManager con cantidades originales
# ==========================================

class HistoriaManager:
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self.metadata = data.get('metadata', {})
        self.veredicto = self.metadata.get('veredicto', {})
        self.interpretacion = self.veredicto.get('interpretacion', {})
   
    def crear_json_humano(self) -> Dict[str, Any]:
        """Extrae empresa y productos del veredicto.interpretacion manteniendo cantidades originales"""
        resultado = {
            "empresa": None,
            "productos": []
        }
        
        # Obtener empresa del veredicto.interpretacion
        if 'empresa' in self.interpretacion:
            empresa_data = self.interpretacion['empresa']
            
            # Primero verificar si hay selección elegida por humano
            if empresa_data.get('elegida_por_humano'):
                resultado["empresa"] = empresa_data['elegida_por_humano']
            # Si no, usar la selección automática
            elif empresa_data.get('seleccion'):
                resultado["empresa"] = empresa_data['seleccion']
            # Si tampoco hay selección, usar el primer candidato de similitudes
            elif empresa_data.get('similitudes') and len(empresa_data['similitudes']) > 0:
                resultado["empresa"] = empresa_data['similitudes'][0]
            # Si solo hay empresa_bruto, usarlo
            elif empresa_data.get('empresa_bruto'):
                resultado["empresa"] = {'name': empresa_data['empresa_bruto']}
        
        # Obtener productos del veredicto.interpretacion
        if 'productos' in self.interpretacion:
            productos_data = self.interpretacion['productos']
            
            if isinstance(productos_data, list):
                for prod_item in productos_data:
                    producto = None
                    
                    # Obtener cantidad original del producto_bruto
                    cantidad_original = 1
                    if 'producto_bruto' in prod_item:
                        cantidad_original = prod_item['producto_bruto'].get('cantidad', 1)
                    
                    # Primero verificar elegida_por_humano
                    if prod_item.get('elegida_por_humano'):
                        producto = prod_item['elegida_por_humano'].copy()
                    # Si no, usar seleccion
                    elif prod_item.get('seleccion'):
                        producto = prod_item['seleccion'].copy()
                    # Si no hay selección, usar producto_bruto
                    elif prod_item.get('producto_bruto'):
                        producto = prod_item['producto_bruto'].copy()
                    
                    # Si tenemos producto, agregar la cantidad original
                    if producto:
                        # Mantener la cantidad original del producto_bruto
                        producto['cantidad'] = cantidad_original
                        resultado["productos"].append(producto)
        
        return resultado

# ==========================================
# PARTE 2: Cliente del Microservicio
# ==========================================

class ProposalMicroserviceClient:
    def __init__(self):
        self.base_url = URL_DOLIBARR.rstrip('/')
        print(f"Configurado para microservicio en: {self.base_url}")
    
    def test_connection(self) -> bool:
        """Verifica que el microservicio esté disponible"""
        try:
            response = requests.get(f"{self.base_url}/healthz", timeout=PROPOSAL_HEALTH_TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                print(f"Microservicio OK: {data}")
                return True
        except Exception as e:
            print(f"Error conectando con microservicio: {e}")
        return False
    
    def crear_proposal_desde_historia(self, historia_data: Dict[str, Any]) -> Dict:
        """Crea un proposal usando el microservicio"""
        #print("Procesando historia para microservicio...")
        
        #manager = HistoriaManager(historia_data)
        #seleccion = historia_data.crear_json_humano()
        
        print(f"Seleccion obtenida del veredicto:")
        
        # Mostrar empresa
        empresa_info = historia_data.get('empresa')
        if isinstance(empresa_info, dict):
            nombre = empresa_info.get('name') or empresa_info.get('text', 'Sin nombre')
            ref = empresa_info.get('ref', 'Sin ID')
            print(f"   - Empresa: {nombre} (ID: {ref})")
        else:
            print(f"   - Empresa: {empresa_info}")
        
        # Mostrar productos con cantidades
        productos = historia_data.get('productos', [])
        print(f"   - Productos: {len(productos)} productos")
        for i, prod in enumerate(productos, 1):
            if isinstance(prod, dict):
                desc = prod.get('descripcion') or prod.get('label') or prod.get('text', 'Sin descripción')
                cant = prod.get('cantidad', 1)
                print(f"      {i}. {desc[:50]}... x{cant}")
        
        if not historia_data.get('productos'):
            return {
                'success': False,
                'error': 'No hay productos en la selección del veredicto'
            }
        
        socid = self._obtener_socid(historia_data.get('empresa'))
        if not socid:
            print("No se encontró ID para la empresa, usando cliente genérico (ID: 1)")
            socid = 1
        
        payload = self._construir_payload_microservicio(
            socid=socid,
            seleccion=historia_data,
            historia_data=historia_data
        )
        
        return self._enviar_al_microservicio(payload)
    
    def _obtener_socid(self, empresa_data: Any) -> Optional[int]:
        """Obtiene el socid de la empresa desde los datos del veredicto"""
        if not empresa_data:
            return None
        
        if isinstance(empresa_data, (int, float)):
            return int(empresa_data)
        
        if isinstance(empresa_data, dict):
            # Buscar ref (que es el ID en Dolibarr)
            if 'ref' in empresa_data:
                try:
                    return int(empresa_data['ref'])
                except (ValueError, TypeError):
                    pass
            
            # Si no hay ref válido, buscar otros campos de ID
            for campo in ['id', '_id', 'socid']:
                if campo in empresa_data:
                    try:
                        return int(empresa_data[campo])
                    except (ValueError, TypeError):
                        pass
            
            # Si solo hay nombre, no podemos determinar el ID real
            nombre = empresa_data.get('name') or empresa_data.get('text')
            if nombre:
                print(f"Empresa '{nombre}' sin ID válido en sistema")
                return None
        
        return None
    
    def _construir_payload_microservicio(self, socid: int, seleccion: Dict, historia_data: Dict) -> Dict:
        """Construye el payload para el microservicio"""
        metadata = historia_data.get('metadata', {})
        veredicto = metadata.get('veredicto', {})
        
        # Obtener comentarios del crítico
        comentarios = veredicto.get('comentarios_critico', '')
        
        historia_id = historia_data.get('_key') or historia_data.get('id')
        
        # Procesar líneas de productos
        lines = []
        productos = seleccion.get('productos', [])
        
        for producto in productos:
            if not producto:
                continue
            
            line = self._crear_linea_microservicio(producto)
            if line:
                lines.append(line)
        
        # Si no hay líneas válidas, crear una por defecto
        if not lines:
            lines = [{
                'desc': 'Servicio según especificaciones',
                'qty': 1,
                'subprice': 100.0,
                'tva_tx': 21.0,
                'product_type': 1
            }]
        
        # Obtener fecha actual para el proposal
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')
        fecha_validez = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        
        payload = {
            'socid': socid,
            'ref_client': f"ANAL_{historia_id}" if historia_id else "ANAL_001",
            'date': fecha_hoy,
            'date_valid': fecha_validez,
            'note_public': comentarios[:500] if comentarios else "",
            'note_private': f"Análisis ID: {historia_id}" if historia_id else "",
            'lines': lines
        }
        
        return payload
    
    def _crear_linea_microservicio(self, producto: Any) -> Optional[Dict]:
        """Crea una línea para el microservicio usando datos del veredicto con cantidades originales"""
        if not producto:
            return None
        
        if isinstance(producto, dict):
            # Obtener descripción
            descripcion = (producto.get('descripcion') or 
                          producto.get('label') or 
                          producto.get('text') or 
                          producto.get('desc'))
            
            # Obtener código/referencia
            codigo = (producto.get('codigo') or 
                     producto.get('ref') or 
                     producto.get('reference', ''))
            
            # Si no hay descripción válida, no podemos crear línea
            if not descripcion:
                return None
            
            # IMPORTANTE: Obtener cantidad - ahora viene del producto procesado
            cantidad = producto.get('cantidad', 1)
            try:
                cantidad = float(str(cantidad).replace(',', '.'))
                if cantidad <= 0:
                    cantidad = 1
            except (ValueError, TypeError):
                cantidad = 1
            
            # Obtener precio - usar multiprices si existe
            precio = None
            
            # Primero intentar multiprices
            if 'multiprices' in producto:
                multiprices = producto['multiprices']
                # Buscar primer precio válido en multiprices
                for key in ['1', '2', '3', '4', '5', '6', '7', '8']:
                    if key in multiprices and multiprices[key]:
                        try:
                            precio_str = str(multiprices[key]).replace(',', '.')
                            precio_temp = float(precio_str)
                            if precio_temp > 0:
                                precio = precio_temp
                                break
                        except (ValueError, TypeError):
                            continue
            
            # Si no hay precio en multiprices, buscar en otros campos
            if not precio or precio <= 0:
                for campo in ['precio_unitario', 'subprice', 'price']:
                    if campo in producto and producto[campo]:
                        try:
                            precio_temp = float(str(producto[campo]).replace(',', '.'))
                            if precio_temp > 0:
                                precio = precio_temp
                                break
                        except (ValueError, TypeError):
                            continue
            
            # Si aún no hay precio válido, usar precio por defecto
            if not precio or precio <= 0:
                precio = 100.0
            
            # Construir línea con cantidad correcta
            line = {
                'desc': str(descripcion).strip(),
                'qty': cantidad,  # Cantidad del producto_bruto
                'subprice': precio,
                'tva_tx': 21.0,
                'remise_percent': 0.0,
                'product_type': 0  # 0 para producto físico por defecto
            }
            
            # Agregar código si existe
            if codigo and str(codigo).strip():
                line['ref'] = str(codigo).strip()
            
            # Agregar product_id si existe
            product_id = producto.get('product_id') or producto.get('fk_product')
            if product_id:
                try:
                    line['product_id'] = int(product_id)
                except (ValueError, TypeError):
                    pass
            
            return line
        
        return None
    
    def _enviar_al_microservicio(self, payload: Dict) -> Dict:
        """Envía el payload al microservicio"""
        print("\nEnviando al microservicio...")
        print(f"Payload:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        
        try:
            response = requests.post(
                f"{self.base_url}/api/index.php/proposals",
                json=payload,
                headers={
                    "DOLAPIKEY": API_KEY_DOLIBARR,
                    'Content-Type': 'application/json'
                    },
                timeout=PROPOSAL_REQUEST_TIMEOUT
            )
            
            print(f"Respuesta: {response.status_code}")
            
            if response.status_code in [200, 201]:
                data = response.json()
                print(f"Proposal creado exitosamente!")
                #print(f"   ID: {data.get('id')}")
                
                return {
                    'success': True,
                    'proposal_id': response.text, #data.get('id'),
                    #'ref': data.get('ref'),
                    #'lines_inserted': data.get('lines_inserted'),
                    'message': f"Proposal creado con éxito. ID: {response.text}",
                    'url': f"{URL_DOLIBARR}/comm/propal/card.php?id={response.text}"
                }
            else:
                error_data = {}
                try:
                    error_data = response.json()
                except:
                    error_data = {'raw': response.text}
                
                print(f"Error del microservicio: {response.status_code}")
                print(f"   Detalle: {error_data}")
                
                return {
                    'success': False,
                    'error': f"Error HTTP {response.status_code}",
                    'error_detail': error_data,
                    'message': f"Error al crear proposal via microservicio"
                }
                
        except requests.exceptions.ConnectionError:
            return {
                'success': False,
                'error': 'No se pudo conectar con el microservicio',
                'message': f'Verifica que el microservicio esté ejecutándose en {self.base_url}'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Error inesperado: {str(e)}'
            }

# ==========================================
# PARTE 3: Sistema completo con ArangoDB
# ==========================================

class ArangoProposalSystem:
    def __init__(
        self,
        arango_config: Dict
    ):
        """Sistema completo: ArangoDB → Microservicio → Dolibarr"""
        self.arango_config = arango_config
        self.db = self._conectar_arango()
        self.client = ProposalMicroserviceClient()
        
    def _conectar_arango(self):
        """Conecta con ArangoDB"""
        try:
            client = ArangoClient(hosts=self.arango_config['host'])
            db = client.db(
                self.arango_config['database'],
                username=self.arango_config['username'],
                password=self.arango_config['password']
            )
            print(f"Conectado a ArangoDB: {self.arango_config['database']}")
            return db
        except Exception as e:
            print(f"Error conectando a ArangoDB: {e}")
            raise
    
    def obtener_historia(self, historia_id: str, collection_name: str = 'tareas') -> Dict:
        """Lee una historia desde ArangoDB"""
        try:
            collection = self.db.collection(collection_name)
            historia = collection.get(historia_id)
            
            if historia:
                print(f"Historia obtenida: {historia_id}")
                return historia
            else:
                print(f"No se encontró la historia: {historia_id}")
                return None
                
        except Exception as e:
            print(f"Error leyendo de ArangoDB: {e}")
            return None
    
    def listar_historias_pendientes(self, collection_name: str = 'tareas', limit: int = 10) -> List:
        """Lista historias que no tienen proposal creado"""
        try:
            query = """
            FOR doc IN @@collection
                FILTER doc.proposal_id == null OR !HAS(doc, "proposal_id")
                FILTER doc.metadata.veredicto != null
                FILTER doc.metadata.veredicto.interpretacion != null
                LIMIT @limit
                RETURN {
                    id: doc._key,
                    tiene_veredicto: doc.metadata.veredicto != null,
                    tiene_interpretacion: doc.metadata.veredicto.interpretacion != null,
                    empresa: doc.metadata.veredicto.interpretacion.empresa.empresa_bruto,
                    productos_count: LENGTH(doc.metadata.veredicto.interpretacion.productos)
                }
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={'@collection': collection_name, 'limit': limit}
            )
            
            pendientes = list(cursor)
            print(f"Encontradas {len(pendientes)} historias pendientes")
            return pendientes
            
        except Exception as e:
            print(f"Error listando pendientes: {e}")
            return []
    
    def crear_proposal_desde_id(self, historia_id: str, collection_name: str = 'tareas') -> Dict:
        """Crea un proposal desde un ID de historia en ArangoDB"""
        print(f"\n{'='*60}")
        print(f"Procesando historia: {historia_id}")
        print(f"{'='*60}")
        
        historia = self.obtener_historia(historia_id, collection_name)
        if not historia:
            return {
                'success': False,
                'error': f'Historia {historia_id} no encontrada en ArangoDB'
            }
        
        resultado = self.client.crear_proposal_desde_historia(historia)
        
        if resultado['success']:
            self._actualizar_historia_con_proposal(
                historia_id, 
                resultado['proposal_id'],
                collection_name
            )
        
        return resultado
    
    def _actualizar_historia_con_proposal(self, historia_id: str, proposal_id: int, collection_name: str):
        """Actualiza la historia en ArangoDB con el ID del proposal creado"""
        try:
            collection = self.db.collection(collection_name)
            collection.update({
                '_key': historia_id,
                'proposal_id': proposal_id,
                'proposal_fecha': str(datetime.now()),
                'proposal_url': f"http://alcazar:8033/comm/propal/card.php?id={proposal_id}"
            })
            print(f"Historia actualizada con proposal_id: {proposal_id}")
        except Exception as e:
            print(f"No se pudo actualizar historia: {e}")
    
    def procesar_pendientes(self, max_procesar: int = 5):
        """Procesa historias pendientes y crea proposals"""
        print("\nPROCESANDO HISTORIAS PENDIENTES")
        print("="*60)
        
        pendientes = self.listar_historias_pendientes(limit=max_procesar)
        
        if not pendientes:
            print("No hay historias pendientes")
            return {'procesadas': 0, 'exitosas': 0, 'errores': 0}
        
        exitosas = 0
        errores = 0
        resultados = []
        
        for item in pendientes:
            historia_id = item['id']
            print(f"\nProcesando {historia_id}...")
            print(f"   Empresa: {item.get('empresa', 'N/A')}")
            print(f"   Productos: {item.get('productos_count', 0)}")
            
            resultado = self.crear_proposal_desde_id(historia_id)
            
            if resultado['success']:
                exitosas += 1
                print(f"   ✓ Proposal creado: ID {resultado['proposal_id']}")
                resultados.append({
                    'historia_id': historia_id,
                    'proposal_id': resultado['proposal_id'],
                    'url': resultado['url']
                })
            else:
                errores += 1
                print(f"   ✗ Error: {resultado['error']}")
                resultados.append({
                    'historia_id': historia_id,
                    'error': resultado['error']
                })
        
        print("\n" + "="*60)
        print(f"RESUMEN:")
        print(f"   - Procesadas: {len(pendientes)}")
        print(f"   - Exitosas: {exitosas}")
        print(f"   - Errores: {errores}")
        print("="*60)
        
        return {
            'procesadas': len(pendientes),
            'exitosas': exitosas,
            'errores': errores,
            'resultados': resultados
        }

# ==========================================
# SCRIPT PRINCIPAL
# ==========================================

if __name__ == "__main__":
    
    # CONFIGURACION
    arango_config = {
        'host': 'http://alcazar:8529',
        'database': 'compai_db',
        'username': 'root',
        'password': 'sabbath'
    }
    
    print("SISTEMA DE CREACION DE PROPOSALS")
    print("="*60)
    
    sistema = ArangoProposalSystem(
        arango_config=arango_config,
    )
    
    print("\nOPCIONES:")
    print("1. Procesar UNA historia específica")
    print("2. Procesar TODAS las pendientes")
    print("3. Listar pendientes sin procesar")
    print("4. Procesar la primera disponible")
    print("5. Test rápido con la historia conocida")
    
    opcion = input("\nElige opción (1-5): ")
    
    if opcion == "1":
        historia_id = input("ID de la historia: ")
        resultado = sistema.crear_proposal_desde_id(historia_id)
        
        if resultado['success']:
            print(f"\n✓ Proposal creado exitosamente!")
            print(f"   ID: {resultado['proposal_id']}")
            print(f"   URL: {resultado['url']}")
        else:
            print(f"\n✗ Error: {resultado['error']}")
    
    elif opcion == "2":
        max_num = input("Cuantas procesar? (default 5): ")
        max_num = int(max_num) if max_num else 5
        
        resumen = sistema.procesar_pendientes(max_procesar=max_num)
    
    elif opcion == "3":
        pendientes = sistema.listar_historias_pendientes(limit=20)
        
        if pendientes:
            print(f"\nHISTORIAS PENDIENTES ({len(pendientes)}):")
            for p in pendientes:
                print(f"   - {p['id']}")
                if p.get('empresa'):
                    print(f"     Empresa: {p['empresa']}")
                print(f"     Productos: {p.get('productos_count', 0)}")
        else:
            print("\nNo hay historias pendientes")
    
    elif opcion == "4":
        pendientes = sistema.listar_historias_pendientes(limit=1)
        if pendientes:
            historia_id = pendientes[0]['id']
            print(f"Procesando: {historia_id}")
            resultado = sistema.crear_proposal_desde_id(historia_id)
            if resultado['success']:
                print(f"\n✓ Proposal creado exitosamente!")
                print(f"   ID: {resultado['proposal_id']}")
                print(f"   URL: {resultado['url']}")
            else:
                print(f"\n✗ Error: {resultado['error']}")
        else:
            print("No hay historias pendientes")
    
    elif opcion == "5":
        # Test rápido con la historia conocida
        historia_id = "76ad36a7-cac8-4b68-8bc9-a5789121535b"
        print(f"Procesando historia de prueba: {historia_id}")
        resultado = sistema.crear_proposal_desde_id(historia_id)
        if resultado['success']:
            print(f"\n✓ Proposal creado exitosamente!")
            print(f"   ID: {resultado['proposal_id']}")
            print(f"   URL: {resultado['url']}")
        else:
            print(f"\n✗ Error: {resultado['error']}")
