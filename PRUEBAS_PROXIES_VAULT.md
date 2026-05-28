# Pruebas locales de proxies con Vault

## Ubicacion del programa

El programa de pruebas esta en la raiz del repositorio:

```text
prueba_proxies_vault.py
```

Ruta local:

```text
C:\Users\srgbe\Documents\GitHub\pulpo\prueba_proxies_vault.py
```

## Objetivo

Validar desde local que los proxies de `pulpo/proxies`:

- cargan configuracion y secretos desde Vault;
- inicializan la autenticacion inter-micro;
- usan `MicroTokenManager` y `MicroHttpClient`;
- pueden hacer llamadas reales a los micros con token Bearer;
- no ejecutan operaciones con efectos secundarios salvo que se activen explicitamente.

## Configuracion base

Abrir PowerShell en la raiz del repositorio:

```powershell
cd C:\Users\srgbe\Documents\GitHub\pulpo
```

Configurar Vault:

```powershell
$env:VAULT_ADDR="http://alcazar:8228"
$env:VAULT_TOKEN="PON_AQUI_EL_TOKEN_DE_VAULT"
$env:VAULT_PATHS="des/clients"
```

Configurar config-service de desarrollo:

```powershell
$env:CONFIG_SERVICE_URL="http://alcazar:7416"
$env:CONFIG_SERVICE_IDS="compai_global"
```

El script carga config-service directamente antes de importar `pulpo.auth`, porque el proxy de config-service ya esta securizado y necesita esas variables para arrancar.

Si config-service no devuelve alguna URL, el script aplica estos defaults de desarrollo basados en los contenedores:

```text
URL_CONFIG_SERVICE=http://alcazar:7416
PROCCESSCONTROLER_URL=http://alcazar:7417
BASE_URL_ORQUESTATOR=http://alcazar:7419
CONTABILIDAD_URL=http://alcazar:7423
ERPDOLIBARR_URL=http://alcazar:7404
```

Para desactivar estos defaults:

```powershell
$env:DISABLE_DEV_DEFAULTS="1"
```

Si `des/clients` solo contiene `CLIENT_ID_*` y `CLIENT_SECRET_*`, pero la configuracion global esta en otro path de Vault, cargar ambos paths separados por coma:

```powershell
$env:VAULT_PATHS="des/clients,des/compai"
```

Si el mount point de Vault no es `secret`, configurarlo tambien:

```powershell
$env:VAULT_MOUNT_POINT="secret"
```

## Ejecucion recomendada

El entorno recomendado para este repo es Python 3.12. Ya existe un virtualenv local:

```text
.venv312
```

Ejecutar smoke tests seguros con ese Python:

```powershell
.\.venv312\Scripts\python.exe .\prueba_proxies_vault.py
```

Estas pruebas no envian correos, no crean cards, no generan forecast y no crean memoria.

## Inicializacion del token de servicio

El script intenta inicializar el token de servicio asi:

1. Si existe `PROXY_TEST_SERVICE_TOKEN`, lo usa directamente.
2. Si `APP_ENV=dev` y existe `DEV_API_KEY`, usa `DEV_API_KEY`.
3. Si no, hace `login_service()` con:
   - `CLIENT_ID_FRONT`
   - `CLIENT_SECRET_FRONT`

Si quieres usar otro cliente origen para la prueba:

```powershell
$env:PROXY_TEST_CLIENT_ID_ENV="CLIENT_ID_ORQUESTATOR"
$env:PROXY_TEST_CLIENT_SECRET_ENV="CLIENT_SECRET_ORQUESTATOR"
python .\prueba_proxies_vault.py
```

## Pruebas incluidas por defecto

| Proxy | Prueba | Tipo | Riesgo |
|---|---|---|---|
| `proxy_contabilidad.py` | Instanciar `ContabilidadClient` | Import/config/auth setup | Sin llamada HTTP |
| `proxy_config_service.py` | `ConfigClient().health()` | HTTP real | Solo lectura |
| `proxy_correo.py` | Instanciar `CorreoClient` | Import/config/auth setup | No envia correo |
| `proxy_dolibarr.py` | `DolibarrProxy().clientes()` | HTTP real | Solo lectura |
| `proxy_erpdolibarr_sincrono.py` | `ERPProxySincrono(...).health()` | HTTP real | Solo lectura |
| `proxy_forecast.py` | Importar `ForecastProxy` | Import/config/auth setup | No genera forecast |
| `proxy_forecast copy.py` | Cargar modulo por ruta | Import/config/auth setup | No genera forecast |
| `proxy_memory.py` | Importar `MemoryAPIClient` | Import/config/auth setup | No crea memoria |
| `proxy_orquestator_flow.py` | Instanciar `OrchestratorProxy` | Import/config/auth setup | No crea cards |
| `proxy_proccess_controler.py` | `similarity_client("test", numero_resultados=1)` | HTTP real | Solo lectura |

## Pruebas opcionales con efectos secundarios

Activar solo si se quiere probar el flujo real de esos micros.

## Prueba completa del proxy ERP Dolibarr sincrono

El script especifico esta en:

```text
prueba_proxy_erp_dolibarr_sincrono.py
```

Ejecuta todos los endpoints GET del proxy que pueda resolver con datos reales obtenidos de los listados. Las operaciones `POST`, `PUT` y `PATCH` quedan en `SKIP` por defecto para evitar crear o modificar datos.

Ejecucion segura:

```powershell
$env:VAULT_ADDR="http://alcazar:8228"
$env:VAULT_TOKEN="PON_AQUI_EL_TOKEN_DE_VAULT"
$env:VAULT_PATHS="des/clients"
$env:CONFIG_SERVICE_URL="http://alcazar:7416"
$env:CONFIG_SERVICE_IDS="compai_global"

.\.venv312\Scripts\python.exe .\prueba_proxy_erp_dolibarr_sincrono.py
```

Este script fuerza por defecto el ERP Dolibarr a:

```text
http://alcazar:7404
```

Aunque config-service devuelva otro valor para `ERPDOLIBARR_URL`. Para cambiarlo:

```powershell
$env:ERP_TEST_BASE_URL="http://alcazar:7404"
```

Variables opcionales para endpoints que requieren ids concretos:

```powershell
$env:ERP_TEST_FECHA="2026-05"
$env:ERP_TEST_START="2026-01-01"
$env:ERP_TEST_END="2026-05-25"
$env:ERP_TEST_BANK_ACCOUNT_ID="1"
$env:ERP_TEST_CLIENT_ID="1"
$env:ERP_TEST_CLIENT_PHONE="600000000"
$env:ERP_TEST_PRODUCT_ID="1"
$env:ERP_TEST_PRODUCT_REF="REF"
$env:ERP_TEST_INVOICE_ID="1"
$env:ERP_TEST_PROPOSAL_ID="1"
$env:ERP_TEST_PROPOSAL_NAME="PROV-XXXX"
$env:ERP_TEST_SHIPMENT_ID="1"
$env:ERP_TEST_SHIPMENT_ORIGIN_ID="1"
$env:ERP_TEST_SHIPMENT_PARAMETRO="tracking_number"
$env:ERP_TEST_SUPPLIER_ID="1"
```

Para activar tambien `POST`, `PUT` y `PATCH`:

```powershell
$env:ERP_TEST_MUTATING="1"
.\.venv312\Scripts\python.exe .\prueba_proxy_erp_dolibarr_sincrono.py
```

Usar `ERP_TEST_MUTATING=1` solo en entorno de pruebas, porque puede crear propuestas, contactos, shipments, sincronizar pedidos o modificar conciliaciones.

### Contabilidad con llamada real

```powershell
$env:PROXY_TEST_CONTABILIDAD_READ="1"
.\.venv312\Scripts\python.exe .\prueba_proxies_vault.py
```

Antes de importar `pulpo.auth.general`, el script comprueba que existan:

```text
SEC_KEYCLOAK_URL
SEC_REALM
```

Tambien aplica estos alias si existen en Vault:

```text
KEYCLOAK_URL -> SEC_KEYCLOAK_URL
REALM -> SEC_REALM
CONFIG_SERVICE_URL -> URL_CONFIG_SERVICE
CLIENT_ID_ORQUESTATORFLOW -> CLIENT_ID_ORQUESTATOR
CLIENT_SECRET_ORQUESTATORFLOW -> CLIENT_SECRET_ORQUESTATOR
CLIENT_ID_PROCESSCONTROLLER -> CLIENT_ID_PROCCESSCONTROLER
CLIENT_SECRET_PROCESSCONTROLLER -> CLIENT_SECRET_PROCCESSCONTROLER
```

Si falta `SEC_KEYCLOAK_URL`, normalmente significa que `VAULT_PATHS` no incluye el path donde esta la configuracion global de seguridad.
Tambien puede significar que `CONFIG_SERVICE_IDS` no incluye el id de config-service donde esta esa variable.

Ejecuta:

```python
ContabilidadClient().listar_cuentas_contables()
```

Nota: este proxy se dejo sin cambios por decision explicita. La prueba por defecto solo lo instancia.

### Memory

```powershell
$env:PROXY_TEST_MEMORY="1"
.\.venv312\Scripts\python.exe .\prueba_proxies_vault.py
```

Ejecuta:

```python
MemoryAPIClient().list_memories()
```

Importante: `MemoryAPIClient()` crea/asegura una memoria en su `__init__`.

### Forecast

```powershell
$env:PROXY_TEST_FORECAST="1"
$env:PROXY_TEST_FORECAST_FECHA="2026-01-01"
.\.venv312\Scripts\python.exe .\prueba_proxies_vault.py
```

Ejecuta:

```python
generar_forecast_minio(...)
```

Puede generar archivos o procesos en el micro de forecast.

### Correo

```powershell
$env:PROXY_TEST_CORREO="1"
$env:PROXY_TEST_CORREO_URL="http://URL_DEL_MICRO_CORREO"
$env:PROXY_TEST_CORREO_DESTINATARIO="destino@empresa.com"
$env:PROXY_TEST_CORREO_ASUNTO="Prueba proxy correo"
$env:PROXY_TEST_CORREO_MENSAJE="Prueba automatica de proxy correo"
.\.venv312\Scripts\python.exe .\prueba_proxies_vault.py
```

Ejecuta:

```python
CorreoClient(...).enviar_correo(...)
```

Envia un correo real.

### Orquestator

```powershell
$env:PROXY_TEST_ORQUESTATOR="1"
$env:PROXY_TEST_ORQUESTATOR_PATH="/health-or-test-endpoint"
.\.venv312\Scripts\python.exe .\prueba_proxies_vault.py
```

Ejecuta un POST al path indicado:

```python
OrchestratorProxy()._post(PROXY_TEST_ORQUESTATOR_PATH, {})
```

Usar solo con un endpoint de prueba. No usar `/card/create` salvo que se quiera crear una card real.

## Resultado esperado

Al final debe aparecer:

```text
Resultado: X/X tests OK
```

Si una prueba falla, el script muestra:

```text
[FAIL] TipoDeError detalle
```

## Validaciones tecnicas adicionales

Comprobar sintaxis:

```powershell
.\.venv312\Scripts\python.exe -m compileall .\pulpo\proxies .\pulpo\auth\general.py
```

Comprobar que los proxies no usan llamadas directas `requests` o `httpx`:

```powershell
rg -n 'requests\.|httpx\.|import requests|import httpx' .\pulpo\proxies
```

Si no devuelve resultados, los proxies no tienen llamadas HTTP directas fuera de `MicroHttpClient`.

## Variables esperadas en Vault

El path `des/clients` debe contener las variables necesarias para los proxies:

```text
SEC_KEYCLOAK_URL
SEC_REALM
APP_ENV
DEV_API_KEY                         # solo si se prueba en dev
CLIENT_ID_FRONT
CLIENT_SECRET_FRONT
CLIENT_ID_CONFIG_SERVICE
CLIENT_SECRET_CONFIG_SERVICE
CLIENT_ID_CORREO
CLIENT_SECRET_CORREO
CLIENT_ID_ERPDOLIBARR
CLIENT_SECRET_ERPDOLIBARR
CLIENT_ID_FORECAST
CLIENT_SECRET_FORECAST
CLIENT_ID_MEMORY
CLIENT_SECRET_MEMORY
CLIENT_ID_ORQUESTATOR
CLIENT_SECRET_ORQUESTATOR
CLIENT_ID_PROCCESSCONTROLER
CLIENT_SECRET_PROCCESSCONTROLER
URL_CONFIG_SERVICE
ERPDOLIBARR_URL
FORECAST_URL
URL_MEMORY
BASE_URL_ORQUESTATOR
PROCCESSCONTROLER_URL
```

## Nota de seguridad

No guardar tokens reales en este documento ni en Git. Configurarlos siempre como variables de entorno locales.
