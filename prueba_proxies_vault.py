import os
import sys
import runpy
import json
import urllib.request
from typing import Callable

import hvac


VAULT_ADDR = os.getenv("VAULT_ADDR", "http://alcazar:8228")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
VAULT_PATHS = [
    path.strip()
    for path in os.getenv("VAULT_PATHS", "des/compai").split(",")
    if path.strip()
]
VAULT_MOUNT_POINT = os.getenv("VAULT_MOUNT_POINT", "secret")
CONFIG_SERVICE_URL = os.getenv("CONFIG_SERVICE_URL", "http://alcazar:7416")
CONFIG_SERVICE_IDS = [
    config_id.strip()
    for config_id in os.getenv("CONFIG_SERVICE_IDS", "compai_global").split(",")
    if config_id.strip()
]

DEV_DEFAULT_ENV = {
    "URL_CONFIG_SERVICE": "http://alcazar:7416",
    "PROCCESSCONTROLER_URL": "http://alcazar:7417",
    "BASE_URL_ORQUESTATOR": "http://alcazar:7419",
    "CONTABILIDAD_URL": "http://alcazar:7423",
    "ERPDOLIBARR_URL": "http://alcazar:7404",
}


BOOTSTRAP_REQUIRED_ENV = [
    "SEC_KEYCLOAK_URL",
    "SEC_REALM",
]

ENV_ALIASES = {
    "KEYCLOAK_URL": "SEC_KEYCLOAK_URL",
    "REALM": "SEC_REALM",
    "CONFIG_SERVICE_URL": "URL_CONFIG_SERVICE",
    "CLIENT_ID_ORQUESTATORFLOW": "CLIENT_ID_ORQUESTATOR",
    "CLIENT_SECRET_ORQUESTATORFLOW": "CLIENT_SECRET_ORQUESTATOR",
    "CLIENT_ID_PROCESSCONTROLLER": "CLIENT_ID_PROCCESSCONTROLER",
    "CLIENT_SECRET_PROCESSCONTROLLER": "CLIENT_SECRET_PROCCESSCONTROLER",
}


def load_vault_recursive() -> None:
    if not VAULT_TOKEN:
        raise RuntimeError(
            "Falta VAULT_TOKEN. En PowerShell: $env:VAULT_TOKEN='tu-token'"
        )

    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)

    if not client.is_authenticated():
        raise RuntimeError("Vault no autentica con el token indicado.")

    def load_path(path: str) -> None:
        try:
            secret = client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=VAULT_MOUNT_POINT,
                raise_on_deleted_version=True,
            )
            data = secret.get("data", {}).get("data", {})
            for key, value in data.items():
                os.environ[str(key)] = str(value)
        except hvac.exceptions.InvalidPath:
            pass

        try:
            listing = client.secrets.kv.v2.list_secrets(
                path=path,
                mount_point=VAULT_MOUNT_POINT,
            )
            for key in listing.get("data", {}).get("keys", []):
                if key.endswith("/"):
                    load_path(f"{path.rstrip('/')}/{key.rstrip('/')}")
        except hvac.exceptions.InvalidPath:
            pass

    for vault_path in VAULT_PATHS:
        load_path(vault_path)


def apply_env_aliases() -> None:
    for source, target in ENV_ALIASES.items():
        if not os.getenv(target) and os.getenv(source):
            os.environ[target] = os.environ[source]


def apply_dev_defaults() -> None:
    if os.getenv("DISABLE_DEV_DEFAULTS") == "1":
        return

    for key, value in DEV_DEFAULT_ENV.items():
        os.environ.setdefault(key, value)


def load_config_service_direct() -> None:
    """
    Bootstrap config-service without using proxy_config_service.

    The proxy itself is secured, so this direct unauthenticated read is only for
    local/dev bootstrap before pulpo.auth can be imported.
    """
    if os.getenv("SKIP_CONFIG_SERVICE") == "1":
        return

    for config_id in CONFIG_SERVICE_IDS:
        url = f"{CONFIG_SERVICE_URL.rstrip('/')}/config/{config_id}"

        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            print(f"[WARN] No se pudo cargar config-service {url}: {exc}")
            continue

        config = payload.get("config", {})

        if isinstance(config, dict):
            static = config.get("static")
            if isinstance(static, dict):
                for key, value in static.items():
                    os.environ[str(key)] = str(value)

            for key, value in config.items():
                if key == "static":
                    continue
                if isinstance(value, (dict, list)):
                    continue
                os.environ[str(key)] = str(value)


def assert_bootstrap_env() -> None:
    missing = [
        name for name in BOOTSTRAP_REQUIRED_ENV
        if not os.getenv(name)
    ]

    if not missing:
        return

    loaded_keys = sorted(
        key for key in os.environ
        if key.startswith(("SEC_", "CLIENT_", "URL_", "BASE_URL_", "ERPDOLIBARR", "FORECAST", "PROCCESS"))
    )

    message = [
        "Faltan variables obligatorias antes de inicializar auth:",
        ", ".join(missing),
        "",
        f"Vault usado: {VAULT_ADDR}",
        f"Vault paths: {', '.join(VAULT_PATHS)}",
        f"Vault mount point: {VAULT_MOUNT_POINT}",
        f"Config-service URL: {CONFIG_SERVICE_URL}",
        f"Config-service IDs: {', '.join(CONFIG_SERVICE_IDS)}",
        "",
        "Variables relacionadas cargadas:",
        ", ".join(loaded_keys) if loaded_keys else "(ninguna)",
        "",
        "Soluciones habituales:",
        '1. Revisa si las variables estan en otro path y usa, por ejemplo: $env:VAULT_PATHS="des/clients,des/compai"',
        '2. Revisa si config-service usa otro id y define: $env:CONFIG_SERVICE_IDS="compai_global,otro_id"',
        '3. Si Vault/config-service usa otros nombres, define alias manuales antes de ejecutar: $env:SEC_KEYCLOAK_URL=$env:KEYCLOAK_URL',
        '4. Las minimas para auth son SEC_KEYCLOAK_URL y SEC_REALM.',
    ]

    raise RuntimeError("\n".join(message))


def bootstrap_service_token() -> None:
    apply_env_aliases()
    apply_dev_defaults()
    assert_bootstrap_env()

    from pulpo.auth.general import Auth, set_service_token

    explicit_token = os.getenv("PROXY_TEST_SERVICE_TOKEN")
    if explicit_token:
        set_service_token(explicit_token)
        return

    if os.getenv("DEV_API_KEY") and os.getenv("PROXY_TEST_USE_DEV_API_KEY", "1") == "1":
        set_service_token(os.environ["DEV_API_KEY"])
        return

    client_id_env = os.getenv("PROXY_TEST_CLIENT_ID_ENV", "CLIENT_ID_FRONT")
    client_secret_env = os.getenv(
        "PROXY_TEST_CLIENT_SECRET_ENV",
        "CLIENT_SECRET_FRONT",
    )

    client_id = os.getenv(client_id_env)
    client_secret = os.getenv(client_secret_env)

    if not client_id or not client_secret:
        raise RuntimeError(
            "No puedo inicializar token de servicio. Define DEV_API_KEY en dev, "
            "PROXY_TEST_SERVICE_TOKEN, o PROXY_TEST_CLIENT_ID_ENV/"
            "PROXY_TEST_CLIENT_SECRET_ENV apuntando a variables cargadas desde Vault."
        )

    Auth(
        client_id=client_id,
        client_secret=client_secret,
    ).login_service()


def run_case(name: str, fn: Callable[[], object]) -> bool:
    print(f"\n[TEST] {name}")
    try:
        result = fn()
        print("[OK]", summarize(result))
        return True
    except Exception as exc:
        print("[FAIL]", type(exc).__name__, exc)
        return False


def summarize(value: object) -> str:
    text = repr(value)
    if len(text) > 500:
        return text[:500] + "..."
    return text


def import_result(module_name: str, attr_name: str) -> str:
    module = __import__(module_name, fromlist=[attr_name])
    attr = getattr(module, attr_name)
    return getattr(attr, "__name__", repr(attr))


def main() -> int:
    print(f"Vault: {VAULT_ADDR}")
    print(f"Vault paths: {', '.join(VAULT_PATHS)}")
    print(f"Config-service: {CONFIG_SERVICE_URL}")
    print(f"Config-service IDs: {', '.join(CONFIG_SERVICE_IDS)}")

    load_vault_recursive()
    load_config_service_direct()
    apply_env_aliases()
    apply_dev_defaults()
    assert_bootstrap_env()
    bootstrap_service_token()

    tests: list[tuple[str, Callable[[], object]]] = [
        (
            "contabilidad.instancia",
            lambda: type(
                __import__(
                    "pulpo.proxies.proxy_contabilidad",
                    fromlist=["ContabilidadClient"],
                ).ContabilidadClient()
            ).__name__,
        ),
        (
            "config.health",
            lambda: __import__(
                "pulpo.proxies.proxy_config_service",
                fromlist=["ConfigClient"],
            ).ConfigClient().health(),
        ),
        (
            "correo.import",
            lambda: import_result(
                "pulpo.proxies.proxy_correo",
                "CorreoClient",
            ),
        ),
        (
            "erp_sincrono.health",
            lambda: __import__(
                "pulpo.proxies.proxy_erpdolibarr_sincrono",
                fromlist=["ERPProxySincrono"],
            ).ERPProxySincrono(os.environ["ERPDOLIBARR_URL"]).health(),
        ),
        (
            "dolibarr.clientes",
            lambda: __import__(
                "pulpo.proxies.proxy_dolibarr",
                fromlist=["DolibarrProxy"],
            ).DolibarrProxy().clientes(),
        ),
        (
            "forecast.import",
            lambda: import_result(
                "pulpo.proxies.proxy_forecast",
                "ForecastProxy",
            ),
        ),
        (
            "forecast_copy.carga_modulo",
            lambda: list(
                runpy.run_path(
                    os.path.join(
                        os.getcwd(),
                        "pulpo",
                        "proxies",
                        "proxy_forecast copy.py",
                    )
                ).keys()
            )[:5],
        ),
        (
            "memory.import",
            lambda: import_result(
                "pulpo.proxies.proxy_memory",
                "MemoryAPIClient",
            ),
        ),
        (
            "orquestator.instancia",
            lambda: type(
                __import__(
                    "pulpo.proxies.proxy_orquestator_flow",
                    fromlist=["OrchestratorProxy"],
                ).OrchestratorProxy()
            ).__name__,
        ),
        (
            "proccess_controler.similarity_client",
            lambda: __import__(
                "pulpo.proxies.proxy_proccess_controler",
                fromlist=["ProccessControlerProxy"],
            ).ProccessControlerProxy().similarity_client(
                "test",
                numero_resultados=1,
            ),
        ),
    ]

    if os.getenv("PROXY_TEST_CONTABILIDAD_READ") == "1":
        tests.append(
            (
                "contabilidad.listar_cuentas_contables",
                lambda: __import__(
                    "pulpo.proxies.proxy_contabilidad",
                    fromlist=["ContabilidadClient"],
                ).ContabilidadClient().listar_cuentas_contables(),
            )
        )

    if os.getenv("PROXY_TEST_MEMORY") == "1":
        tests.append(
            (
                "memory.list_memories",
                lambda: __import__(
                    "pulpo.proxies.proxy_memory",
                    fromlist=["MemoryAPIClient"],
                ).MemoryAPIClient().list_memories(),
            )
        )

    if os.getenv("PROXY_TEST_FORECAST") == "1":
        from pulpo.proxies.proxy_forecast import generar_forecast_minio
        import asyncio

        tests.append(
            (
                "forecast.generate",
                lambda: asyncio.run(
                    generar_forecast_minio(
                        os.getenv("PROXY_TEST_FORECAST_FECHA", "2026-01-01")
                    )
                ),
            )
        )

    if os.getenv("PROXY_TEST_CORREO") == "1":
        tests.append(
            (
                "correo.enviar_correo",
                lambda: __import__(
                    "pulpo.proxies.proxy_correo",
                    fromlist=["CorreoClient"],
                ).CorreoClient(
                    os.getenv("PROXY_TEST_CORREO_URL") or os.environ["CORREO_URL"]
                ).enviar_correo(
                    destinatario=os.environ["PROXY_TEST_CORREO_DESTINATARIO"],
                    asunto=os.getenv("PROXY_TEST_CORREO_ASUNTO", "Prueba proxy correo"),
                    mensaje=os.getenv("PROXY_TEST_CORREO_MENSAJE", "Prueba automatica"),
                ),
            )
        )

    if os.getenv("PROXY_TEST_ORQUESTATOR") == "1":
        tests.append(
            (
                "orquestator.post_test",
                lambda: __import__(
                    "pulpo.proxies.proxy_orquestator_flow",
                    fromlist=["OrchestratorProxy"],
                ).OrchestratorProxy()._post(
                    os.environ["PROXY_TEST_ORQUESTATOR_PATH"],
                    {},
                ),
            )
        )

    print("\n[INFO] Por defecto no se ejecutan operaciones con efectos secundarios.")
    print("       Flags opcionales: PROXY_TEST_MEMORY=1, PROXY_TEST_FORECAST=1,")
    print("       PROXY_TEST_CORREO=1, PROXY_TEST_ORQUESTATOR=1,")
    print("       PROXY_TEST_CONTABILIDAD_READ=1.")

    ok = 0
    for name, fn in tests:
        ok += 1 if run_case(name, fn) else 0

    total = len(tests)
    print(f"\nResultado: {ok}/{total} tests OK")
    return 0 if ok == total else 1


if __name__ == "__main__":
    sys.exit(main())
