import os

from arango import ArangoClient


def require_env(name: str) -> str:
    value = os.getenv(name)

    if value is None:
        raise RuntimeError(
            f"La variable '{name}' no existe"
        )

    return value


class ArangoEnv:

    def __init__(
        self,
        host: str,
        port: int,
        database: str = "config",
        username: str = "root",
        password: str = "arangodb123",
        collection: str = "env",
    ):
        client = ArangoClient(
            hosts=f"http://{host}:{port}"
        )

        self.db = client.db(
            database,
            username=username,
            password=password,
        )

        self.collection = self.db.collection(collection)

    def load(self, config_id: str):

        configuracion = {
            "global": {},
            "especifico": {},
        }

        for doc in self.collection.find(
            {"config_id": config_id}
        ):
            nombre = doc["name"]
            valor = str(doc["value"])
            tipo = doc.get("tipo", "especifico")

            # También dejamos las variables disponibles
            # directamente en el entorno
            os.environ[nombre] = valor

            configuracion[tipo][nombre] = valor

        return configuracion

    def put(
        self,
        config_id: str,
        key: str,
        value: str,
        tipo: str = "especifico",
    ):
        self.collection.insert(
            {
                "_key": f"{config_id}_{key}",
                "config_id": config_id,
                "name": key,
                "value": value,
                "tipo": tipo,
            },
            overwrite=True,
        )

    def put_config(
        self,
        config_id: str,
        config: dict,
    ):
        for tipo in ("global", "especifico"):

            valores = config.get(tipo, {})

            for key, value in valores.items():

                self.put(
                    config_id=config_id,
                    key=key,
                    value=str(value),
                    tipo=tipo,
                )