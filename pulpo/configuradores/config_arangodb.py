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
        application: str | None = None,
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

        self.application = application

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
            scope = doc.get("scope", "global")

            if scope == "global":
                configuracion["global"][nombre] = valor
                os.environ[nombre] = valor

            elif scope == self.application:
                configuracion["especifico"][nombre] = valor
                os.environ[nombre] = valor

        return configuracion

    def put(
        self,
        config_id: str,
        key: str,
        value: str,
        scope: str = "global",
    ):
        self.collection.insert(
            {
                "_key": f"{config_id}_{scope}_{key}",
                "config_id": config_id,
                "name": key,
                "value": value,
                "scope": scope,
            },
            overwrite=True,
        )

    def delete(
        self,
        config_id: str,
        key: str,
        scope: str = "global",
    ):
        document_key = f"{config_id}_{scope}_{key}"

        if self.collection.has(document_key):
            self.collection.delete(document_key)

        # También eliminarla del entorno del proceso
        os.environ.pop(key, None)

    def put_config(
        self,
        config_id: str,
        config: dict,
    ):
        for scope, valores in config.items():

            for key, value in valores.items():

                self.put(
                    config_id=config_id,
                    key=key,
                    value=str(value),
                    scope=scope,
                )