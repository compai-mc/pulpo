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
        for doc in self.collection.find(
            {"config_id": config_id}
        ):
            os.environ[doc["name"]] = str(doc["value"])

    def put(
        self,
        config_id: str,
        key: str,
        value: str,
    ):
        self.collection.insert(
            {
                "_key": f"{config_id}_{key}",
                "config_id": config_id,
                "name": key,
                "value": value,
            },
            overwrite=True,
        )

    def put_config(self, config_id: str, config: dict):
        for key, value in config.items():
            self.put(
                config_id=config_id,
                key=key,
                value=str(value),
            )