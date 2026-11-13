
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from urllib.parse import quote

from pulpo.util.util import require_env


if __name__ == "__main__": #######   PRUEBAS
    STORAGE_ACCESS_KEY = "cb0e5e5a60f9b346f719dc97"
    STORAGE_SECRET_KEY = "2b60cd199f82630f03be8f25c5ffd39db7debe7b9da09f47ad573e4add44b46d"
    CONFIG_ID = "local"
    STORAGE_BUCKET = "pruebas-rustfs"
    STORAGE_URL_EXTERNA = "http://alcazar:9700"
    STORAGE_URL = "http://alcazar:9700"
else:  # REAL
    STORAGE_ACCESS_KEY = require_env("STORAGE_ACCESS_KEY")
    STORAGE_SECRET_KEY = require_env("STORAGE_SECRET_KEY")
    CONFIG_ID = require_env("CONFIG_ID")    
    STORAGE_URL_EXTERNA = require_env("STORAGE_URL_EXTERNA")
    STORAGE_URL = require_env("STORAGE_URL")


class AlmacenObjetos:

    def __init__(self, bucket: str = None):

        self.config_id = CONFIG_ID

        self.endpoint = (
            STORAGE_URL_EXTERNA
            if self.config_id == "local"
            else STORAGE_URL
        )

        self.access_key = STORAGE_ACCESS_KEY
        self.secret_key = STORAGE_SECRET_KEY

        self.bucket = bucket

        # Implementación actual: API compatible con S3
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"}
            )
        )

    def put(
        self,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream"
    ):
        """Almacena un objeto."""
        return self.client.put_object(
            Bucket=self.bucket,
            Key=object_name.lstrip("/"),
            Body=data,
            ContentType=content_type
        )

    def get(self, object_name: str) -> bytes:
        """Recupera un objeto."""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=object_name.lstrip("/")
        )

        try:
            return response["Body"].read()
        finally:
            response["Body"].close()

    def exists(self, object_name: str) -> bool:
        """Comprueba si existe un objeto."""
        try:
            self.client.head_object(
                Bucket=self.bucket,
                Key=object_name.lstrip("/")
            )
            return True

        except ClientError as e:
            status = e.response["ResponseMetadata"]["HTTPStatusCode"]

            if status == 404:
                return False

            raise

    def delete(self, object_name: str):
        """Elimina un objeto."""
        return self.client.delete_object(
            Bucket=self.bucket,
            Key=object_name.lstrip("/")
        )

    def list(self, prefix: str = ""):
        """Enumera los objetos de un prefijo."""
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=self.bucket,
            Prefix=prefix.lstrip("/")
        ):
            for obj in page.get("Contents", []):
                yield obj

    def get_url(self, object_name: str) -> str:
        """Devuelve la URL directa del objeto."""
        return (
            f"{self.endpoint.rstrip('/')}"
            f"/{self.bucket}"
            f"/{quote(object_name.lstrip('/'), safe='/')}"
        )

    def get_signed_url(self, object_name: str, expires: int = 3600) -> str:
        """Genera una URL temporal firmada para descargar un objeto."""
        return self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.bucket,
                "Key": object_name.lstrip("/")
            },
            ExpiresIn=expires
        )

    def close(self):
        """Cierra el cliente de almacenamiento."""
        self.client.close()




#########################################################
##############   Test  ##################################
#########################################################



if __name__ == "__main__":

    almacen = AlmacenObjetos(STORAGE_BUCKET)

    try:
        object_name = "analysis/reembolsos/resultado2.json"

        contenido = b'{"workspace": "reembolsos", "estado": "OK"}'

        # 1. Guardar
        print("\n1. Guardando objeto...")
        respuesta = almacen.put(
            object_name,
            contenido,
            "application/json"
        )
        print("Objeto guardado:", respuesta.get("ETag"))

        # 2. Comprobar existencia
        print("\n2. Comprobando existencia...")
        existe = almacen.exists(object_name)
        print("Existe:", existe)

        # 3. Recuperar
        print("\n3. Recuperando objeto...")
        recuperado = almacen.get(object_name)
        print("Contenido:", recuperado.decode("utf-8"))

        # 4. Verificar contenido
        print("\n4. Verificando contenido...")
        assert recuperado == contenido, "El contenido no coincide"
        print("Contenido verificado correctamente")

        # 5. Listar objetos
        print("\n5. Listando objetos...")
        for objeto in almacen.list("analysis/reembolsos/"):
            print(f"{objeto['Key']} ({objeto['Size']} bytes)")

        # 6. Generar URL temporal
        print("\n6. Generando URL temporal...")
        url = almacen.get_signed_url(object_name)
        print(url)

        print("\nTODAS LAS PRUEBAS FINALIZADAS CORRECTAMENTE")

    finally:
        almacen.close()
