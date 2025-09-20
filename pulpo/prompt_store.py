from arango import ArangoClient
from datetime import datetime
from typing import Optional, List, Dict


class PromptStore:
    def __init__(self, db_url: str, db_name: str, username: str, password: str, collection_name: str = "prompts"):
        # Inicializar cliente
        self.client = ArangoClient()
        self.sys_db = self.client.db("_system", username=username, password=password)

        # Crear base de datos si no existe
        if not self.sys_db.has_database(db_name):
            self.sys_db.create_database(db_name)

        # Conectar a la base de datos
        self.db = self.client.db(db_name, username=username, password=password)

        # Crear colección si no existe
        if not self.db.has_collection(collection_name):
            self.db.create_collection(collection_name)

        self.collection = self.db.collection(collection_name)

    def add_prompt(self, name: str, text: str, metadata: Optional[Dict] = None) -> str:
        """Inserta un prompt en ArangoDB y devuelve la clave del documento"""
        doc = {
            "name": name,
            "text": text,
            "metadata": metadata or {},
            "created_at": datetime.utcnow().isoformat()
        }
        result = self.collection.insert(doc, overwrite=False)
        return result["_key"]

    def get_prompt(self, key: str) -> Optional[Dict]:
        """Obtiene un prompt por clave"""
        return self.collection.get(key)

    def get_prompts_by_name(self, name: str) -> List[Dict]:
        """Obtiene todos los prompts con un nombre dado"""
        cursor = self.db.aql.execute(
            "FOR p IN @@col FILTER p.name == @name RETURN p",
            bind_vars={"@col": self.collection.name, "name": name}
        )
        return list(cursor)

    def update_prompt(self, key: str, text: Optional[str] = None, metadata: Optional[Dict] = None) -> bool:
        """Actualiza un prompt existente"""
        doc = self.collection.get(key)
        if not doc:
            return False

        if text:
            doc["text"] = text
        if metadata:
            doc["metadata"].update(metadata)

        doc["updated_at"] = datetime.utcnow().isoformat()
        self.collection.update(doc)
        return True

    def delete_prompt(self, key: str) -> bool:
        """Elimina un prompt por clave"""
        try:
            self.collection.delete(key)
            return True
        except:
            return False
