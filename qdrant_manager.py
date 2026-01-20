"""
Servicio de Gestión de Colecciones de Qdrant
Usa metadata NATIVA de colección (Qdrant >= 1.16)
"""
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PayloadSchemaType
)
from pydantic import BaseModel


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

DEFAULT_VECTOR_SIZE = 768
DEFAULT_DISTANCE = Distance.COSINE


# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class CollectionCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    vector_size: int = DEFAULT_VECTOR_SIZE
    distance: str = "Cosine"


class CollectionUpdateRequest(BaseModel):
    description: Optional[str] = None


class CollectionStats(BaseModel):
    name: str
    description: Optional[str]
    vector_size: int
    distance: str
    points_count: int
    indexed_vectors_count: int
    status: str
    created_at: Optional[str]


# ============================================================================
# GESTOR DE COLECCIONES
# ============================================================================

DEFAULT_PAYLOAD_INDEXES = {
    "document_hash": PayloadSchemaType.KEYWORD,
    "filename": PayloadSchemaType.KEYWORD,
    "format": PayloadSchemaType.KEYWORD,
    "chunk": PayloadSchemaType.INTEGER,
    "total_pages": PayloadSchemaType.INTEGER,
    "date": PayloadSchemaType.KEYWORD,
}


class QdrantCollectionManager:
    _instance = None
    _client: Optional[QdrantClient] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            print(f"🔗 Conectando a Qdrant: {QDRANT_HOST}:{QDRANT_PORT}")
            self._client = QdrantClient(
                url=f"http://{QDRANT_HOST}:{QDRANT_PORT}",
                timeout=60
            )
            print("✅ Cliente Qdrant inicializado")

    # ========================================================================
    # CRUD
    # ========================================================================

    def create_collection(
        self,
        name: str,
        description: Optional[str],
        vector_size: int,
        distance: str
    ) -> Dict[str, Any]:

        if self.collection_exists(name):
            raise ValueError(f"La colección '{name}' ya existe")

        distance_map = {
            "Cosine": Distance.COSINE,
            "Euclid": Distance.EUCLID,
            "Dot": Distance.DOT
        }
        distance_metric = distance_map.get(distance, Distance.COSINE)

        metadata = {
            "description": description,
            "created_at": datetime.utcnow().isoformat()
        }

        self._client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(
                size=vector_size,
                distance=distance_metric
            ),
            metadata=metadata
        )

        for field, schema in DEFAULT_PAYLOAD_INDEXES.items():
            try:
                self._client.create_payload_index(
                    collection_name=name,
                    field_name=field,
                    field_schema=schema
                )
            except Exception as e:
                print(f"⚠️ Índice '{field}' no creado: {e}")

        return {
            "success": True,
            "collection_name": name,
            "message": f"Colección '{name}' creada correctamente"
        }

    def list_collections(self) -> List[CollectionStats]:
        collections = self._client.get_collections().collections
        result = []

        for col in collections:
            info = self._client.get_collection(col.name)
            metadata = info.config.metadata or {}

            result.append(
                CollectionStats(
                    name=col.name,
                    description=metadata.get("description"),
                    vector_size=info.config.params.vectors.size,
                    distance=info.config.params.vectors.distance.name,
                    points_count=info.points_count or 0,
                    indexed_vectors_count=info.indexed_vectors_count or 0,
                    status=info.status.name,
                    created_at=metadata.get("created_at")
                )
            )

        return result

    def get_collection_info(self, name: str) -> CollectionStats:
        if not self.collection_exists(name):
            raise ValueError(f"La colección '{name}' no existe")

        info = self._client.get_collection(name)
        metadata = info.config.metadata or {}

        return CollectionStats(
            name=name,
            description=metadata.get("description"),
            vector_size=info.config.params.vectors.size,
            distance=info.config.params.vectors.distance.name,
            points_count=info.points_count or 0,
            indexed_vectors_count=info.indexed_vectors_count or 0,
            status=info.status.name,
            created_at=metadata.get("created_at")
        )

    def update_collection(self, name: str, description: Optional[str]) -> Dict[str, Any]:
        if not self.collection_exists(name):
            raise ValueError(f"La colección '{name}' no existe")

        metadata = {
            "description": description,
            "updated_at": datetime.utcnow().isoformat()
        }

        self._client.update_collection(
            collection_name=name,
            metadata=metadata
        )

        return {
            "success": True,
            "collection_name": name,
            "message": f"Colección '{name}' actualizada correctamente"
        }

    def delete_collection(self, name: str, force: bool = False) -> Dict[str, Any]:
        if not self.collection_exists(name):
            raise ValueError(f"La colección '{name}' no existe")

        info = self._client.get_collection(name)
        if info.points_count > 0 and not force:
            raise ValueError(
                f"La colección '{name}' tiene vectores. Use force=true."
            )

        self._client.delete_collection(name)

        return {
            "success": True,
            "collection_name": name,
            "message": f"Colección '{name}' eliminada correctamente"
        }

    def clear_collection(self, name: str) -> Dict[str, Any]:
        if not self.collection_exists(name):
            raise ValueError(f"La colección '{name}' no existe")

        info = self._client.get_collection(name)
        metadata = info.config.metadata

        self._client.delete_collection(name)
        self._client.create_collection(
            collection_name=name,
            vectors_config=info.config.params.vectors,
            metadata=metadata
        )

        return {
            "success": True,
            "collection_name": name,
            "message": f"Colección '{name}' limpiada correctamente"
        }

    def collection_exists(self, name: str) -> bool:
        return name in [c.name for c in self._client.get_collections().collections]


# ========================================================================
# GESTIÓN DE DOCUMENTOS
# ========================================================================

    def delete_document_by_filename(
        self, 
        collection_name: str, 
        filename: str
    ) -> Dict[str, Any]:
        """
        Elimina todos los puntos (chunks) que pertenecen a un documento específico
        basándose en el campo 'filename' del payload
        """
        if not self.collection_exists(collection_name):
            raise ValueError(f"La colección '{collection_name}' no existe")

        scroll_result = self._client.scroll(
            collection_name=collection_name,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="filename",
                        match=MatchValue(value=filename)
                    )
                ]
            ),
            limit=10000, 
            with_payload=False,
            with_vectors=False
        )

        points_to_delete = scroll_result[0]
        total_points = len(points_to_delete)

        if total_points == 0:
            raise ValueError(
                f"No se encontraron puntos con filename='{filename}' "
                f"en la colección '{collection_name}'"
            )

        self._client.delete(
            collection_name=collection_name,
            points_selector=Filter(
                must=[
                    FieldCondition(
                        key="filename",
                        match=MatchValue(value=filename)
                    )
                ]
            )
        )

        return {
            "success": True,
            "collection_name": collection_name,
            "filename": filename,
            "deleted_points": total_points,
            "message": f"Eliminados {total_points} chunks del documento '{filename}'"
        }

    def list_documents_in_collection(self, collection_name: str) -> Dict[str, Any]:
        """
        Lista todos los documentos únicos en una colección
        agrupados por filename con información de chunks
        """
        if not self.collection_exists(collection_name):
            raise ValueError(f"La colección '{collection_name}' no existe")

        all_points = []
        offset = None
        
        while True:
            scroll_result = self._client.scroll(
                collection_name=collection_name,
                limit=100,
                offset=offset,
                with_payload=True,
                with_vectors=False
            )
            
            points, next_offset = scroll_result
            all_points.extend(points)
            
            if next_offset is None:
                break
            offset = next_offset

        # Agrupamos por filename
        documents = {}
        for point in all_points:
            payload = point.payload or {}
            filename = payload.get("filename", "unknown")
            
            if filename not in documents:
                documents[filename] = {
                    "filename": filename,
                    "document_hash": payload.get("document_hash"),
                    "format": payload.get("format"),
                    "total_pages": payload.get("total_pages"),
                    "total_chunks": payload.get("total_chunks"),
                    "date": payload.get("date"),
                    "chunks_count": 0
                }
            
            documents[filename]["chunks_count"] += 1

        return {
            "collection_name": collection_name,
            "total_documents": len(documents),
            "total_points": len(all_points),
            "documents": list(documents.values())
        }

# ============================================================================
# SINGLETON
# ============================================================================

_collection_manager: Optional[QdrantCollectionManager] = None


def get_collection_manager() -> QdrantCollectionManager:
    global _collection_manager
    if _collection_manager is None:
        _collection_manager = QdrantCollectionManager()
    return _collection_manager
