"""
API REST para Gestión de Colecciones de Qdrant
Endpoints CRUD para administrar colecciones
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from contextlib import asynccontextmanager

from qdrant_manager import (
    get_collection_manager,
    CollectionCreateRequest,
    CollectionUpdateRequest,
    CollectionStats
)


# ==================== LIFESPAN ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicializa el gestor de colecciones al arrancar"""
    print("🚀 Iniciando API de Colecciones...")
    get_collection_manager()
    print("✅ Gestor de colecciones listo")
    
    yield
    
    print("👋 Cerrando API de Colecciones...")


# ==================== APP ====================
app = FastAPI(
    title="Qdrant Collections Manager",
    version="1.0.0",
    description="API para gestionar colecciones de Qdrant"
    #lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== ENDPOINTS ====================

@app.get("/health")
async def health_check():
    """Health check"""
    return {
        "status": "healthy",
        "service": "qdrant-collections-manager",
        "version": "1.0.0"
    }


@app.post("/api/collections", status_code=201)
async def create_collection(request: CollectionCreateRequest):
    """
    Crea una nueva colección en Qdrant
    
    **Parámetros:**
    - name: Nombre único de la colección (Nomenclatura: Curso_curid)
    - description: Descripción opcional
    - vector_size: Dimensión de vectores (default: 768)
    - distance: Métrica de distancia (Cosine, Euclid, Dot)
    
    **Ejemplo:**
    ```json
    {
        "name": "Curso_101",
        "description": "Documentos del área de educación",
        "vector_size": 768,
        "distance": "Cosine"
    }
    ```
    """
    try:
        manager = get_collection_manager()
        result = manager.create_collection(
            name=request.name,
            description=request.description,
            vector_size=request.vector_size,
            distance=request.distance
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/collections", response_model=List[CollectionStats])
async def list_collections():
    """
    Lista todas las colecciones disponibles con sus estadísticas
    
    **Retorna:**
    - Lista de colecciones con nombre, descripción, tamaño de vectores, 
      métrica de distancia, cantidad de puntos y estado
    """
    try:
        manager = get_collection_manager()
        collections = manager.list_collections()
        return collections
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/collections/{collection_name}", response_model=CollectionStats)
async def get_collection(collection_name: str):
    """
    Obtiene información detallada de una colección específica
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    """
    try:
        manager = get_collection_manager()
        info = manager.get_collection_info(collection_name)
        return info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/collections/{collection_name}")
async def update_collection(
    collection_name: str,
    request: CollectionUpdateRequest
):
    """
    Actualiza metadata de una colección
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    - description: Nueva descripción (opcional)
    """
    try:
        manager = get_collection_manager()
        result = manager.update_collection(
            name=collection_name,
            description=request.description
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/collections/{collection_name}")
async def delete_collection(
    collection_name: str,
    force: bool = Query(False, description="Forzar eliminación aunque tenga datos")
):
    """
    Elimina una colección de Qdrant
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    - force: Si True, elimina aunque contenga vectores (query param)
    
    **Ejemplo:**
    ```
    DELETE /api/collections/mi_coleccion?force=true
    ```
    """
    try:
        manager = get_collection_manager()
        result = manager.delete_collection(collection_name, force=force)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/collections/{collection_name}/clear")
async def clear_collection(collection_name: str):
    """
    Limpia todos los vectores de una colección manteniendo su estructura
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    """
    try:
        manager = get_collection_manager()
        result = manager.clear_collection(collection_name)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/collections/{collection_name}/stats")
async def get_collection_stats(collection_name: str):
    """
    Obtiene estadísticas detalladas de una colección
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    """
    try:
        manager = get_collection_manager()
        
        # Info básica
        info = manager.get_collection_info(collection_name)
        
        # Conteo de documentos
        counts = manager.get_collection_documents_count(collection_name)
        
        return {
            **info.dict(),
            **counts
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/collections/{collection_name}/exists")
async def check_collection_exists(collection_name: str):
    """
    Verifica si una colección existe
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    """
    try:
        manager = get_collection_manager()
        exists = manager.collection_exists(collection_name)
        return {
            "collection_name": collection_name,
            "exists": exists
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# ==================== ENDPOINTS DE DOCUMENTOS ====================

@app.get("/api/collections/{collection_name}/documents")
async def list_documents(collection_name: str):
    """
    Lista todos los documentos únicos en una colección
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    
    **Retorna:**
    - Lista de documentos con información de filename, formato, 
      total de páginas, chunks y fecha de carga
    
    **Ejemplo de respuesta:**
    ```json
    {
        "collection_name": "documentos_educacion",
        "total_documents": 3,
        "total_points": 150,
        "documents": [
            {
                "filename": "Aprendo_en_Casa_Curso1.pdf",
                "document_hash": "f282bcdb04b3...",
                "format": "pdf",
                "total_pages": 26,
                "total_chunks": 58,
                "chunks_count": 58,
                "date": "2026-01-20T13:43:34.101354"
            }
        ]
    }
    ```
    """
    try:
        manager = get_collection_manager()
        result = manager.list_documents_in_collection(collection_name)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/collections/{collection_name}/documents/{filename:path}")
async def delete_document(
    collection_name: str,
    filename: str
):
    """
    Elimina un documento completo (todos sus chunks) de una colección
    
    **Parámetros:**
    - collection_name: Nombre de la colección
    - filename: Nombre exacto del archivo a eliminar (incluir extensión)
    
    **Importante:** 
    - Elimina TODOS los chunks/puntos que tengan ese filename
    - Esta operación no se puede deshacer
    
    **Ejemplo:**
    ```
    DELETE /api/collections/documentos_educacion/documents/Aprendo_en_Casa_Curso1_Unidad2_Sesión3.pdf
    ```
    
    **Respuesta exitosa:**
    ```json
    {
        "success": true,
        "collection_name": "documentos_educacion",
        "filename": "Aprendo_en_Casa_Curso1_Unidad2_Sesión3.pdf",
        "deleted_points": 58,
        "message": "Eliminados 58 chunks del documento 'Aprendo_en_Casa_Curso1_Unidad2_Sesión3.pdf'"
    }
    ```
    """
    try:
        manager = get_collection_manager()
        result = manager.delete_document_by_filename(
            collection_name=collection_name,
            filename=filename
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9000)
