from fastapi import APIRouter, HTTPException

from src.services.search_service import semantic_search_service

router = APIRouter(
    tags=["pgvector"],
)


@router.get("/search")
def search_products(query: str):
    """
    Performs a semantic search for products using pgvector.
    """
    if not query.strip():
        raise HTTPException(status_code=400, detail="Search query cannot be empty.")

    results = semantic_search_service.natural_language_search(query)
    return {"query": query, "results": results}


@router.get("/products/{product_id}/similar")
def find_similar_products(product_id: str):
    """
    Finds products that are semantically similar to a given product.
    """
    similar_products = semantic_search_service.find_similar_products(product_id, top_k=5)
    if not similar_products:
        return {"message": "Could not find any similar products."}
    return {"product_id": product_id, "similar_products": similar_products}
