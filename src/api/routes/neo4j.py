from fastapi import APIRouter

from src.db.neo4j_client import neo4j_client

router = APIRouter(
    tags=["Neo4j"],
)


@router.get("/users/{user_id}/recommendations")
def get_recommendations(user_id: str):
    """
    Generates product recommendations for a user using Neo4j.
    """
    recommendations = neo4j_client.get_recommendations(user_id)

    if not recommendations:
        return {"message": "No recommendations found. This user may be new or has no overlapping purchases with others."}

    return {"user_id": user_id, "recommendations": recommendations}


@router.get("/products/{product_id}/also-bought")
def get_also_bought(product_id: str):
    """
    Gets products also bought by customers who purchased this product.
    """
    also_bought = neo4j_client.get_also_bought_products(product_id, limit=5)
    if not also_bought:
        return {"message": "Could not find any related products."}
    return {"product_id": product_id, "also_bought": also_bought}
