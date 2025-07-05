from fastapi import APIRouter, HTTPException

from src.db.mongodb_client import mongo_client

router = APIRouter(
    tags=["MongoDB"],
)


@router.get("/products/{product_id}/reviews")
def get_product_reviews(product_id: str):
    """
    Fetches all reviews for a specific product from MongoDB.
    """
    reviews = list(mongo_client.get_collection("reviews").find({"product_id": product_id}, {'_id': 0}))
    if not reviews:
        return {"message": "No reviews found for this product."}
    return reviews


@router.get("/products/{product_id}/specs")
def get_product_specs(product_id: str):
    """
    Fetches the detailed specifications for a product from MongoDB.
    """
    specs = mongo_client.get_collection("product_specs").find_one({"product_id": product_id}, {'_id': 0})
    if not specs:
        raise HTTPException(status_code=404, detail="Specifications not found for this product.")
    return specs


@router.get("/seller_profiles")
def get_seller_profiles():
    """Fetches all seller profiles from MongoDB."""
    profiles = mongo_client.get_collection("seller_profiles").find({}, {'_id': 0})
    return list(profiles)


@router.get("/user_preferences")
def get_user_preferences():
    """Fetches all user preferences from MongoDB."""
    profiles = mongo_client.get_collection("user_preferences").find({}, {'_id': 0})
    return list(profiles)
