from fastapi import APIRouter, HTTPException, Query

from src.api.models import CartItem, CartItemUpdate
from src.db.redis_client import redis_client
from src.services import product_service
from src.services.cart_service import cart_service

router = APIRouter(
    tags=["Redis"],
)


@router.get("/products/{product_id}/cached")
def get_product_from_cache(product_id: str, user_id: str | None = Query(None)):
    """
    Fetches a product, demonstrating caching with Redis.
    """
    # Using the dedicated product service which has caching built-in
    product = product_service.get_product_by_id(product_id, user_id=user_id)

    if not product:
        raise HTTPException(status_code=404, detail="Product not found.")

    # We need a way to know if it was a cache hit. The service prints it.
    # For an API, the service should return this info. Let's simulate it.
    cache_key = f"product:{product_id}"
    if redis_client.client.exists(cache_key):
        product['cache_status'] = 'HIT'
    else:
        product['cache_status'] = 'MISS'

    return product


@router.get("/products/hot")
def get_hot_products():
    """
    Gets the top 10 'hot products' from a Redis Sorted Set.
    The score is incremented on each product view.
    """
    hot_products = redis_client.get_hot_products(top_n=10)
    return {"hot_products": hot_products}


@router.get("/cache-metrics")
def get_cache_metrics():
    """
    Gets cache hit/miss statistics from Redis.
    This is useful for monitoring cache effectiveness.
    """
    return redis_client.get_cache_metrics()


@router.post("/cart/{user_id}")
def add_item_to_cart(user_id: str, item: CartItem):
    """Adds a product to the user's shopping cart (stored in a Redis Hash)."""
    try:
        cart_service.add_to_cart(user_id, item.product_id, item.quantity)
        return {"message": f"Added {item.quantity} of product {item.product_id} to cart for user {user_id}."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.put("/cart/{user_id}/items/{product_id}")
def update_cart_item_quantity(user_id: str, product_id: str, item_update: CartItemUpdate):
    """Updates the quantity of a specific item in the cart. A quantity of 0 will remove the item."""
    try:
        cart_service.update_item_quantity(user_id, product_id, item_update.quantity)
        return {"message": f"Updated product {product_id} quantity to {item_update.quantity} for user {user_id}."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@router.get("/cart/{user_id}")
def get_user_cart(user_id: str):
    """Retrieves the full shopping cart for a user from Redis."""
    return cart_service.get_cart(user_id)


@router.delete("/cart/{user_id}/items/{product_id}")
def remove_item_from_cart(user_id: str, product_id: str):
    """Removes a specific item from a user's shopping cart in Redis."""
    cart_service.remove_from_cart(user_id, product_id)
    return {"message": f"Removed product {product_id} from cart for user {user_id}."}


@router.delete("/cart/{user_id}")
def clear_user_cart(user_id: str):
    """Clears all items from a user's shopping cart."""
    cart_service.clear_cart(user_id)
    return {"message": f"Cart for user {user_id} has been cleared."}


@router.post("/cart/{user_id}/checkout")
def checkout(user_id: str):
    """Converts the user's cart into an order."""
    try:
        result = cart_service.convert_cart_to_order(user_id)
        return {"message": "Order created successfully", "order_details": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
