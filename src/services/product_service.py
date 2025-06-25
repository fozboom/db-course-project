"""
Service layer for handling product-related logic,
demonstrating caching and other Redis features.
"""

import time

from src.db.postgres_client import db
from src.db.redis_client import redis_client


def get_product_from_db(product_id: str) -> dict | None:
    """Fetch a single product directly from PostgreSQL."""
    with db.get_cursor() as cursor:
        cursor.execute(
            "SELECT * FROM products WHERE id = %s",
            (product_id,),
        )
        product = cursor.fetchone()
        if product and "price" in product:
            product["price"] = float(product["price"])
        return product


def get_product_by_id(product_id: str) -> dict:
    """
    Get product by ID, using Redis as a cache.
    Also increments the product's view count for the 'hot products' list.
    """
    cache_key = f"product:{product_id}"
    start_time = time.monotonic()

    # Check cache
    cached_product = redis_client.get_json(cache_key)
    if cached_product:
        source = "CACHE"
        product = cached_product
    else:
        # Query database on cache miss
        source = "DATABASE"
        product = get_product_from_db(product_id)
        if product:
            # Store in cache for future requests
            redis_client.set_json(cache_key, product)

    end_time = time.monotonic()
    duration = (end_time - start_time) * 1000  # in milliseconds

    if product:
        # Increment score in the 'hot products' sorted set
        redis_client.increment_hot_product_score(product_id)
        print(f"Fetched product {product_id} from {source} in {duration:.2f}ms")
    else:
        print(f"Product {product_id} not found.")

    return product


if __name__ == "__main__":
    print("--- Simulating product views to demonstrate caching and hot products ---")

    # List of product IDs to simulate views for
    product_ids_to_view = ["P002", "P005", "P003", "P001", "P019", "P004", "P001"]

    for pid in product_ids_to_view:
        print(f"\nRequesting product: {pid}")
        get_product_by_id(pid)
        time.sleep(0.1)  # Small delay between requests

    print("\n--- Final Demonstration ---")
    print("Requesting P001 again, should be very fast (from CACHE):")
    get_product_by_id("P001")

    print("\nTop 5 Hot Products:")
    hot_products = redis_client.get_hot_products(top_n=5)
    for i, (prod_id, score) in enumerate(hot_products):
        print(f"{i + 1}. Product ID: {prod_id}, Views: {int(score)}")

    print("\nClearing 'hot_products' key for next run...")
    redis_client.client.delete("hot_products")
