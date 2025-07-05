"""
Service layer for handling product-related logic,
demonstrating caching and other Redis features.
"""

import time
from typing import Any

from src.db.neo4j_client import neo4j_client
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


def get_product_by_id(product_id: str, user_id: str | None = None) -> dict | None:
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

        # Create the VIEWED relationship in Neo4j if a user_id is provided
        if user_id:
            neo4j_client.add_view(user_id, product_id)

        print(f"Fetched product {product_id} from {source} in {duration:.2f}ms")
    else:
        print(f"Product {product_id} not found.")

    return product


def search_products(
    query: str | None = None,
    category_id: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Search for products with optional filters and cache the results.
    Tracks cache hit/miss rates.
    """
    # 1. Create a unique cache key from search parameters
    cache_key_parts = [
        "search",
        query or "all",
        category_id or "all",
        f"minp{min_price}" if min_price is not None else "any",
        f"maxp{max_price}" if max_price is not None else "any",
        f"limit{limit}",
    ]
    cache_key = ":".join(cache_key_parts)

    # 2. Check cache first
    cached_results = redis_client.get_json(cache_key)
    if cached_results is not None:
        redis_client.increment_cache_metric("hits")
        print(f"Search results for query '{query}' found in CACHE.")
        return cached_results

    # 3. On cache miss, query the database
    redis_client.increment_cache_metric("misses")
    print(f"Searching DATABASE for query '{query}'...")

    # Build the SQL query dynamically
    sql_query = "SELECT id, name, price, category_id FROM products WHERE 1=1"
    params = []

    if query:
        # Use websearch_to_tsquery for flexible user input.
        # It handles multiple words, quotes, etc.
        # We search across name, description, and tags.
        sql_query += """
            AND to_tsvector('english', name || ' ' || description || ' ' || array_to_string(tags, ' ')) @@ websearch_to_tsquery('english', %s)
        """
        params.append(query)

    if category_id:
        sql_query += " AND category_id = %s"
        params.append(category_id)

    if min_price is not None:
        sql_query += " AND price >= %s"
        params.append(min_price)

    if max_price is not None:
        sql_query += " AND price <= %s"
        params.append(max_price)

    sql_query += " LIMIT %s"
    params.append(limit)

    results = []
    with db.get_cursor() as cursor:
        cursor.execute(sql_query, tuple(params))
        rows = cursor.fetchall()
        # Convert Decimal to float for JSON serialization
        results = [{k: (float(v) if k == "price" else v) for k, v in row.items()} for row in rows]

    # 4. Store results in cache for future requests
    redis_client.set_json(cache_key, results)  # Uses default CACHE_TTL

    return results


if __name__ == "__main__":
    print("--- Simulating product views to demonstrate caching and hot products ---")

    # List of product IDs to simulate views for
    product_ids_to_view = ["P002", "P005", "P003", "P001", "P019", "P004", "P001"]
    simulated_user_id = "U001"  # Example user

    for pid in product_ids_to_view:
        print(f"\nRequesting product: {pid}")
        get_product_by_id(pid, user_id=simulated_user_id)
        time.sleep(0.1)  # Small delay between requests

    print("\n--- Final Demonstration ---")
    print("Requesting P001 again, should be very fast (from CACHE):")
    get_product_by_id("P001", user_id=simulated_user_id)

    print("\nTop 5 Hot Products:")
    hot_products = redis_client.get_hot_products(top_n=5)
    for i, (prod_id, score) in enumerate(hot_products):
        print(f"{i + 1}. Product ID: {prod_id}, Views: {int(score)}")

    print("\n--- Demonstrating search with caching ---")
    search_query = "handcrafted wooden"
    print(f"\n1. First search for '{search_query}' (should be a DB query):")
    search_products(query=search_query)

    print(f"\n2. Second search for '{search_query}' (should be a CACHE hit):")
    search_products(query=search_query)

    print("\n--- Demonstrating filtered search ---")
    print("\nSearching for 'leather journal' under $50 in category 'C005':")
    search_products(query="leather journal", max_price=50.0, category_id="C005")

    print("\n--- Final Cache Metrics ---")
    metrics = redis_client.get_cache_metrics()
    print(f"Cache Hits: {metrics['hits']}, Cache Misses: {metrics['misses']}")

    print("\nClearing temporary keys for next run...")
    redis_client.client.delete("hot_products")
    # A more robust approach would be to scan and delete search:* keys
    # For this example, we'll just let them expire.
