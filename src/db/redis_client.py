"""Redis connection and utilities."""

import json
from typing import Any

import redis

from src.config import CACHE_TTL, CART_TTL, REDIS_CONFIG


class RedisClient:
    def __init__(self):
        self.client = redis.Redis(**REDIS_CONFIG)

    def get_json(self, key: str) -> Any | None:
        """Get JSON data from Redis, handling potential decoding errors."""
        data = self.client.get(key)
        if not data:
            return None
        try:
            return json.loads(data)  # type: ignore[arg-type]
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON for key {key}")
            return None

    def set_json(self, key: str, value: Any, ttl: int = CACHE_TTL) -> bool:
        """Set JSON data in Redis with TTL."""
        return self.client.setex(key, ttl, json.dumps(value))  # type: ignore[return-value]

    def increment_hot_product_score(self, product_id: str, increment_by: int = 1):
        """Increment the score of a product in the hot products sorted set."""
        hot_products_key = "hot_products"
        self.client.zincrby(hot_products_key, increment_by, product_id)

    def get_hot_products(self, top_n: int = 10) -> list[tuple[str, float]]:
        """Get the top N hot products with their scores."""
        hot_products_key = "hot_products"
        # zrevrange with withscores=True and decode_responses=True directly returns list[tuple[str, float]]
        return self.client.zrevrange(hot_products_key, 0, top_n - 1, withscores=True)  # type: ignore[return-value]

    def get_cart_key(self, user_id: str) -> str:
        """Generate the Redis key for a user's cart."""
        return f"cart:{user_id}"

    def add_to_cart(self, user_id: str, product_id: str, quantity: int = 1):
        """Add a product to the user's cart or increment its quantity."""
        cart_key = self.get_cart_key(user_id)
        self.client.hincrby(cart_key, product_id, quantity)
        self.client.expire(cart_key, CART_TTL)

    def update_cart_item_quantity(self, user_id: str, product_id: str, quantity: int):
        """Set a specific quantity for a product in the cart."""
        cart_key = self.get_cart_key(user_id)
        self.client.hset(cart_key, product_id, str(quantity))
        self.client.expire(cart_key, CART_TTL)

    def remove_from_cart(self, user_id: str, product_id: str):
        """Remove a product from the user's cart."""
        cart_key = self.get_cart_key(user_id)
        self.client.hdel(cart_key, product_id)

    def get_cart(self, user_id: str) -> dict[str, int]:
        """Retrieve the user's shopping cart, safely handling data types."""
        cart_key = self.get_cart_key(user_id)
        cart_data = self.client.hgetall(cart_key)
        cart = {}
        for product, quantity_str in cart_data.items():  # type: ignore[union-attr]
            try:
                cart[product] = int(quantity_str)
            except (ValueError, TypeError):
                print(f"Warning: Invalid quantity '{quantity_str}' for product '{product}' in cart for user '{user_id}'. Skipping.")
                continue
        return cart

    def clear_cart(self, user_id: str):
        """Clear all items from a user's cart."""
        cart_key = self.get_cart_key(user_id)
        self.client.delete(cart_key)

    def increment_cache_metric(self, metric_name: str):
        """Increment a cache metric (e.g., 'hits' or 'misses')."""
        self.client.incr(f"cache_metrics:{metric_name}")

    def get_cache_metrics(self) -> dict[str, int]:
        """Get all cache metrics like hits and misses by scanning keys."""
        metrics = {}
        # Use scan_iter for performance; it avoids blocking the server like KEYS.
        for key in self.client.scan_iter("cache_metrics:*"):
            # 'key' is a string because decode_responses=True
            metric_name = key.split(":", 1)[1]  # Extracts 'hits' from 'cache_metrics:hits'
            value = self.client.get(key)
            metrics[metric_name] = int(value) if value else 0  # type: ignore[arg-type]
        return metrics


# Singleton instance
redis_client = RedisClient()
