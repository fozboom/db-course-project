"""Redis connection and utilities."""

import redis
import json
from typing import Optional, Any
from src.config import REDIS_CONFIG, CACHE_TTL


class RedisClient:
    def __init__(self):
        self.client = redis.Redis(**REDIS_CONFIG)

    def get_json(self, key: str) -> Optional[Any]:
        """Get JSON data from Redis."""
        data = self.client.get(key)
        return json.loads(data) if data else None

    def set_json(self, key: str, value: Any, ttl: int = CACHE_TTL) -> bool:
        """Set JSON data in Redis with TTL."""
        return self.client.setex(key, ttl, json.dumps(value))

    def add_to_cart(self, user_id: str, product_id: str, quantity: int):
        """Add item to user's cart."""
        cart_key = f"cart:{user_id}"
        # TODO: Implement cart logic
        pass

    def rate_limit_check(self, user_id: str, endpoint: str) -> bool:
        """Check if user has exceeded rate limit."""
        # TODO: Implement rate limiting logic
        pass


# Singleton instance
redis_client = RedisClient()
