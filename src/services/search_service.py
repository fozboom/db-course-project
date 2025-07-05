"""Service for semantic product search using vector embeddings."""

import hashlib
from typing import Any

import numpy as np
from sentence_transformers import SentenceTransformer

from src.db.postgres_client import db as postgres_db
from src.db.redis_client import redis_client


class SemanticSearchService:
    """Encapsulates semantic search logic using pgvector and caching."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        print(f"SentenceTransformer model '{model_name}' loaded.")

    def _generate_cache_key(self, params: dict) -> str:
        sorted_params = sorted(params.items())
        param_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        return f"semantic_search:{hashlib.md5(param_string.encode()).hexdigest()}"

    def _get_embedding(self, text: str) -> np.ndarray:
        embedding = self.model.encode(text)
        if not isinstance(embedding, np.ndarray):
            return np.array(embedding)
        return embedding

    def natural_language_search(
        self,
        query: str,
        category: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        top_k: int = 10,
    ) -> list[dict]:
        search_params = {
            "query": query,
            "category": category,
            "min_price": min_price,
            "max_price": max_price,
            "top_k": top_k,
        }
        active_params = {k: v for k, v in search_params.items() if v is not None}
        cache_key = self._generate_cache_key(active_params)

        cached_results = redis_client.get_json(cache_key)
        if cached_results is not None:
            redis_client.increment_cache_metric("semantic_hits")
            print(f"Semantic cache hit for key: {cache_key}")
            return cached_results

        redis_client.increment_cache_metric("semantic_misses")
        print(f"Semantic cache miss for key: {cache_key}. Querying database.")

        query_embedding = self._get_embedding(query)

        sql = """
            SELECT
                p.id, p.name, p.description, p.price, c.name as category_name,
                1 - (pe.embedding <=> %s) AS similarity
            FROM products p
            JOIN product_embeddings pe ON p.id = pe.product_id
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE 1=1
        """
        sql_params: list[Any] = [query_embedding]

        if category:
            sql += " AND c.name = %s"
            sql_params.append(category)
        if min_price is not None:
            sql += " AND p.price >= %s"
            sql_params.append(min_price)
        if max_price is not None:
            sql += " AND p.price <= %s"
            sql_params.append(max_price)

        sql += " ORDER BY similarity DESC LIMIT %s;"
        sql_params.append(top_k)

        with postgres_db.get_cursor() as cursor:
            cursor.execute(sql, tuple(sql_params))
            results = [dict(row) for row in cursor.fetchall()]
            for r in results:
                if r.get("price"):
                    r["price"] = float(r["price"])
                if r.get("similarity"):
                    r["similarity"] = float(r["similarity"])

        redis_client.set_json(cache_key, results)
        return results

    def find_similar_products(self, product_id: str, top_k: int = 5) -> list[dict]:
        cache_key = f"similar_products:{product_id}:{top_k}"
        cached_results = redis_client.get_json(cache_key)
        if cached_results is not None:
            redis_client.increment_cache_metric("similar_hits")
            return cached_results

        redis_client.increment_cache_metric("similar_misses")

        sql = """
            WITH target_embedding AS (
                SELECT embedding FROM product_embeddings WHERE product_id = %s
            )
            SELECT
                p.id, p.name, p.description, p.price, c.name as category_name,
                1 - (pe.embedding <=> (SELECT embedding FROM target_embedding)) AS similarity
            FROM products p
            JOIN product_embeddings pe ON p.id = pe.product_id
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE p.id != %s
            ORDER BY similarity DESC
            LIMIT %s;
        """

        with postgres_db.get_cursor() as cursor:
            cursor.execute(sql, (product_id, product_id, top_k))
            results = [dict(row) for row in cursor.fetchall()]
            for r in results:
                if r.get("price"):
                    r["price"] = float(r["price"])
                if r.get("similarity"):
                    r["similarity"] = float(r["similarity"])

        redis_client.set_json(cache_key, results)
        return results

    def get_cache_stats(self) -> dict[str, int]:
        stats = redis_client.client.keys("cache_metrics:*")
        metrics = {}
        for key in stats:
            metric_name = key.split(":")[1]
            value = redis_client.client.get(key)
            metrics[metric_name] = int(value) if value else 0
        print(f"Cache stats: {metrics}")
        return metrics


semantic_search_service = SemanticSearchService()
