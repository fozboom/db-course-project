"""Load vector embeddings into pgvector."""

import numpy as np
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

from src.db.postgres_client import db
from src.utils.data_parser import DataParser


class VectorLoader:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        self.parser = DataParser()
        self.db = db

    def load_embeddings(self, batch_size: int = 32):
        """Generate and load embeddings for all product descriptions."""
        products = self.parser.parse_products()
        print(f"Generating embeddings for {len(products)} products...")

        # Process in batches for efficiency
        for i in tqdm(range(0, len(products), batch_size)):
            batch = products.iloc[i : i + batch_size]

            texts = (batch["name"] + " " + batch["description"] + " " + batch["tags"].apply(" ".join)).tolist()

            product_ids = batch["id"].tolist()
            embeddings = self.model.encode(texts, convert_to_numpy=True)

            self._store_embeddings_batch(product_ids, embeddings)

        print("Embeddings loaded successfully.")

    def _store_embeddings_batch(self, product_ids: list[str], embeddings: np.ndarray):
        """Store a batch of embeddings in pgvector."""

        with self.db.get_cursor() as cursor:
            data = [(pid, emb.tolist()) for pid, emb in zip(product_ids, embeddings, strict=False)]

            execute_values(
                cursor,
                """
                INSERT INTO product_embeddings (product_id, embedding)
                VALUES %s
                ON CONFLICT (product_id) DO UPDATE
                SET embedding = EXCLUDED.embedding;
                """,
                data,
                template="(%s, %s)",
                page_size=100,
            )


if __name__ == "__main__":
    print("Starting vector embedding loading process...")

    loader = VectorLoader()
    loader.load_embeddings()
    print("Vector embedding loading complete.")
