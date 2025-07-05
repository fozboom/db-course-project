"""Load graph data into Neo4j."""
from tqdm import tqdm

from src.db.neo4j_client import neo4j_client
from src.db.postgres_client import db as pg_db
from src.utils.data_parser import DataParser


class GraphLoader:
    """Load graph data into Neo4j."""

    def __init__(self):
        """Initialize the graph loader."""
        self.client = neo4j_client
        self.parser = DataParser()

    def load_all(self):
        """Load all graph data into Neo4j."""
        print("Creating constraints in Neo4j...")
        self.create_constraints()

        print("Clearing existing graph data...")
        self.clear_database()

        print("Loading nodes and relationships...")
        self.load_categories()
        self.load_users()
        self.load_products_and_relationships()

        print("Calculating and loading similarity relationships...")
        self.load_similar_product_relationships()

        print("Graph data loading complete!")

    def create_constraints(self):
        """Create uniqueness constraints for nodes."""
        with self.client.driver.session() as session:
            session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
            session.run("CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE")
            session.run("CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE")

    def clear_database(self):
        """Clear all nodes and relationships from the database."""
        with self.client.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def load_categories(self):
        """Load categories into Neo4j."""
        categories = self.parser.parse_categories()
        with self.client.driver.session() as session:
            for _, row in tqdm(categories.iterrows(), total=len(categories), desc="Loading Categories"):
                session.run(
                    "CREATE (c:Category {id: $id, name: $name})",
                    id=row["id"],
                    name=row["name"],
                )
        print(f"Loaded {len(categories)} categories.")

    def load_users(self):
        """Load users into Neo4j."""
        users = self.parser.parse_users()
        with self.client.driver.session() as session:
            for _, row in tqdm(users.iterrows(), total=len(users), desc="Loading Users"):
                session.run(
                    "CREATE (u:User {id: $id, name: $name, join_date: $join_date})",
                    id=row["id"],
                    name=row["name"],
                    join_date=str(row["join_date"]),
                )
        print(f"Loaded {len(users)} users.")

    def load_products_and_relationships(self):
        """Load products and their relationships to categories."""
        products = self.parser.parse_products()
        categories = self.parser.parse_categories()
        category_map = categories.set_index("name")["id"].to_dict()
        products["category_id_mapped"] = products["category_id"].replace(category_map)

        with self.client.driver.session() as session:
            for _, row in tqdm(products.iterrows(), total=len(products), desc="Loading Products"):
                # Create Product node
                session.run(
                    """
                    CREATE (p:Product {id: $id, name: $name, price: $price})
                    """,
                    id=row["id"],
                    name=row["name"],
                    price=row["price"],
                )
                # Create BELONGS_TO relationship
                session.run(
                    """
                    MATCH (p:Product {id: $product_id})
                    MATCH (c:Category {id: $category_id})
                    MERGE (p)-[:BELONGS_TO]->(c)
                    """,
                    product_id=row["id"],
                    category_id=row["category_id_mapped"],
                )
        print(f"Loaded {len(products)} products and their category relationships.")

    def load_similar_product_relationships(self, top_k: int = 5):
        """
        Create SIMILAR_TO relationships between products based on vector similarity.

        This method fetches product embeddings from pgvector, and for each product,
        finds the `top_k` most similar products, then writes the corresponding
        `SIMILAR_TO` relationship into Neo4j with a `score`.
        """
        with pg_db.get_cursor() as cursor:
            # Get all product IDs that have an embedding
            cursor.execute("SELECT product_id FROM product_embeddings")
            product_ids = [row["product_id"] for row in cursor.fetchall()]

            # Use a single Neo4j session for all updates
            with self.client.driver.session() as session:
                for product_id in tqdm(product_ids, desc="Creating SIMILAR_TO relationships"):
                    # Find top_k similar products for the current product
                    cursor.execute(
                        """
                        SELECT B.product_id, 1 - (A.embedding <=> B.embedding) AS similarity
                        FROM product_embeddings A, product_embeddings B
                        WHERE A.product_id = %s AND A.product_id <> B.product_id
                        ORDER BY similarity DESC
                        LIMIT %s;
                        """,
                        (product_id, top_k),
                    )
                    similar_products = cursor.fetchall()

                    # For each similar product, create the relationship in Neo4j
                    for similar in similar_products:
                        session.run(
                            """
                            MATCH (p1:Product {id: $p1_id})
                            MATCH (p2:Product {id: $p2_id})
                            MERGE (p1)-[r:SIMILAR_TO]->(p2)
                            SET r.score = $score
                            """,
                            p1_id=product_id,
                            p2_id=similar["product_id"],
                            score=similar["similarity"],
                        )
        print(f"Created SIMILAR_TO relationships for {len(product_ids)} products.")


if __name__ == "__main__":
    loader = GraphLoader()
    loader.load_all()
