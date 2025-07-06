"""Neo4j connection and utilities."""

from datetime import datetime
from typing import Any

from neo4j import GraphDatabase

from src.config import NEO4J_CONFIG


class Neo4jClient:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_CONFIG["uri"], auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]))


    def close(self):
        self.driver.close()

    def create_constraints(self):
        """Create uniqueness and existence constraints."""
        with self.driver.session() as session:
            # Uniqueness constraints (also create indexes)
            session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
            session.run("CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE")
            session.run("CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE")

            # Property existence constraints
            session.run("CREATE CONSTRAINT user_name IF NOT EXISTS FOR (u:User) REQUIRE u.name IS NOT NULL")
            session.run("CREATE CONSTRAINT product_name IF NOT EXISTS FOR (p:Product) REQUIRE p.name IS NOT NULL")
            session.run("CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS NOT NULL")
            print("Neo4j constraints created.")

    def add_view(self, user_id: str, product_id: str):
        """Add a VIEWED relationship."""
        with self.driver.session() as session:
            session.run(
                """
                MATCH (u:User {id: $user_id})
                MATCH (p:Product {id: $product_id})
                CREATE (u)-[:VIEWED {timestamp: $timestamp}]->(p)
                """,
                user_id=user_id,
                product_id=product_id,
                timestamp=datetime.now().isoformat(),
            )

    def add_purchase(self, user_id: str, product_id: str, quantity: int, date: Any):
        """Add a purchase relationship."""
        with self.driver.session() as session:
            session.run(
                """
                MATCH (u:User {id: $user_id})
                MATCH (p:Product {id: $product_id})
                CREATE (u)-[:PURCHASED {quantity: $quantity, date: $date}]->(p)
                """,
                user_id=user_id,
                product_id=product_id,
                quantity=quantity,
                date=date,
            )

    def get_recommendations(self, user_id: str, limit: int = 5):
        """Get product recommendations for a user based on collaborative filtering."""
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (target:User {id: $user_id})-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(other:User)
                MATCH (other)-[:PURCHASED]->(rec:Product)
                WHERE NOT (target)-[:PURCHASED]->(rec)
                WITH rec, count(*) as frequency
                ORDER BY frequency DESC
                LIMIT $limit
                RETURN rec.id as product_id, rec.name as product_name
                """,
                user_id=user_id,
                limit=limit,
            )
            return [record.data() for record in result]

    def get_also_bought_products(self, product_id: str, limit: int = 5):
        """Get products that are frequently bought by users who purchased a specific product."""
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (:Product {id: $product_id})<-[:PURCHASED]-(u:User)-[:PURCHASED]->(other:Product)
                WHERE other.id <> $product_id
                WITH other, count(u) as purchase_count
                ORDER BY purchase_count DESC
                LIMIT $limit
                RETURN other.id as product_id, other.name as product_name
                """,
                product_id=product_id,
                limit=limit,
            )
            return [record.data() for record in result]


# Singleton instance
neo4j_client = Neo4jClient()
