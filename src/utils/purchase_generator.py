"""Generate random purchase history."""

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm

from src.db.neo4j_client import neo4j_client
from src.db.postgres_client import db
from src.utils.data_parser import DataParser


class PurchaseGenerator:
    def __init__(self):
        self.parser = DataParser()
        self.users = self.parser.parse_users()
        self.products = self.parser.parse_products()

        # Pre-process for faster lookups
        self.products["tags_set"] = self.products["tags"].apply(set)

    def generate_purchases(self, num_purchases: int = 100) -> pd.DataFrame:
        """Generate random purchases based on user interests."""
        purchases = []
        users = self.users
        products = self.products

        for _ in range(num_purchases):
            user = users.sample(1).iloc[0]
            user_interests_set = set(user["interests"])

            # Find products matching user interests
            relevant_products = products[[not x.isdisjoint(user_interests_set) for x in products["tags_set"]]]

            if relevant_products.empty:
                relevant_products = products  # Fallback to any product

            num_items = random.randint(1, 3)
            order_products = relevant_products.sample(n=num_items, replace=True)

            join_date = user["join_date"]
            now = datetime.now()

            # Ensure order date is after join date
            order_date = join_date + timedelta(seconds=random.randint(0, int((now - join_date).total_seconds())))

            order_id = str(uuid.uuid4())
            status = random.choice(["shipped", "delivered", "pending", "cancelled"])

            for _, product in order_products.iterrows():
                purchases.append(
                    {
                        "order_id": order_id,
                        "user_id": user["id"],
                        "product_id": product["id"],
                        "quantity": random.randint(1, 3),
                        "price_at_purchase": product["price"],
                        "order_date": order_date,
                        "status": status,
                    }
                )

        purchases_df = pd.DataFrame(purchases)

        # Calculate total price per order and merge it back
        order_totals = purchases_df.groupby("order_id")[["price_at_purchase", "quantity"]].apply(
            lambda r: (r["price_at_purchase"] * r["quantity"]).sum()
        )
        order_totals.name = "total_price"
        purchases_df = purchases_df.merge(order_totals, on="order_id")

        return purchases_df

    def load_into_postgres(self, purchases_df: pd.DataFrame):
        """Load purchases into PostgreSQL."""
        if purchases_df.empty:
            print("No purchases to load into PostgreSQL.")
            return

        orders_df = purchases_df[["order_id", "user_id", "order_date", "status", "total_price"]]
        assert isinstance(orders_df, pd.DataFrame)
        orders = orders_df.drop_duplicates(subset="order_id")

        order_items = purchases_df[["order_id", "product_id", "quantity", "price_at_purchase"]]

        with db.get_cursor() as cursor:
            # Load Orders
            for _, order in orders.iterrows():
                cursor.execute(
                    """
                    INSERT INTO orders (id, user_id, order_date, status, total_price)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
                    """,
                    (
                        order["order_id"],
                        order["user_id"],
                        order["order_date"],
                        order["status"],
                        order["total_price"],
                    ),
                )
            # Load Order Items
            for _, item in order_items.iterrows():
                cursor.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase)
                    VALUES (%s, %s, %s, %s) ON CONFLICT (order_id, product_id) DO NOTHING;
                    """,
                    (
                        item["order_id"],
                        item["product_id"],
                        item["quantity"],
                        item["price_at_purchase"],
                    ),
                )
        print(f"Loaded {len(orders)} orders and {len(order_items)} order items into PostgreSQL.")

    def load_into_neo4j(self, purchases_df: pd.DataFrame):
        """Load purchases into Neo4j."""
        if purchases_df.empty:
            print("No purchases to load into Neo4j.")
            return

        # Use a minimal DataFrame for Neo4j to avoid type issues
        neo4j_df = purchases_df[["user_id", "product_id", "quantity", "order_date"]]
        assert isinstance(neo4j_df, pd.DataFrame)
        neo4j_data = neo4j_df.to_dict("records")

        for purchase in tqdm(neo4j_data, desc="Loading Purchases into Neo4j"):
            neo4j_client.add_purchase(
                user_id=str(purchase["user_id"]),
                product_id=str(purchase["product_id"]),
                quantity=int(purchase["quantity"]),
                date=purchase["order_date"],
            )
        print(f"Loaded {len(neo4j_data)} purchase relationships into Neo4j.")


if __name__ == "__main__":
    generator = PurchaseGenerator()
    print("Generating purchase data...")
    purchases = generator.generate_purchases()
    print(f"Generated {len(purchases)} purchase line items.")

    print("Loading purchases into databases...")
    generator.load_into_postgres(purchases)
    generator.load_into_neo4j(purchases)
    print("Purchase generation and loading complete!")
