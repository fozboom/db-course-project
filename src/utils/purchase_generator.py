"""Generate random purchase history."""

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd

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

        order_totals = (
            purchases_df.groupby("order_id")
            .apply(lambda x: (x["price_at_purchase"] * x["quantity"]).sum())
            .rename("total_price")
        )
        purchases_df = purchases_df.merge(order_totals, on="order_id")

        return purchases_df

    def load_into_postgres(self, purchases_df: pd.DataFrame):
        """Load purchases into PostgreSQL."""
        if purchases_df.empty:
            print("No purchases to load into PostgreSQL.")
            return

        orders = purchases_df[["order_id", "user_id", "order_date", "status", "total_price"]].drop_duplicates(
            "order_id"
        )

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
        print("Neo4j loading is not implemented yet.")
        pass  # TODO: Implement Neo4j loading


if __name__ == "__main__":
    generator = PurchaseGenerator()
    print("Generating purchase data...")
    purchases = generator.generate_purchases()
    print(f"Generated {len(purchases)} purchase line items.")

    print("Loading purchases into databases...")
    generator.load_into_postgres(purchases)
    generator.load_into_neo4j(purchases)
    print("Purchase generation and loading complete!")
