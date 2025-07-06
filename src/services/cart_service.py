"""Service for managing shopping carts."""

import uuid
from datetime import datetime

from src.db.neo4j_client import neo4j_client
from src.db.postgres_client import db as postgres_db
from src.db.redis_client import redis_client


class CartService:
    """Encapsulates business logic for shopping cart management."""

    @staticmethod
    def add_to_cart(user_id: str, product_id: str, quantity: int = 1):
        """Add a product to a user's cart."""
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("Quantity must be a positive integer.")
        redis_client.add_to_cart(user_id, product_id, quantity)
        print(f"Added {quantity} of product {product_id} to cart for user {user_id}.")

    @staticmethod
    def remove_from_cart(user_id: str, product_id: str):
        """Remove a product from a user's cart."""
        redis_client.remove_from_cart(user_id, product_id)
        print(f"Removed product {product_id} from cart for user {user_id}.")

    @staticmethod
    def get_cart(user_id: str) -> dict[str, int]:
        """Get a user's shopping cart."""
        cart = redis_client.get_cart(user_id)
        print(f"Cart for user {user_id}: {cart}")
        return cart

    @staticmethod
    def update_item_quantity(user_id: str, product_id: str, quantity: int):
        """Update the quantity of a product in the cart."""
        if not isinstance(quantity, int) or quantity < 0:
            raise ValueError("Quantity must be a non-negative integer.")

        if quantity == 0:
            CartService.remove_from_cart(user_id, product_id)
        else:
            redis_client.update_cart_item_quantity(user_id, product_id, quantity)
            print(f"Updated product {product_id} quantity to {quantity} for user {user_id}.")

    @staticmethod
    def clear_cart(user_id: str):
        """Clear a user's entire shopping cart."""
        redis_client.clear_cart(user_id)
        print(f"Cleared cart for user {user_id}.")

    @staticmethod
    def convert_cart_to_order(user_id: str) -> dict:
        """
        Converts a user's cart into a permanent order.
        1. Reads cart from Redis.
        2. Fetches product prices from PostgreSQL.
        3. Creates 'orders' and 'order_items' records in a single transaction.
        4. Creates 'PURCHASED' relationships in Neo4j.
        5. Clears the cart from Redis.
        """
        cart = redis_client.get_cart(user_id)
        if not cart:
            raise ValueError("Cart is empty. Cannot create an order.")

        product_ids = list(cart.keys())
        order_id = str(uuid.uuid4())
        order_date = datetime.now()
        total_amount = 0.0

        with postgres_db.get_cursor() as cursor:
            # Fetch product details and calculate total
            cursor.execute(
                "SELECT id, price FROM products WHERE id = ANY(%s)",
                (product_ids,),
            )
            product_prices = {row["id"]: float(row["price"]) for row in cursor.fetchall()}

            if len(product_prices) != len(product_ids):
                raise ValueError("One or more products in the cart could not be found.")

            # Calculate total order amount
            order_items_to_insert = []
            for product_id, quantity in cart.items():
                price = product_prices[product_id]
                total_amount += price * quantity
                order_items_to_insert.append((order_id, product_id, quantity, price))

            # Create the main order record
            cursor.execute(
                "INSERT INTO orders (id, user_id, order_date, total_price, status) VALUES (%s, %s, %s, %s, %s)",
                (order_id, user_id, order_date, total_amount, "COMPLETED"),
            )

            # Bulk insert all order items
            cursor.executemany(
                "INSERT INTO order_items (order_id, product_id, quantity, price_at_purchase) VALUES (%s, %s, %s, %s)",
                order_items_to_insert,
            )

        # After successful DB transaction, update other systems
        for product_id, quantity in cart.items():
            neo4j_client.add_purchase(user_id, product_id, quantity, order_date.isoformat())

        redis_client.clear_cart(user_id)

        return {"order_id": order_id, "total_amount": total_amount, "items_count": len(cart)}


# Singleton instance
cart_service = CartService()
