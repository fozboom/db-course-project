"""Service for managing shopping carts."""

from src.db.redis_client import redis_client


class CartService:
    """Encapsulates business logic for shopping cart management."""

    def add_to_cart(self, user_id: str, product_id: str, quantity: int = 1):
        """Add a product to a user's cart."""
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError("Quantity must be a positive integer.")
        redis_client.add_to_cart(user_id, product_id, quantity)
        print(f"Added {quantity} of product {product_id} to cart for user {user_id}.")

    def remove_from_cart(self, user_id: str, product_id: str):
        """Remove a product from a user's cart."""
        redis_client.remove_from_cart(user_id, product_id)
        print(f"Removed product {product_id} from cart for user {user_id}.")

    def get_cart(self, user_id: str) -> dict[str, int]:
        """Get a user's shopping cart."""
        cart = redis_client.get_cart(user_id)
        print(f"Cart for user {user_id}: {cart}")
        return cart

    def update_item_quantity(self, user_id: str, product_id: str, quantity: int):
        """Update the quantity of a product in the cart."""
        if not isinstance(quantity, int) or quantity <= 0:
            self.remove_from_cart(user_id, product_id)
        else:
            redis_client.update_cart_item_quantity(user_id, product_id, quantity)
            print(f"Updated product {product_id} quantity to {quantity} for user {user_id}.")

    def clear_cart(self, user_id: str):
        """Clear a user's entire shopping cart."""
        redis_client.clear_cart(user_id)
        print(f"Cleared cart for user {user_id}.")


# Singleton instance
cart_service = CartService()
