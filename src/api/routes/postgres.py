from fastapi import APIRouter, HTTPException

from src.db.postgres_client import db as pg_db

router = APIRouter(
    tags=["PostgreSQL"],
)


@router.get("/users/{user_id}")
def get_user(user_id: str):
    """
    Fetches a user by their ID from PostgreSQL.
    """
    with pg_db.get_cursor() as cursor:
        cursor.execute("SELECT id, name, email, join_date FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@router.get("/categories")
def get_all_categories():
    """Fetches all product categories from PostgreSQL."""
    with pg_db.get_cursor() as cursor:
        cursor.execute("SELECT id, name, description FROM categories ORDER BY name")
        categories = cursor.fetchall()
    return categories


@router.get("/sellers")
def get_all_sellers():
    """Fetches all sellers from PostgreSQL."""
    with pg_db.get_cursor() as cursor:
        cursor.execute("SELECT id, name, rating, joined FROM sellers ORDER BY name")
        sellers = cursor.fetchall()
    return sellers


@router.get("/users/{user_id}/orders")
def get_user_orders(user_id: str):
    """Fetches all orders and their items for a specific user from PostgreSQL."""
    with pg_db.get_cursor() as cursor:
        query = """
        SELECT o.id, o.order_date, o.status, o.total_price,
               json_agg(json_build_object(
                   'product_id', oi.product_id,
                   'quantity', oi.quantity,
                   'price', oi.price_at_purchase
               )) as items
        FROM orders o
        JOIN order_items oi ON o.id = oi.order_id
        WHERE o.user_id = %s
        GROUP BY o.id
        ORDER BY o.order_date DESC;
        """
        cursor.execute(query, (user_id,))
        orders = cursor.fetchall()
    if not orders:
        return {"message": "No orders found for this user."}
    return orders
