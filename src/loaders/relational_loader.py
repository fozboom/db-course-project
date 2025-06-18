"""Load data into PostgreSQL database."""

from src.db.postgres_client import db
from src.utils.data_parser import DataParser


class RelationalLoader:
    def __init__(self):
        self.db = db
        self.parser = DataParser()

    def load_categories(self):
        """Load categories into PostgreSQL."""
        categories = self.parser.parse_categories()

        with self.db.get_cursor() as cursor:
            for _, row in categories.iterrows():
                query = """
                        INSERT INTO categories (id, name, description)
                        VALUES (%(id)s, %(name)s, %(description)s) ON CONFLICT (id) DO NOTHING; \
                        """
                cursor.execute(query, row.to_dict())

        print(f"Loaded {len(categories)} categories")

    def load_sellers(self):
        """Load sellers into PostgreSQL."""
        sellers = self.parser.parse_sellers()
        with self.db.get_cursor() as cursor:
            for _, row in sellers.iterrows():
                query = """
                    INSERT INTO sellers (id, name, rating, joined)
                    VALUES (%(id)s, %(name)s, %(rating)s, %(joined)s) ON CONFLICT (id) DO NOTHING;
                """
                cursor.execute(query, row.to_dict())
        print(f"Loaded {len(sellers)} sellers")

    def load_users(self):
        """Load users into PostgreSQL."""
        users = self.parser.parse_users()
        with self.db.get_cursor() as cursor:
            for _, row in users.iterrows():
                query = """
                    INSERT INTO users (id, name, email, join_date, interests)
                    VALUES (%(id)s, %(name)s, %(email)s, %(join_date)s, %(interests)s) ON CONFLICT (id) DO NOTHING;
                """
                cursor.execute(query, row.to_dict())
        print(f"Loaded {len(users)} users")

    def load_products(self):
        """Load products into PostgreSQL."""
        products = self.parser.parse_products()
        categories = self.parser.parse_categories()

        # Create a mapping from category name to category ID
        category_map = categories.set_index("name")["id"].to_dict()
        # Replace category names with IDs in the products dataframe
        products["category_id"] = products["category_id"].replace(category_map)

        with self.db.get_cursor() as cursor:
            for _, row in products.iterrows():
                query = """
                    INSERT INTO products (id, name, description, price, category_id, seller_id, tags, stock)
                    VALUES (%(id)s, %(name)s, %(description)s, %(price)s, %(category_id)s, %(seller_id)s, %(tags)s, %(stock)s)
                    ON CONFLICT (id) DO NOTHING;
                """
                cursor.execute(query, row.to_dict())
        print(f"Loaded {len(products)} products")

    def load_all(self):
        """Load all data into PostgreSQL."""
        print("Creating tables...")
        self.db.create_tables()

        print("Loading categories...")
        self.load_categories()

        print("Loading sellers...")
        self.load_sellers()

        print("Loading users...")
        self.load_users()

        print("Loading products...")
        self.load_products()

        print("Relational data loading complete!")


if __name__ == "__main__":
    loader = RelationalLoader()
    loader.load_all()
