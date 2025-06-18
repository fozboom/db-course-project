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
                # TODO: Implement INSERT query
                query = """
                        INSERT INTO categories (id, name, description)
                        VALUES (%(id)s, %(name)s, %(description)s) ON CONFLICT (id) DO NOTHING; \
                        """
                cursor.execute(query, row.to_dict())

        print(f"Loaded {len(categories)} categories")

    def load_sellers(self):
        """Load sellers into PostgreSQL."""
        # TODO: Implement seller loading
        pass

    def load_users(self):
        """Load users into PostgreSQL."""
        # TODO: Implement user loading
        pass

    def load_products(self):
        """Load products into PostgreSQL."""
        # TODO: Implement product loading
        pass

    def load_all(self):
        """Load all data into PostgreSQL."""
        print("Creating tables...")
        self.db.create_tables()

        print("Loading categories...")
        self.load_categories()

        # TODO: Load remaining data
        print("Relational data loading complete!")


if __name__ == "__main__":
    loader = RelationalLoader()
    loader.load_all()
