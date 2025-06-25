"""Load document data into MongoDB."""

import random

from faker import Faker
from tqdm import tqdm

from src.db.mongodb_client import mongo_client
from src.utils.data_parser import DataParser


class DocumentLoader:
    def __init__(self):
        self.mongo = mongo_client
        self.parser = DataParser()
        self.faker = Faker()

    def load_all(self):
        """Load all document data into MongoDB."""
        print("Clearing existing document data...")
        self.clear_collections()

        print("Loading user preferences...")
        self.load_user_preferences()

        print("Loading seller profiles...")
        self.load_seller_profiles()

        print("Loading product reviews...")
        self.load_reviews()

        print("Loading product specifications...")
        self.load_product_specs()

        print("Document data loading complete!")

    def clear_collections(self):
        """Clear all relevant collections."""
        collections = ["user_preferences", "seller_profiles", "reviews", "product_specs"]
        for name in collections:
            self.mongo.get_collection(name).delete_many({})

    def load_user_preferences(self):
        """Load user preferences."""
        users = self.parser.parse_users()
        preferences = []
        for _, user in users.iterrows():
            preferences.append(
                {
                    "user_id": user["id"],
                    "email": user["email"],
                    "interests": user["interests"],
                    "theme": random.choice(["light", "dark"]),
                    "language": random.choice(["en", "es", "fr"]),
                }
            )

        if preferences:
            self.mongo.get_collection("user_preferences").insert_many(preferences)
        print(f"Loaded {len(preferences)} user preferences.")

    def load_seller_profiles(self):
        """Generate and load rich seller profiles."""
        sellers = self.parser.parse_sellers()
        profiles = []
        for _, seller in sellers.iterrows():
            profiles.append(
                {
                    "seller_id": seller["id"],
                    "name": seller["name"],
                    "profile_bio": self.faker.paragraph(nb_sentences=3),
                    "portfolio_images": [self.faker.image_url() for _ in range(random.randint(2, 5))],
                    "social_links": {
                        "instagram": f"https://instagram.com/{self.faker.user_name()}",
                        "website": self.faker.url(),
                    },
                }
            )

        if profiles:
            self.mongo.get_collection("seller_profiles").insert_many(profiles)
        print(f"Loaded {len(profiles)} seller profiles.")

    def load_reviews(self, num_reviews_per_product: int = 3):
        """Generate and load product reviews."""
        products = self.parser.parse_products()
        users = self.parser.parse_users()
        reviews = []

        for _, product in tqdm(products.iterrows(), total=len(products)):
            for _ in range(random.randint(1, num_reviews_per_product)):
                user = users.sample(1).iloc[0]
                review_date = self.faker.date_time_between(start_date=user["join_date"])

                has_comments = random.choice([True, False])
                comments = []
                if has_comments:
                    for _ in range(random.randint(1, 2)):
                        commenter = users.sample(1).iloc[0]
                        comments.append(
                            {
                                "user_id": commenter["id"],
                                "username": commenter["name"],
                                "content": self.faker.sentence(),
                                "created_at": self.faker.date_time_between(start_date=review_date),
                            }
                        )

                reviews.append(
                    {
                        "product_id": product["id"],
                        "user_id": user["id"],
                        "username": user["name"],
                        "rating": random.randint(3, 5),
                        "title": self.faker.sentence(nb_words=4),
                        "content": self.faker.paragraph(nb_sentences=2),
                        "images": [self.faker.image_url() for _ in range(random.randint(0, 2))],
                        "helpful_votes": random.randint(0, 50),
                        "verified_purchase": random.choice([True, False]),
                        "created_at": review_date,
                        "comments": comments,
                    }
                )
        if reviews:
            self.mongo.get_collection("reviews").insert_many(reviews)
        print(f"Generated and loaded {len(reviews)} reviews.")

    def load_product_specs(self):
        """Generate and load product specs."""
        products = self.parser.parse_products()
        all_specs = []

        for _, product in products.iterrows():
            category_name = product["category_id"]
            specs = {}
            if not isinstance(category_name, str):
                continue

            if "Apparel" in category_name:
                specs = {
                    "material": random.choice(["Cotton", "Polyester", "Wool", "Silk"]),
                    "sizes_available": random.sample(["S", "M", "L", "XL"], k=random.randint(1, 4)),
                    "care_instructions": ["Machine wash cold", "Tumble dry low"],
                }
            elif "Home & Kitchen" in category_name:
                specs = {
                    "material": random.choice(["Ceramic", "Wood", "Stainless Steel"]),
                    "dimensions": f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)} cm",
                    "weight": f"{random.uniform(0.5, 5.0):.2f} kg",
                }
            elif "Jewelry" in category_name:
                specs = {
                    "material": random.choice(["Gold", "Silver", "Platinum"]),
                    "gemstone": random.choice(["Diamond", "Ruby", "Sapphire", "None"]),
                    "chain_length_inches": random.choice([16, 18, 20, 24]),
                }

            if specs:
                all_specs.append({"product_id": product["id"], "category": category_name, "specs": specs})

        if all_specs:
            self.mongo.get_collection("product_specs").insert_many(all_specs)
        print(f"Generated and loaded {len(all_specs)} product specifications.")


if __name__ == "__main__":
    loader = DocumentLoader()
    loader.load_all()
