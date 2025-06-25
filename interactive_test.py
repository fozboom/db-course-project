"""Interactive script for testing ArtisanMarket services."""

from rich.console import Console
from rich.table import Table

from src.services.cart_service import cart_service
from src.services.search_service import semantic_search_service

console = Console()


def print_products(products: list[dict]):
    """Prints a list of products in a table format."""
    if not products:
        console.print("[yellow]Products not found.[/yellow]")
        return

    table = Table(
        title="Результаты поиска",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("ID", style="dim", width=12)
    table.add_column("Name", max_width=30)
    table.add_column("Category")
    table.add_column("Price", justify="right")
    table.add_column("Similarity", justify="right")

    for product in products:
        similarity = f"{product.get('similarity', 0):.2f}"
        table.add_row(
            product["id"],
            product["name"],
            product.get("category_name", "N/A"),  # Handle missing category
            f"${product['price']:.2f}",
            similarity,
        )
    console.print(table)


def run_tests():
    """Main function to run tests."""

    # --- Test semantic search ---
    console.print(
        "\n[bold cyan]--- 1. Test semantic search ---[/bold cyan]"
    )
    query = "something to decorate the living room"
    console.print(f"[yellow]Searching for:[/yellow] '{query}'")
    products = semantic_search_service.natural_language_search(query, top_k=5)
    print_products(products)

    # --- Test search with filters ---
    console.print(
        "\n[bold cyan]--- 2. Test semantic search with filters ---[/bold cyan]"
    )
    query = "handmade wooden kitchenware"
    console.print(
        f"[yellow]Searching for:[/yellow] '{query}' [yellow]in category[/yellow] 'Home & Kitchen' [yellow]with price up to $50[/yellow]"
    )
    products = semantic_search_service.natural_language_search(
        query, category="Home & Kitchen", max_price=50.0, top_k=5
    )
    print_products(products)

    # --- Test similar products search ---
    product_id_to_test = "P001"  # Assuming product with this ID exists
    console.print(
        f"\n[bold cyan]--- 3. Test similar products search (More like this) for ID: {product_id_to_test} ---[/bold cyan]"
    )
    similar_products = semantic_search_service.find_similar_products(
        product_id=product_id_to_test, top_k=5
    )
    print_products(similar_products)

    # --- Test cart ---
    user_id = "U001" # ID of test user
    console.print(f"\n[bold cyan]--- 4. Test cart for user {user_id} ---[/bold cyan]")

    console.print("\n[yellow]Step 4.1: Clear cart in case there's something left[/yellow]")
    cart_service.clear_cart(user_id)
    cart_service.get_cart(user_id)

    console.print("\n[yellow]Step 4.2: Add products to cart[/yellow]")
    cart_service.add_to_cart(user_id, "P001", 2) # 2 items of P001
    cart_service.add_to_cart(user_id, "P005", 1) # 1 item of P005
    cart_service.get_cart(user_id)

    console.print("\n[yellow]Step 4.3: Update product quantity[/yellow]")
    cart_service.update_item_quantity(user_id, "P001", 3) # Change quantity of P001 to 3
    cart_service.get_cart(user_id)

    console.print("\n[yellow]Step 4.4: Remove product from cart[/yellow]")
    cart_service.remove_from_cart(user_id, "P005")
    cart_service.get_cart(user_id)

    # --- Cache statistics ---
    console.print("\n[bold cyan]--- 5. Cache statistics ---[/bold cyan]")
    console.print("Run first search again to see 'cache hit'...")
    _ = semantic_search_service.natural_language_search(query, top_k=5)
    stats = semantic_search_service.get_cache_stats()
    console.print(f"[green]Current cache statistics:[/green] {stats}")


if __name__ == "__main__":
    run_tests()
