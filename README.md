# ArtisanMarket: A Polyglot Persistence Showcase

ArtisanMarket is a backend project for an e-commerce platform specializing in handmade goods. It demonstrates a polyglot persistence architecture, leveraging multiple database technologies to handle diverse data types and access patterns efficiently. This project showcases how to integrate relational, document, graph, and in-memory databases to build a robust and scalable data layer for a modern web application.

## 🎥 Video Demonstration

A video walkthrough of the project architecture and features is available [here](https://drive.google.com/file/d/1Ec0UKJJt7iiOCauLKjzUgxtCVFSmXAX9/view?usp=sharing)

## ✨ Key Features

-   **Polyglot Persistence Architecture**: Strategically uses PostgreSQL, MongoDB, Neo4j, and Redis to optimize for different data needs.
-   **Core E-commerce Engine**: Manages users, products, and orders with transactional integrity using PostgreSQL.
-   **Advanced Search Capabilities**:
    -   Full-text search for finding products by name and description.
    -   Semantic search powered by `pgvector` to discover visually or functionally similar items.
-   **High-Performance Caching**: Utilizes Redis to cache frequently accessed product data, ensuring low-latency responses.
-   **Scalable Shopping Carts**: Implements session-based shopping carts in Redis for speed and efficiency.
-   **Graph-Based Recommendations**: Generates "users who bought this also bought" and other personalized suggestions using Neo4j to analyze customer behavior.
-   **Flexible Data Models**: Employs MongoDB to store rich, unstructured data like product reviews with nested comments and category-specific product specifications.

## 🛠️ Technology Stack

-   **Backend**: Python 3.12, FastAPI
-   **Databases**:
    -   **PostgreSQL**: Core relational data (users, products, orders).
    -   **pgvector**: Vector embeddings for semantic search.
    -   **MongoDB**: Document storage (reviews, product specifications).
    -   **Neo4j**: Graph database for purchase history and recommendations.
    -   **Redis**: Caching, session management, and rate limiting.
-   **Infrastructure**: Docker & Docker Compose
-   **Package Management**: `uv`

## 🏛️ Database Architecture

This project divides data storage responsibilities based on the strengths of each database system:

-   **PostgreSQL** serves as the primary transactional database for core business entities that require a strict schema and ACID compliance.
-   **MongoDB** stores documents with flexible or evolving schemas, such as user reviews and varied product specifications.
-   **Redis** provides a high-performance in-memory store for caching, user sessions (shopping carts), and other ephemeral data.
-   **Neo4j** models the complex relationships between users and products to power a sophisticated recommendation engine.
-   **pgvector** (an extension for PostgreSQL) stores vector embeddings of product information to enable powerful semantic search.

## 🚀 Getting Started

### Prerequisites
- Python 3.12+
- `uv`
- Docker

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/fozboom/db-course-project.git
    cd db-course-project
    ```

2.  **Set up the environment:**
    ```bash
    uv sync
    ```

3.  **Configure environment variables:**
    ```bash
    # Copy the example .env file
    cp .env.example .env
    # Update .env with your database credentials
    ```

4.  **Launch database services:**
    ```bash
    docker-compose up -d
    ```

### Running the Application

1.  **Load Initial Data**

    Run the following scripts to populate the databases with sample data:

    ```bash
    uv run python -m src.loaders.relational_loader
    uv run python -m src.loaders.document_loader
    uv run python -m src.loaders.graph_loader
    uv run python -m src.loaders.vector_loader
    ```

2.  **Generate Purchase History**

    To simulate user activity, generate a realistic purchase history:

    ```bash
    uv run python -m src.utils.purchase_generator
    ```

3.  **Launch the API**

    Start the backend server:

    ```bash
    uvicorn src.api.main:app --reload
    ```

    You can access the interactive API documentation at [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs).