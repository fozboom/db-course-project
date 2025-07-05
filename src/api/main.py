from fastapi import FastAPI

from src.api.routes import mongodb, neo4j, pgvector, postgres, redis

app = FastAPI(
    title="ArtisanMarket API",
    description="Demonstrating a polyglot persistence architecture.",
    version="1.0.0",
)

app.include_router(postgres.router)
app.include_router(mongodb.router)
app.include_router(redis.router)
app.include_router(neo4j.router)
app.include_router(pgvector.router)
