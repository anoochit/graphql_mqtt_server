from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import strawberry

# Define your GraphQL schema using Strawberry

@strawberry.type
class Query:
    @strawberry.field
    def hello(self) -> str:
        return "Hello from GraphQL!"

@strawberry.type
class Mutation:
    @strawberry.mutation
    def say_hello(self, name: str) -> str:
        return f"Hello, {name}!"

schema = strawberry.Schema(query=Query, mutation=Mutation)

# Create FastAPI app
app = FastAPI()

# Create GraphQL route
graphql_app = GraphQLRouter(schema)

# Mount GraphQL app at /graphql
app.include_router(graphql_app, prefix="/graphql")

# Define a simple REST endpoint
@app.get("/rest")
async def rest_hello():
    return {"message": "Hello from REST API"}

