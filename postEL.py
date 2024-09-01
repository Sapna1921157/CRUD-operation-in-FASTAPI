from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch
from typing import List

app = FastAPI()

# Elasticsearch client setup
es = AsyncElasticsearch(
    hosts=["http://localhost:9200"]  # Specify your Elasticsearch server URL here
)

# Pydantic models
class UserCreate(BaseModel):
    name: str
    email: str

class UserResponse(BaseModel):
    id: str
    name: str
    email: str

# Helper function to convert Elasticsearch hit to UserResponse
def user_helper(hit) -> UserResponse:
    return UserResponse(
        id=hit["_id"],
        name=hit["_source"].get("name", ""),
        email=hit["_source"].get("email", "")
    )

# Function to retrieve all documents from Elasticsearch
async def get_all_documents_from_es():
    try:
        search_body = {
            "query": {
                "match_all": {}
            }
        }
        response = await es.search(index="test", body=search_body)
        documents = [user_helper(hit) for hit in response["hits"]["hits"]]
        return documents
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying Elasticsearch: {str(e)}")



# POST endpoint to create a new document
@app.post("/search", response_model=UserResponse, status_code=201, responses={
    201: {
        "description": "Document created successfully",
        "model": UserResponse
    },
    400: {
        "description": "Invalid request",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Validation error"
                }
            }
        }
    },
    500: {
        "description": "Internal server error",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Error indexing document"
                }
            }
        }
    }
})
async def create_document(user: UserCreate):
    try:
        # Indexing the document in Elasticsearch
        response = await es.index(
            index="test",  # Index name
            document=user.dict()  # Document body
        )
        # Return the created document with its ID
        return UserResponse(
            id=response["_id"],
            name=user.name,
            email=user.email
        )
    except ValueError as ve:
        # Handle validation errors
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # Handle other errors
        raise HTTPException(status_code=500, detail=f"Error indexing document: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
