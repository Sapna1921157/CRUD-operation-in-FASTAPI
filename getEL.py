from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch
from typing import List

app = FastAPI()

# Elasticsearch client setup
es = AsyncElasticsearch(
    hosts=["http://localhost:9200"]  # Specify your Elasticsearch server URL here
)

# Pydantic model for user response
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
        # Search query to match all documents
        search_body = {
            "query": {
                "match_all": {}
            }
        }
        response = await es.search(index="test", body=search_body)
        
        # Convert each hit to UserResponse
        documents = [user_helper(hit) for hit in response["hits"]["hits"]]
        return documents
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying Elasticsearch: {str(e)}")

# GET endpoint to retrieve all documents
@app.get("/search", response_model=List[UserResponse])
async def search_documents():
    return await get_all_documents_from_es()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
