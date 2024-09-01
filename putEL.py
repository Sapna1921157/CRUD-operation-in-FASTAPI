from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch
from typing import Optional

app = FastAPI()

# Elasticsearch client setup
es = AsyncElasticsearch(
    hosts=["http://localhost:9200"]  # Specify your Elasticsearch server URL here
)

# Pydantic models
class UserUpdate(BaseModel):
    id: str
    name: Optional[str] = None
    email: Optional[str] = None

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

# Function to update a document in Elasticsearch
async def update_document_in_es(doc_id: str, update_data: dict):
    try:
        response = await es.update(
            index="test",  # Index name
            id=doc_id,  # Document ID
            body={"doc": update_data}  # Update body
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating document: {str(e)}")

# PUT endpoint to update a document
@app.put("/update", response_model=UserResponse, responses={
    200: {
        "description": "Document updated successfully",
        "model": UserResponse
    },
    400: {
        "description": "Invalid request",
        "content": {
            "application/json": {
                "example": {
                    "detail": "No fields to update or missing document ID"
                }
            }
        }
    },
    404: {
        "description": "Document not found",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Document with the specified ID not found"
                }
            }
        }
    },
    500: {
        "description": "Internal server error",
        "content": {
            "application/json": {
                "example": {
                    "detail": "Error updating document"
                }
            }
        }
    }
})
async def update_document(user_update: UserUpdate):
    if not user_update.name and not user_update.email:
        raise HTTPException(status_code=400, detail="No fields to update")

    doc_id = user_update.id
    update_data = {}
    if user_update.name:
        update_data["name"] = user_update.name
    if user_update.email:
        update_data["email"] = user_update.email

    try:
        response = await update_document_in_es(doc_id, update_data)
        if response["result"] == "updated":
            # Fetch the updated document to return
            updated_doc = await es.get(index="test", id=doc_id)
            return user_helper(updated_doc)
        else:
            raise HTTPException(status_code=404, detail="Document with the specified ID not found")
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating document: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
