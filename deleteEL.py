from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from elasticsearch import AsyncElasticsearch

app = FastAPI()

# Initialize the Elasticsearch client
es = AsyncElasticsearch(hosts=["http://localhost:9200"])

# Pydantic model for the DELETE request body
class DeleteRequest(BaseModel):
    id: str  # Document ID to be deleted

# DELETE API to delete a document by ID
@app.delete("/delete", status_code=status.HTTP_204_NO_CONTENT)
async def delete_document(request: DeleteRequest):
    try:
        # Attempt to delete the document from Elasticsearch
        response = await es.delete(index="test", id=request.id)
        
        if response['result'] == 'deleted':
            return {"message": "Document deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Document not found")

    except Exception as e:
        # Handle exceptions such as document not found
        raise HTTPException(status_code=500, detail="Data already  deleted")

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
