from fastapi import FastAPI
from config import API_TITLE
from routes import ingest, jobs

app = FastAPI(title=API_TITLE)

app.include_router(ingest.router)
app.include_router(jobs.router)

@app.get("/health")
def health():
    return {"status": "ok"}