from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="PaaS MapReduce WordCount")

class TextInput(BaseModel):
    text: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run")
def run_job(data: TextInput):
    words = data.text.split()
    result = {}
    for w in words:
        result[w] = result.get(w, 0) + 1
    return result
