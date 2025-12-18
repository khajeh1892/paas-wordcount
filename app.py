import os
import uuid
import re
from collections import Counter

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import mysql.connector
from dotenv import load_dotenv

# Load env vars (for local dev; on PaaS they come from panel)
load_dotenv()

app = FastAPI(title="PaaS MapReduce WordCount")

# ---------- Database ----------

def get_db():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
    )

def init_schema():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS map_results (
        job_id VARCHAR(36) NOT NULL,
        k VARCHAR(255) NOT NULL,
        v INT NOT NULL,
        INDEX(job_id),
        INDEX(k)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reduce_results (
        job_id VARCHAR(36) NOT NULL,
        k VARCHAR(255) NOT NULL,
        total INT NOT NULL,
        PRIMARY KEY (job_id, k)
    );
    """)

    conn.commit()
    cur.close()
    conn.close()

# ---------- Utils ----------

def tokenize(text: str):
    return re.findall(r"\w+", text.lower())

def chunk_list(items, size=800):
    for i in range(0, len(items), size):
        yield items[i:i + size]

# ---------- API Models ----------

class RunReq(BaseModel):
    text: str

# ---------- API Endpoints ----------

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run")
def run_job(req: RunReq):
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="text is required")

    init_schema()
    job_id = str(uuid.uuid4())

    words = tokenize(req.text)

    conn = get_db()
    cur = conn.cursor()

    # MAP phase
    for chunk in chunk_list(words):
        counts = Counter(chunk)
        rows = [(job_id, k, int(v)) for k, v in counts.items()]
        cur.executemany(
            "INSERT INTO map_results (job_id, k, v) VALUES (%s, %s, %s)",
            rows
        )

    conn.commit()

    # REDUCE phase
    cur.execute("""
        INSERT INTO reduce_results (job_id, k, total)
        SELECT job_id, k, SUM(v)
        FROM map_results
        WHERE job_id = %s
        GROUP BY job_id, k
    """, (job_id,))

    conn.commit()
    cur.close()
    conn.close()

    return {
        "job_id": job_id,
        "total_words": len(words)
    }

@app.get("/result/{job_id}")
def get_result(job_id: str, top: int = 10):
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT k, total
        FROM reduce_results
        WHERE job_id = %s
        ORDER BY total DESC
        LIMIT %s
    """, (job_id, top))

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return {
        "job_id": job_id,
        "top_words": [
            {"word": k, "count": int(v)} for k, v in rows
        ]
    }
