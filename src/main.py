import asyncio
import json
import os
import re
import sys
from datetime import datetime
from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, make_asgi_app
import PyPDF2
from uuid import uuid4

app = FastAPI()

# Prometheus metrics
processed_total = Counter("jobs_processed_total", "Total documents processed")
succeeded_total = Counter("jobs_succeeded_total", "Documents successfully processed")
failed_total = Counter("jobs_failed_total", "Documents failed processing")
latency_histogram = Histogram("extraction_latency_ms", "Extraction latency (ms)")

# In-memory job store (replacing Redis)
job_store = {}

# Lock file to prevent multiple runs
LOCK_FILE = ".job_lock"

# Mock LLM for extraction (regex-based for PoC)
def mock_extract(text: str, schema: List[Dict]) -> Dict:
    result = {}
    for field in schema:
        name = field["name"]
        hint = field.get("hint", "")
        if name == "authors":
            matches = re.findall(r"(?:By|Authors?)\s*[:\n](.*?)(?:\n\n|\Z)", text, re.DOTALL)
            result[name] = matches[0].split(", ") if matches else []
        elif name == "publish_date":
            date_match = re.search(r"(?:Date|Submitted)\s*[:\s](.*?)(?:\n|\Z)", text)
            if date_match:
                try:
                    date = datetime.strptime(date_match.group(1), "%B %d, %Y")
                    result[name] = date.isoformat()
                except ValueError:
                    result[name] = None
        elif name == "abstract_summary":
            abstract_match = re.search(r"Abstract\s*[:\n](.*?)(?:\n\n|\Z)", text, re.DOTALL)
            result[name] = abstract_match.group(1)[:500] + "..." if abstract_match else ""
        elif name == "code_snippets":
            result[name] = re.findall(r"```(.*?)```", text, re.DOTALL)
    return result

# Pydantic models
class FieldSchema(BaseModel):
    name: str
    type: str
    hint: Optional[str] = None

class JobRequest(BaseModel):
    source: str
    schema: List[FieldSchema]
    sink: str

class JobStatus(BaseModel):
    job_id: str
    status: str
    processed: int
    succeeded: int
    failed: int

# Job processing
async def process_document(doc_path: str, schema: List[Dict], sink: str, job_id: str):
    try:
        with latency_histogram.time():
            with open(doc_path, "rb") as f:
                pdf = PyPDF2.PdfReader(f)
                text = "".join(page.extract_text() for page in pdf.pages)
            
            fields = mock_extract(text, schema)
            
            for field in schema:
                name = field["name"]
                if field["type"] == "list[str]" and not isinstance(fields.get(name), list):
                    fields[name] = []
                elif field["type"] == "date" and fields.get(name):
                    try:
                        datetime.fromisoformat(fields[name])
                    except ValueError:
                        fields[name] = None
            
            output = {"doc_id": os.path.basename(doc_path), "source_path": doc_path, "fields": fields, "errors": []}
            with open(sink, "a") as f:
                f.write(json.dumps(output) + "\n")
            
            succeeded_total.inc()
            return True
    except Exception as e:
        failed_total.inc()
        with open(sink, "a") as f:
            f.write(json.dumps({"doc_id": os.path.basename(doc_path), "source_path": doc_path, "fields": {}, "errors": [str(e)]}) + "\n")
        return False

async def process_job(job_id: str, source: str, schema: List[Dict], sink: str, max_retries: int = 3):
    # Truncate sink file
    print(f"DEBUG: Truncating sink file {sink}")
    with open(sink, 'w') as f:
        pass
    
    print(f"DEBUG: Starting job {job_id}. Source: {source}, Sink: {sink}")
    files_in_source = [f for f in os.listdir(source) if f.endswith(('.pdf', '.md'))]
    print(f"DEBUG: Files in source: {files_in_source}")
    
    processed = 0
    succeeded = 0
    for doc_path in [os.path.join(source, f) for f in files_in_source]:
        print(f"DEBUG: Processing doc {os.path.basename(doc_path)} (total processed so far: {processed + 1})")
        processed_total.inc()
        processed += 1

        for attempt in range(max_retries):
            try:
                if await process_document(doc_path, schema, sink, job_id):
                    succeeded += 1
                    print(f"DEBUG: Wrote output for {os.path.basename(doc_path)}")
                    break
                else:
                    if attempt == max_retries - 1:
                        failed_total.inc()
            except Exception:
                if attempt == max_retries - 1:
                    failed_total.inc()
                await asyncio.sleep(2 ** attempt)
        
        if processed % 10 == 0:
            await asyncio.sleep(0.1)
    
    job_store[job_id] = {
        "status": "complete",
        "processed": str(processed),
        "succeeded": str(succeeded),
        "failed": str(processed - succeeded)
    }
    print(f"DEBUG: Job complete. Processed: {processed}, Succeeded: {succeeded}")

# Load job from JSON config file
def load_job_config(config_path: str) -> JobRequest:
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return JobRequest(**config)
    except FileNotFoundError:
        print(f"Error: Config file {config_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in {config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        sys.exit(1)

# Check for existing job lock
def acquire_lock():
    if os.path.exists(LOCK_FILE):
        print(f"Error: Lock file {LOCK_FILE} exists. Another job may be running.")
        sys.exit(1)
    with open(LOCK_FILE, 'w') as f:
        f.write(str(os.getpid()))
    return True

def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

# API endpoints
@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    job_data = job_store.get(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatus(
        job_id=job_id,
        status=job_data.get("status", "unknown"),
        processed=int(job_data.get("processed", 0)),
        succeeded=int(job_data.get("succeeded", 0)),
        failed=int(job_data.get("failed", 0))
    )

# Mount Prometheus metrics endpoint
app.mount("/metrics", make_asgi_app())

if __name__ == "__main__":
    import uvicorn
    
    # Check for config file argument
    if len(sys.argv) != 2:
        print("Usage: python run_local.py <config.json>")
        sys.exit(1)
    
    # Load and validate config
    config_path = sys.argv[1]
    job_config = load_job_config(config_path)
    
    # Check for existing job
    acquire_lock()
    try:
        # Start job processing
        job_id = str(uuid4())
        job_store[job_id] = {
            "status": "pending",
            "source": job_config.source,
            "schema": json.dumps([field.dict() for field in job_config.schema]),
            "sink": job_config.sink,
            "processed": "0",
            "succeeded": "0",
            "failed": "0"
        }
        asyncio.run(process_job(job_id, job_config.source, [field.dict() for field in job_config.schema], job_config.sink))
        
        # Run FastAPI server for metrics and status
        print(f"Job {job_id} completed. Access metrics at http://0.0.0.0:8001/metrics")
        uvicorn.run(app, host="0.0.0.0", port=8001, reload=False)  # Disable reload
    finally:
        release_lock()