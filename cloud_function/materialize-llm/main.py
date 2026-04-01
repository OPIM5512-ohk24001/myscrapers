import functions_framework
import json
import csv
import io
import os
from google.cloud import storage

PROJECT_ID = os.environ.get("PROJECT_ID", "myscrapers-ohk24001")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "myscrapers-ohk24001")
OUTPUT_BLOB = "llm_listings.csv"

FIELDS = [
    "post_id", "run_id", "scraped_at", "source_txt",
    "price", "year", "make", "model", "mileage",
    "body_type", "color", "title_status", "condition", "location",
    "llm_provider", "llm_model", "llm_ts"
]

@functions_framework.http
def materialize_llm(request):
    request_json = request.get_json(silent=True) or {}
    overwrite = request_json.get("overwrite", True)
    run_id = request_json.get("run_id", "20260401100008")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    prefix = f"structured/run_id={run_id}/jsonl_llm/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    llm_blobs = [b for b in blobs if b.name.endswith(".jsonl")]

    if not llm_blobs:
        return {"ok": False, "error": f"No LLM JSONL files found in {prefix}"}, 404

    rows = []
    errors = 0
    for blob in llm_blobs:
        try:
            data = json.loads(blob.download_as_text())
            rows.append(data)
        except Exception as e:
            errors += 1

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=FIELDS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    out_blob = bucket.blob(OUTPUT_BLOB)
    out_blob.upload_from_string(output.getvalue(), content_type="text/csv")

    return {
        "ok": True,
        "written": len(rows),
        "errors": errors,
        "output": OUTPUT_BLOB,
        "run_id": run_id,
        "version": "materialize-llm"
    }
