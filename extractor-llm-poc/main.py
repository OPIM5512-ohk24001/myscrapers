"""
extractor-llm-poc/main.py
A08 update: adds city, state, zip_code to LLM extraction schema.
Deployed as a GCP Cloud Function (gen2) in project myscrapers-ohk24001.
"""

import os
import json
import functions_framework
from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

# ── Config ─────────────────────────────────────────────────────────────────
PROJECT_ID  = os.environ.get("GCP_PROJECT",  "myscrapers-ohk24001")
REGION      = os.environ.get("GCP_REGION",   "us-central1")
BUCKET_NAME = os.environ.get("GCS_BUCKET",   "myscrapers-ohk24001")
MODEL_ID    = "gemini-1.5-flash-001"

# ── Vertex AI init ──────────────────────────────────────────────────────────
vertexai.init(project=PROJECT_ID, location=REGION)
model = GenerativeModel(MODEL_ID)

# ── A08 extraction schema (A07 fields + city / state / zip_code) ────────────
EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        # ── A07 fields ──────────────────────────────────────────────────────
        "body_type": {
            "type": "string",
            "description": (
                "Vehicle body style. One of: sedan, suv, truck, coupe, "
                "hatchback, van, wagon, convertible, minivan, pickup, other, unknown"
            ),
        },
        "color": {
            "type": "string",
            "description": (
                "Exterior color of the vehicle, normalized to a simple color name "
                "(e.g. 'white', 'black', 'silver', 'red', 'blue', 'gray'). "
                "Return 'unknown' if not mentioned."
            ),
        },
        "title_status": {
            "type": "string",
            "description": (
                "Title status of the vehicle. One of: clean, salvage, rebuilt, "
                "lien, missing, parts only, unknown"
            ),
        },
        "condition": {
            "type": "string",
            "description": (
                "Overall condition. One of: new, like new, excellent, good, "
                "fair, salvage, unknown"
            ),
        },
        # ── A08 NEW fields ───────────────────────────────────────────────────
        "city": {
            "type": "string",
            "description": (
                "City where the vehicle is located, extracted from the listing "
                "text or location field. Title-case the city name (e.g. 'Hartford'). "
                "Return 'unknown' if not found."
            ),
        },
        "state": {
            "type": "string",
            "description": (
                "US state abbreviation (2 letters, uppercase) where the vehicle "
                "is located, e.g. 'CT', 'NY', 'CA'. "
                "Return 'unknown' if not found."
            ),
        },
        "zip_code": {
            "type": "string",
            "description": (
                "5-digit US zip code of the listing location. "
                "Return 'unknown' if not found."
            ),
        },
    },
    "required": [
        "body_type", "color", "title_status", "condition",
        "city", "state", "zip_code",
    ],
}

GENERATION_CONFIG = GenerationConfig(
    temperature=0.0,
    response_mime_type="application/json",
    response_schema=EXTRACTION_SCHEMA,
)

SYSTEM_PROMPT = """You are a structured data extractor for used car listings.
Extract the requested fields from the listing text provided.
Be concise and consistent. Return only the JSON object — no explanation."""


def extract_fields_llm(listing_text: str) -> dict:
    """Call Gemini to extract structured fields from one listing."""
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"Listing text:\n{listing_text[:2000]}"  # truncate to keep costs low
    )
    try:
        response = model.generate_content(
            prompt,
            generation_config=GENERATION_CONFIG,
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"[LLM ERROR] {e}")
        return {
            "body_type":    "unknown",
            "color":        "unknown",
            "title_status": "unknown",
            "condition":    "unknown",
            "city":         "unknown",
            "state":        "unknown",
            "zip_code":     "unknown",
        }


@functions_framework.http
def extractor_llm_poc(request):
    """
    HTTP Cloud Function entry point.

    Expects JSON body:
        {
          "input_blob":  "data/raw/YYYY-MM-DD.jsonl",   # source JSONL in GCS
          "output_blob": "data/llm/YYYY-MM-DD.jsonl"    # destination in GCS
        }

    Reads each line from input_blob, runs LLM extraction,
    merges results, and writes to output_blob.
    """
    request_json = request.get_json(silent=True) or {}

    input_blob_name  = request_json.get("input_blob")
    output_blob_name = request_json.get("output_blob")

    if not input_blob_name or not output_blob_name:
        return ("Missing 'input_blob' or 'output_blob' in request body.", 400)

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    # ── Read raw listings ───────────────────────────────────────────────────
    raw_blob = bucket.blob(input_blob_name)
    try:
        raw_data = raw_blob.download_as_text()
    except Exception as e:
        return (f"Could not read {input_blob_name}: {e}", 500)

    records = []
    for line in raw_data.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pass

    print(f"[extractor-llm-poc] Processing {len(records)} listings...")

    # ── Extract + merge ─────────────────────────────────────────────────────
    output_lines = []
    for rec in records:
        listing_text = " | ".join(filter(None, [
            rec.get("title", ""),
            rec.get("description", ""),
            rec.get("location", ""),
        ]))
        llm_fields = extract_fields_llm(listing_text)
        merged = {**rec, **llm_fields}
        output_lines.append(json.dumps(merged))

    # ── Write output JSONL ──────────────────────────────────────────────────
    out_blob = bucket.blob(output_blob_name)
    out_blob.upload_from_string(
        "\n".join(output_lines) + "\n",
        content_type="application/jsonl",
    )

    msg = (
        f"Done. Processed {len(output_lines)} listings. "
        f"Output: gs://{BUCKET_NAME}/{output_blob_name}"
    )
    print(f"[extractor-llm-poc] {msg}")
    return (msg, 200)
