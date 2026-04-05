"""
materialize-llm/main.py
A08 update: adds city, state, zip_code columns to the materialized output.
Deployed as a GCP Cloud Function (gen2) in project myscrapers-ohk24001.

Reads LLM-enriched JSONL from GCS, validates + casts fields,
writes a clean JSONL artifact back to GCS for downstream modeling.
"""

import os
import re
import json
import datetime
import functions_framework
from google.cloud import storage

# ── Config ─────────────────────────────────────────────────────────────────
PROJECT_ID  = os.environ.get("GCP_PROJECT", "myscrapers-ohk24001")
BUCKET_NAME = os.environ.get("GCS_BUCKET",  "myscrapers-ohk24001")

# ── Field definitions ───────────────────────────────────────────────────────
# Each entry: (output_field_name, source_field(s), cast_fn, default)

VALID_BODY_TYPES = {
    "sedan", "suv", "truck", "coupe", "hatchback",
    "van", "wagon", "convertible", "minivan", "pickup", "other", "unknown",
}
VALID_TITLE_STATUSES = {
    "clean", "salvage", "rebuilt", "lien", "missing", "parts only", "unknown",
}
VALID_CONDITIONS = {
    "new", "like new", "excellent", "good", "fair", "salvage", "unknown",
}
VALID_TRANSMISSIONS = {"automatic", "manual", "other", "unknown"}
VALID_FUELS = {"gas", "diesel", "electric", "hybrid", "other", "unknown"}

# US state abbreviations (for validation)
VALID_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC","unknown",
}


# ── Helper casts ────────────────────────────────────────────────────────────

def cast_price(val) -> float | None:
    """Strip non-numeric chars and return float, or None."""
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        v = float(cleaned)
        return v if 100 <= v <= 500_000 else None
    except (ValueError, TypeError):
        return None


def cast_mileage(val) -> float | None:
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        v = float(cleaned)
        return v if 0 <= v <= 1_000_000 else None
    except (ValueError, TypeError):
        return None


def cast_year(val) -> int | None:
    try:
        v = int(str(val).strip()[:4])
        return v if 1900 <= v <= datetime.datetime.now().year + 1 else None
    except (ValueError, TypeError):
        return None


def cast_enum(val, valid_set: set, default="unknown") -> str:
    s = str(val).strip().lower()
    return s if s in valid_set else default


def cast_color(val) -> str:
    """Normalize color to lowercase, strip adjectives like 'pearl', 'metallic'."""
    if not val or str(val).lower() in ("unknown", "none", ""):
        return "unknown"
    # Keep only the last word (e.g. "pearl white" → "white")
    parts = str(val).strip().lower().split()
    return parts[-1] if parts else "unknown"


def cast_city(val) -> str:
    if not val or str(val).lower() in ("unknown", "none", ""):
        return "unknown"
    return str(val).strip().title()


def cast_state(val) -> str:
    s = str(val).strip().upper()
    return s if s in VALID_STATES else "unknown"


def cast_zip(val) -> str:
    cleaned = re.sub(r"\D", "", str(val))
    return cleaned[:5] if len(cleaned) >= 5 else "unknown"


def cast_transmission(val) -> str:
    s = str(val).strip().lower()
    if s in ("auto", "automatic"):
        return "automatic"
    if s in ("manual", "stick", "5-speed", "6-speed"):
        return "manual"
    return cast_enum(s, VALID_TRANSMISSIONS)


def materialize_record(raw: dict) -> dict:
    """
    Validate and cast one raw LLM-enriched record into a clean output record.
    All A07 fields + A08 additions (city, state, zip_code) are included.
    """
    return {
        # ── Identity ────────────────────────────────────────────────────────
        "posting_id":   str(raw.get("posting_id", "")),
        "url":          str(raw.get("url", "")),
        "scrape_date":  str(raw.get("scrape_date", "")),

        # ── Regex-extracted numerics ────────────────────────────────────────
        "price":        cast_price(raw.get("price")),
        "mileage":      cast_mileage(raw.get("mileage")),
        "year":         cast_year(raw.get("year")),

        # ── Regex-extracted categoricals ────────────────────────────────────
        "transmission": cast_transmission(raw.get("transmission")),
        "fuel":         cast_enum(raw.get("fuel", ""), VALID_FUELS),

        # ── A07 LLM fields ───────────────────────────────────────────────────
        "body_type":    cast_enum(raw.get("body_type", ""),   VALID_BODY_TYPES),
        "color":        cast_color(raw.get("color", "")),
        "title_status": cast_enum(raw.get("title_status", ""), VALID_TITLE_STATUSES),
        "condition":    cast_enum(raw.get("condition", ""),   VALID_CONDITIONS),

        # ── A08 NEW LLM fields ───────────────────────────────────────────────
        "city":         cast_city(raw.get("city", "")),
        "state":        cast_state(raw.get("state", "")),
        "zip_code":     cast_zip(raw.get("zip_code", "")),
    }


@functions_framework.http
def materialize_llm(request):
    """
    HTTP Cloud Function entry point.

    Expects JSON body:
        {
          "input_blob":  "data/llm/YYYY-MM-DD.jsonl",
          "output_blob": "data/materialized/YYYY-MM-DD.jsonl"
        }

    Reads LLM-enriched JSONL, materializes each record,
    drops rows missing price, and writes clean output JSONL.
    """
    request_json = request.get_json(silent=True) or {}

    input_blob_name  = request_json.get("input_blob")
    output_blob_name = request_json.get("output_blob")

    if not input_blob_name or not output_blob_name:
        return ("Missing 'input_blob' or 'output_blob' in request body.", 400)

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    # ── Read LLM-enriched JSONL ─────────────────────────────────────────────
    in_blob = bucket.blob(input_blob_name)
    try:
        raw_data = in_blob.download_as_text()
    except Exception as e:
        return (f"Could not read {input_blob_name}: {e}", 500)

    raw_records = []
    for line in raw_data.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw_records.append(json.loads(line))
        except json.JSONDecodeError:
            pass

    print(f"[materialize-llm] Read {len(raw_records)} raw records.")

    # ── Materialize ─────────────────────────────────────────────────────────
    output_lines = []
    skipped = 0
    for raw in raw_records:
        rec = materialize_record(raw)
        if rec["price"] is None:
            skipped += 1
            continue
        output_lines.append(json.dumps(rec))

    # ── Write output ────────────────────────────────────────────────────────
    out_blob = bucket.blob(output_blob_name)
    out_blob.upload_from_string(
        "\n".join(output_lines) + "\n",
        content_type="application/jsonl",
    )

    msg = (
        f"Materialized {len(output_lines)} records "
        f"({skipped} skipped — missing price). "
        f"Output: gs://{BUCKET_NAME}/{output_blob_name}"
    )
    print(f"[materialize-llm] {msg}")
    return (msg, 200)
