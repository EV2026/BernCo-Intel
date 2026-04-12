"""
BernCo Intel — GoHighLevel Contact Push
=========================================
Reads dashboard/records.json and pushes each motivated seller lead
as a contact into GoHighLevel (JustJarvis) via the GHL API v2.

- Creates contact if not exists (matched by email placeholder or phone)
- Tags each contact with lead type (Lis Pendens, Lien, Judgment, etc.)
- Adds a note with the full lead details and clerk URL
- Skips contacts already pushed today (dedup by doc number tag)
- Only pushes hot leads (score >= HOT_THRESHOLD)
"""

import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
RECORDS_PATH  = BASE_DIR / "dashboard" / "records.json"

GHL_API_KEY     = os.environ["GHL_API_KEY"]
GHL_LOCATION_ID = os.environ["GHL_LOCATION_ID"]
HOT_THRESHOLD   = int(os.environ.get("HOT_THRESHOLD", "50"))

GHL_BASE    = "https://services.leadconnectorhq.com"
GHL_HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Content-Type":  "application/json",
    "Version":       "2021-07-28",
}

# ── Tag colours by category ───────────────────────────────────────────────
CATEGORY_TAGS = {
    "foreclosure": "Lis Pendens",
    "judgment":    "Judgment Lien",
    "lien":        "Lien",
    "tax":         "Tax Lien",
    "probate":     "Probate",
    "release":     "Release",
    "other":       "Other",
}


def ghl_request(method: str, endpoint: str, payload: dict = None,
                retries: int = 3) -> dict | None:
    """Make a GHL API request with retries."""
    url = f"{GHL_BASE}{endpoint}"
    for attempt in range(retries):
        try:
            resp = requests.request(
                method, url,
                headers=GHL_HEADERS,
                json=payload,
                timeout=20,
            )
            # Rate limit — wait and retry
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"  Rate limited — waiting {wait}s…")
                time.sleep(wait)
                continue
            if resp.status_code in (200, 201):
                return resp.json()
            print(f"  GHL {method} {endpoint} → {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as exc:
            print(f"  Request error (attempt {attempt+1}): {exc}")
            time.sleep(3)
    return None


def parse_name(owner: str) -> tuple[str, str]:
    """Parse LAST FIRST kiosk format → (first, last)."""
    owner = owner.replace(",", " ").strip()
    parts = owner.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0].title()
    last  = parts[0].title()
    first_candidates = parts[1:]
    if len(first_candidates) > 1 and len(first_candidates[-1]) == 1:
        first_candidates = first_candidates[:-1]
    first = first_candidates[0].title() if first_candidates else ""
    return first, last


def is_entity(owner: str) -> bool:
    """Return True if owner looks like an LLC/corp."""
    entity_re = re.compile(
        r"\b(LLC|INC|CORP|LLP|LP|LTD|TRUST|PC|PA|PLLC|NA|BANK|"
        r"CREDIT\s+UNION|FEDERAL|ASSOCIATION|COUNTY|CITY|STATE|"
        r"DEPARTMENT|HOSPITAL|CENTER|FOUNDATION|SERVICES|SOLUTIONS|"
        r"GROUP|PROPERTIES|HOLDINGS|MANAGEMENT|VENTURES|ENTERPRISES|"
        r"INVESTMENTS?)\b",
        re.I
    )
    return bool(entity_re.search(owner))


def build_contact_payload(rec: dict) -> dict:
    """Build the GHL contact creation payload from a lead record."""
    owner  = rec.get("owner", "")
    first, last = parse_name(owner)

    # Build tags
    tags = ["BernCo Intel", "Motivated Seller"]
    cat_tag = CATEGORY_TAGS.get(rec.get("cat", "other"), "Other")
    tags.append(cat_tag)
    if rec.get("doc_type"):
        tags.append(rec["doc_type"])
    tags.extend(rec.get("flags", []))
    if is_entity(owner):
        tags.append("LLC / Corp Owner")

    # Build address
    mail_addr = " ".join(filter(None, [
        rec.get("mail_address", ""),
        rec.get("mail_city", ""),
        rec.get("mail_state", ""),
        rec.get("mail_zip", ""),
    ]))

    # Build property address for the address2 field
    prop_addr = rec.get("prop_address", "")
    if rec.get("prop_city"):
        prop_addr += f", {rec['prop_city']}"
    if rec.get("prop_zip"):
        prop_addr += f" {rec['prop_zip']}"

    payload = {
        "locationId": GHL_LOCATION_ID,
        "firstName":  first,
        "lastName":   last,
        "name":       f"{first} {last}".strip() or owner,
        "address1":   rec.get("mail_address", ""),
        "city":       rec.get("mail_city", ""),
        "state":      rec.get("mail_state", "NM"),
        "postalCode": rec.get("mail_zip", ""),
        "country":    "US",
        "source":     "BernCo Intel",
        "tags":       tags,
    }

    # Only include non-empty fields
    payload = {k: v for k, v in payload.items() if v}
    # tags and locationId must always be present
    payload["locationId"] = GHL_LOCATION_ID
    payload["tags"]       = tags

    return payload


def build_note(rec: dict) -> str:
    """Build a detailed note to attach to the GHL contact."""
    flags = "; ".join(rec.get("flags", [])) or "None"
    prop  = rec.get("prop_address", "N/A")
    if rec.get("prop_city"):
        prop += f", {rec['prop_city']}"
    amount = f"${rec['amount']:,.0f}" if rec.get("amount") else "N/A"

    return (
        f"🏠 BernCo Motivated Seller Lead\n"
        f"{'='*40}\n"
        f"Document #:    {rec.get('doc_num', 'N/A')}\n"
        f"Type:          {rec.get('cat_label', rec.get('doc_type', 'N/A'))}\n"
        f"Date Filed:    {rec.get('filed', 'N/A')}\n"
        f"Score:         {rec.get('score', 0)}/100\n"
        f"Flags:         {flags}\n"
        f"Amount:        {amount}\n"
        f"\nProperty Address:\n  {prop}\n"
        f"\nPublic Record:\n  {rec.get('clerk_url', 'N/A')}\n"
        f"\nSource: Bernalillo County Clerk — Tyler Kiosk\n"
        f"Imported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )


def contact_exists(doc_num: str) -> str | None:
    """
    Check if a contact with this doc number tag already exists.
    Returns contact ID if found, None otherwise.
    """
    result = ghl_request(
        "GET",
        f"/contacts/search/duplicate?locationId={GHL_LOCATION_ID}"
        f"&name={doc_num}",
    )
    # Simple check — search by tag containing the doc number
    search = ghl_request(
        "POST",
        "/contacts/search",
        {
            "locationId": GHL_LOCATION_ID,
            "filters": [{"field": "tags", "operator": "contains",
                         "value": doc_num}],
            "pageLimit": 1,
        }
    )
    if search and search.get("contacts"):
        return search["contacts"][0].get("id")
    return None


def push_contact(rec: dict) -> bool:
    """
    Push one lead to GHL. Returns True on success.
    """
    doc_num = rec.get("doc_num", "")
    owner   = rec.get("owner", "Unknown")

    # Check if already exists
    existing_id = contact_exists(doc_num)
    if existing_id:
        print(f"  ↷ Already in GHL: {owner} ({doc_num})")
        return True

    # Create contact
    payload = build_contact_payload(rec)

    # Add doc number as a tag so we can dedup next time
    if doc_num:
        payload["tags"].append(doc_num)

    # Log the payload for debugging
    import json as _json
    print(f"  → Sending payload: {_json.dumps({k:v for k,v in payload.items() if k != 'tags'})}")

    result = ghl_request("POST", "/contacts/", payload)
    if not result:
        print(f"  ✗ Failed to create contact: {owner}")
        return False

    # Log full response so we can see what GHL returns
    print(f"  ← Response: {str(result)[:300]}")

    contact_id = (result.get("contact") or {}).get("id") or result.get("id")
    if not contact_id:
        print(f"  ✗ No contact ID in response for: {owner} — full response: {result}")
        return False

    # Add note
    note_body = build_note(rec)
    ghl_request("POST", f"/contacts/{contact_id}/notes", {
        "body": note_body,
        "userId": "",
    })

    print(f"  ✓ Created: {owner} ({doc_num}) — score {rec.get('score',0)}")
    time.sleep(0.3)   # gentle rate limiting
    return True


def main() -> None:
    print("=" * 50)
    print("BernCo Intel → GoHighLevel Contact Push")
    print(f"Location: {GHL_LOCATION_ID}")
    print("=" * 50)

    if not RECORDS_PATH.exists():
        print("No records.json found — skipping")
        sys.exit(0)

    data    = json.loads(RECORDS_PATH.read_text(encoding="utf-8"))
    records = data.get("records", [])

    # Filter: hot leads, individuals only (skip LLCs)
    hot = [
        r for r in records
        if r.get("score", 0) >= HOT_THRESHOLD
        and not is_entity(r.get("owner", ""))
        and r.get("owner", "")   # must have an owner name
    ]

    print(f"Total records : {len(records)}")
    print(f"Hot individuals (≥{HOT_THRESHOLD}): {len(hot)}")

    if not hot:
        print("No hot individual leads to push — done")
        return

    pushed = 0
    failed = 0
    for rec in hot:
        try:
            if push_contact(rec):
                pushed += 1
            else:
                failed += 1
        except Exception:
            print(f"  Error: {traceback.format_exc()}")
            failed += 1

    print("=" * 50)
    print(f"Pushed: {pushed}  |  Failed: {failed}  |  Total: {len(hot)}")
    print("Done ✓")


if __name__ == "__main__":
    main()
