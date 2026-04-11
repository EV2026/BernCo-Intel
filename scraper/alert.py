"""
BernCo Intel — Morning Lead Alert
===================================
Reads dashboard/records.json and sends a digest email via SendGrid
for all hot leads (score >= 70) filed in the last 24 hours.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sendgrid
from sendgrid.helpers.mail import Mail, To, From, Subject, HtmlContent

# ── Config ────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent.parent
RECORDS_PATH   = BASE_DIR / "dashboard" / "records.json"
API_KEY        = os.environ["SENDGRID_API_KEY"]
ALERT_EMAIL    = os.environ["ALERT_EMAIL"]
FROM_EMAIL     = os.environ.get("FROM_EMAIL", "alerts@envisionventuresllc.com")
HOT_THRESHOLD  = int(os.environ.get("HOT_THRESHOLD", "50"))
DASHBOARD_URL  = "https://ev2026.github.io/BernCo-Intel/"

# ── Category colors ───────────────────────────────────────────────────────
CAT_COLORS = {
    "foreclosure": "#e74c3c",
    "judgment":    "#e67e22",
    "lien":        "#f1c40f",
    "tax":         "#e74c3c",
    "probate":     "#9b59b6",
    "release":     "#95a5a6",
    "other":       "#3498db",
}

FLAG_ICONS = {
    "Lis pendens":     "🏚️",
    "Pre-foreclosure": "🏚️",
    "Judgment lien":   "⚖️",
    "Tax lien":        "🏛️",
    "Mechanic lien":   "🔧",
    "Probate / estate":"⚰️",
    "LLC / corp owner":"🏢",
    "New this week":   "🆕",
}


def load_records() -> dict:
    if not RECORDS_PATH.exists():
        print(f"records.json not found at {RECORDS_PATH}")
        sys.exit(0)
    return json.loads(RECORDS_PATH.read_text(encoding="utf-8"))


def filter_hot(records: list[dict]) -> list[dict]:
    """Return leads scored >= HOT_THRESHOLD."""
    return [r for r in records if r.get("score", 0) >= HOT_THRESHOLD]


def score_bar(score: int) -> str:
    filled = round(score / 10)
    empty  = 10 - filled
    return "█" * filled + "░" * empty


def flag_html(flags: list[str]) -> str:
    parts = []
    for f in flags:
        icon  = FLAG_ICONS.get(f, "•")
        parts.append(f'<span style="margin-right:8px">{icon} {f}</span>')
    return " ".join(parts)


def build_email_html(data: dict, hot: list[dict]) -> str:
    fetched   = data.get("fetched_at", "")[:16].replace("T", " ")
    date_range= data.get("date_range", "")
    total     = data.get("total", 0)
    with_addr = data.get("with_address", 0)

    # Category breakdown
    cats: dict[str, int] = {}
    for r in data.get("records", []):
        c = r.get("cat", "other")
        cats[c] = cats.get(c, 0) + 1

    cat_pills = ""
    for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
        color = CAT_COLORS.get(cat, "#3498db")
        cat_pills += (
            f'<span style="background:{color};color:#fff;padding:3px 10px;'
            f'border-radius:12px;font-size:12px;margin:2px;display:inline-block">'
            f'{cat.title()} {count}</span>'
        )

    # Lead rows
    rows_html = ""
    for r in hot[:50]:   # cap at 50 in email
        score     = r.get("score", 0)
        doc_type  = r.get("cat_label", r.get("doc_type", ""))
        filed     = r.get("filed", "")
        owner     = r.get("owner", "Unknown")
        prop_addr = r.get("prop_address", "")
        prop_city = r.get("prop_city", "")
        mail_addr = r.get("mail_address", "")
        flags     = r.get("flags", [])
        clerk_url = r.get("clerk_url", "")
        amount    = r.get("amount")
        cat       = r.get("cat", "other")
        color     = CAT_COLORS.get(cat, "#3498db")

        addr_line = ""
        if prop_addr:
            full = f"{prop_addr}, {prop_city}".strip(", ")
            addr_line = f'<div style="color:#27ae60;font-size:13px">📍 {full}</div>'
        elif mail_addr:
            addr_line = f'<div style="color:#7f8c8d;font-size:13px">📬 {mail_addr}</div>'

        amount_line = ""
        if amount:
            amount_line = f'<span style="color:#e74c3c;font-weight:bold">${amount:,.0f}</span> · '

        doc_link = (
            f'<a href="{clerk_url}" style="color:#3498db;font-size:12px;'
            f'text-decoration:none">View Record →</a>'
            if clerk_url else ""
        )

        rows_html += f"""
        <tr>
          <td style="padding:14px 12px;border-bottom:1px solid #2c2c2c;vertical-align:top">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
              <span style="background:{color};color:#fff;padding:2px 8px;
                border-radius:4px;font-size:11px;font-weight:bold">{doc_type}</span>
              <span style="color:#ecf0f1;font-weight:bold;font-size:15px">{owner}</span>
              <span style="color:#7f8c8d;font-size:12px;margin-left:auto">{filed}</span>
            </div>
            {addr_line}
            <div style="margin-top:6px;font-size:12px;color:#95a5a6">
              {amount_line}{flag_html(flags)}
            </div>
            <div style="margin-top:6px;font-family:monospace;font-size:11px;
              color:{color};letter-spacing:1px">
              {score_bar(score)} {score}/100
            </div>
            <div style="margin-top:4px">{doc_link}</div>
          </td>
        </tr>"""

    more_note = ""
    if len(hot) > 50:
        more_note = (
            f'<p style="text-align:center;color:#7f8c8d;font-size:13px">'
            f'+ {len(hot)-50} more leads on the dashboard</p>'
        )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#0d0d0d;font-family:
  -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#ecf0f1">

  <div style="max-width:680px;margin:0 auto;padding:20px">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1a1a2e,#16213e);
      border-radius:12px;padding:28px;margin-bottom:20px;
      border:1px solid #2c2c2c">
      <div style="display:flex;align-items:center;justify-content:space-between">
        <div>
          <h1 style="margin:0;font-size:24px;color:#f39c12">
            🏠 BCNM Leads
          </h1>
          <p style="margin:4px 0 0;color:#7f8c8d;font-size:13px">
            Daily Motivated Seller Alert · {fetched}
          </p>
        </div>
        <div style="text-align:right">
          <div style="font-size:36px;font-weight:bold;color:#e74c3c">{len(hot)}</div>
          <div style="font-size:12px;color:#7f8c8d">HOT LEADS</div>
        </div>
      </div>
    </div>

    <!-- Stats -->
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;
      margin-bottom:20px">
      <div style="background:#1a1a1a;border-radius:8px;padding:16px;
        border:1px solid #2c2c2c;text-align:center">
        <div style="font-size:28px;font-weight:bold;color:#ecf0f1">{total}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase">
          Total Records</div>
      </div>
      <div style="background:#1a1a1a;border-radius:8px;padding:16px;
        border:1px solid #2c2c2c;text-align:center">
        <div style="font-size:28px;font-weight:bold;color:#27ae60">{with_addr}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase">
          With Address</div>
      </div>
      <div style="background:#1a1a1a;border-radius:8px;padding:16px;
        border:1px solid #2c2c2c;text-align:center">
        <div style="font-size:28px;font-weight:bold;color:#f39c12">{len(hot)}</div>
        <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase">
          Score ≥ {HOT_THRESHOLD}</div>
      </div>
    </div>

    <!-- Categories -->
    <div style="background:#1a1a1a;border-radius:8px;padding:14px;
      margin-bottom:20px;border:1px solid #2c2c2c">
      <div style="font-size:11px;color:#7f8c8d;text-transform:uppercase;
        margin-bottom:8px">Document Types</div>
      {cat_pills}
    </div>

    <!-- Date range -->
    <p style="color:#7f8c8d;font-size:12px;margin-bottom:16px">
      📅 Records filed: <strong style="color:#ecf0f1">{date_range}</strong>
    </p>

    <!-- Lead table -->
    <div style="background:#1a1a1a;border-radius:8px;
      border:1px solid #2c2c2c;overflow:hidden;margin-bottom:20px">
      <div style="background:#16213e;padding:12px 16px">
        <span style="font-weight:bold;font-size:14px">
          🔥 Hot Leads (Score ≥ {HOT_THRESHOLD})
        </span>
      </div>
      <table style="width:100%;border-collapse:collapse">
        {rows_html}
      </table>
    </div>

    {more_note}

    <!-- CTA -->
    <div style="text-align:center;margin:24px 0">
      <a href="{DASHBOARD_URL}"
        style="background:#f39c12;color:#000;padding:14px 32px;
        border-radius:8px;text-decoration:none;font-weight:bold;
        font-size:15px;display:inline-block">
        Open Full Dashboard →
      </a>
    </div>

    <!-- Footer -->
    <div style="text-align:center;color:#4a4a4a;font-size:11px;
      padding-top:16px;border-top:1px solid #2c2c2c">
      BernCo Intel · Bernalillo County, NM · Automated Daily Report<br>
      Powered by Tyler Technologies Clerk Records + ArcGIS Parcel Data
    </div>

  </div>
</body>
</html>"""


def send_alert(html: str, hot_count: int, date_range: str) -> None:
    sg = sendgrid.SendGridAPIClient(api_key=API_KEY)
    message = Mail(
        from_email=From(FROM_EMAIL, "BernCo Intel"),
        to_emails=To(ALERT_EMAIL),
        subject=Subject(
            f"🏠 BernCo Leads: {hot_count} hot leads · {date_range}"
        ),
        html_content=HtmlContent(html),
    )
    response = sg.send(message)
    print(f"Alert sent → {ALERT_EMAIL}  status={response.status_code}")


def main() -> None:
    print("Loading records…")
    data    = load_records()
    records = data.get("records", [])
    hot     = filter_hot(records)

    print(f"Total records : {len(records)}")
    print(f"Hot leads (≥{HOT_THRESHOLD}): {len(hot)}")

    if not hot:
        print("No hot leads today — skipping email")
        return

    date_range = data.get("date_range", "")
    html       = build_email_html(data, hot)
    send_alert(html, len(hot), date_range)


if __name__ == "__main__":
    main()
