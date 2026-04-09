# Bernalillo County — Motivated Seller Lead Scraper

Automated daily scraper for public records filed at the Bernalillo County Clerk (NM).  
Runs via GitHub Actions every morning, publishes a live dashboard to GitHub Pages.

---

## What it collects

| Code | Type |
|------|------|
| LP | Lis Pendens |
| NOFC | Notice of Foreclosure |
| TAXDEED | Tax Deed |
| JUD / CCJ / DRJUD | Judgments |
| LNCORPTX / LNIRS / LNFED | Tax & Federal Liens |
| LN / LNMECH / LNHOA | Liens |
| MEDLN | Medicaid Lien |
| PRO | Probate |
| NOC | Notice of Commencement |
| RELLP | Release Lis Pendens |

---

## Seller Score (0–100)

| Condition | Points |
|-----------|--------|
| Base | +30 |
| Per motivated flag | +10 each |
| LP + Foreclosure combo | +20 |
| Amount > $100k | +15 |
| Amount > $50k | +10 |
| Filed this week | +5 |
| Has property address | +5 |

**Flags:** Lis pendens · Pre-foreclosure · Judgment lien · Tax lien · Mechanic lien · Probate/estate · LLC/corp owner · New this week

---

## File structure

```
.
├── scraper/
│   ├── fetch.py          # Main scraper (Playwright + requests)
│   └── requirements.txt
├── dashboard/
│   ├── index.html        # Live dashboard (GitHub Pages)
│   └── records.json      # Latest results (committed by Actions)
├── data/
│   ├── records.json      # Mirror of dashboard/records.json
│   └── ghl_export_YYYYMMDD.csv   # GoHighLevel CSV exports
└── .github/
    └── workflows/
        └── scrape.yml    # Daily cron + deploy
```

---

## Setup

### 1. Enable GitHub Pages
- Go to **Settings → Pages**
- Set source to **GitHub Actions**

### 2. Enable Actions permissions
- **Settings → Actions → General → Workflow permissions** → Read and write

### 3. (Optional) Run manually
- Go to **Actions → Daily Motivated Seller Scrape → Run workflow**

---

## Local development

```bash
pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium
python scraper/fetch.py
```

Results are written to `dashboard/records.json` and `data/records.json`.

---

## Output fields

`doc_num` · `doc_type` · `filed` · `cat` · `cat_label` · `owner` · `grantee` · `amount` · `legal` · `prop_address` · `prop_city` · `prop_state` · `prop_zip` · `mail_address` · `mail_city` · `mail_state` · `mail_zip` · `clerk_url` · `flags` · `score`
