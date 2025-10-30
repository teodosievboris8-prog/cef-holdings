import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone
import time

# The tickers you care about
TICKERS = ["DFP", "FFC", "FLC", "FPF", "HPF", "HPI", "HPS", "JPC", "LDP", "NPFD", "PDT", "PFD", "PFO", "PSF", "PTA"]

# Output path
OUTPUT_FILE = "output/selected_holdings.csv"

# SEC base URL
SEC_BASE = "https://data.sec.gov/api/xbrl/company_tickers.json"

HEADERS = {"User-Agent": "MyResearchScript (contact@example.com)"}


def get_cik_mapping():
    """Map tickers to CIKs from SEC's public API"""
    print("Fetching ticker → CIK map...")
    r = requests.get(SEC_BASE, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    mapping = {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in data.values()}
    return {t: mapping.get(t) for t in TICKERS if t in mapping}


def get_latest_nport_url(cik):
    """Find the most recent NPORT-P filing for a given CIK"""
    feed = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(feed, headers=HEADERS)
    if r.status_code != 200:
        return None
    data = r.json()
    filings = data.get("filings", {}).get("recent", {})
    forms = list(filings.get("form", []))
    accession = None
    for i, f in enumerate(forms):
        if f == "NPORT-P":
            accession = filings["accessionNumber"][i].replace("-", "")
            break
    if not accession:
        return None
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/primary_doc.xml"


def extract_holdings_from_nport(xml_text):
    """Extract holding names and values from NPORT XML"""
    df = pd.read_xml(xml_text, xpath=".//invstOrSecs")
    cols = [c for c in df.columns if "name" in c.lower() or "value" in c.lower()]
    return df[cols]


def extract_and_save():
    cik_map = get_cik_mapping()
    all_df = []

    for ticker, cik in cik_map.items():
        print(f"→ Fetching {ticker} ({cik})")
        url = get_latest_nport_url(cik)
        if not url:
            print(f"  ⚠️  No NPORT-P found for {ticker}")
            continue
        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            print(f"  ⚠️  Couldn't fetch {url}")
            continue
        df = extract_holdings_from_nport(r.text)
        df["Ticker"] = ticker
        all_df.append(df)
        time.sleep(1)

    if not all_df:
        print("No data extracted.")
        return

    out = pd.concat(all_df)
    os.makedirs("output", exist_ok=True)
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(out)} holdings to {OUTPUT_FILE}")


if __name__ == "__main__":
    extract_and_save()
