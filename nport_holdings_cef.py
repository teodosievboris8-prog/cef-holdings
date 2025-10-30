import os
import requests
import pandas as pd
import time

# Target CEF tickers
TICKERS = ["DFP", "FFC", "FLC", "FPF", "HPF", "HPI", "HPS", "JPC", "LDP", "NPFD", "PDT", "PFD", "PFO", "PSF", "PTA"]

# Output CSV path
OUTPUT_FILE = "output/holdings.csv"

# User-Agent for SEC requests
HEADERS = {"User-Agent": "CEF Holdings Script (youremail@example.com)"}

# Hardcoded ticker → CIK mapping (2025)
CIK_MAP = {
    "DFP": "0000765658",
    "FFC": "0000826485",
    "FLC": "0001048620",
    "FPF": "0000851680",
    "HPF": "0000911136",
    "HPI": "0000906455",
    "HPS": "0000912247",
    "JPC": "0001396866",
    "LDP": "0001103535",
    "NPFD": "0001198886",
    "PDT": "0001392671",
    "PFD": "0000853080",
    "PFO": "0000915005",
    "PSF": "0000924426",
    "PTA": "0000913278"
}

def get_latest_nport_url(cik):
    """Get the most recent NPORT-P filing XML URL"""
    feed_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(feed_url, headers=HEADERS)
    if r.status_code != 200:
        return None
    data = r.json()
    filings = data.get("filings", {}).get("recent", {})
    for i, form in enumerate(filings.get("form", [])):
        if form == "NPORT-P":
            accession = filings["accessionNumber"][i].replace("-", "")
            return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/primary_doc.xml"
    return None

def extract_holdings_from_nport(xml_text):
    """Extract holdings from NPORT-P XML"""
    try:
        df = pd.read_xml(xml_text, xpath=".//invstOrSecs")
        cols = [c for c in df.columns if "name" in c.lower() or "value" in c.lower()]
        return df[cols]
    except Exception as e:
        print(f"⚠️ Failed to parse XML: {e}")
        return pd.DataFrame()

def extract_and_save():
    os.makedirs("output", exist_ok=True)
    all_data = []

    for ticker in TICKERS:
        cik = CIK_MAP[ticker]
        print(f"🔹 Fetching {ticker} ({cik})")
        url = get_latest_nport_url(cik)
        if not url:
            print(f"  ⚠️ No NPORT-P found for {ticker}")
            continue
        r = requests.get(url, headers=HEADERS)
        if r.status_code != 200:
            print(f"  ⚠️ Could not fetch {url}")
            continue
        df = extract_holdings_from_nport(r.text)
        if df.empty:
            print("  ⚠️ No holdings found in filing")
            continue
        df["Ticker"] = ticker
        all_data.append(df)
        time.sleep(1)

    if not all_data:
        print("❌ No holdings extracted")
        return

    final_df = pd.concat(all_data)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(final_df)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    extract_and_save()
