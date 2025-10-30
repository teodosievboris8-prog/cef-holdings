import os
import requests
import pandas as pd
import time

# === Your target tickers ===
TICKERS = ["DFP", "FFC", "FLC", "FPF", "HPF", "HPI", "HPS", "JPC", "LDP", "NPFD", "PDT", "PFD", "PFO", "PSF", "PTA"]

# === Output path ===
OUTPUT_FILE = "output/holdings.csv"

# === SEC API URL for ticker → CIK mapping ===
SEC_TICKER_API = "https://data.sec.gov/api/xbrl/company_tickers.json"
HEADERS = {"User-Agent": "CEF Holdings Script (youremail@example.com)"}


def get_cik_mapping():
    """Map tickers to CIKs from SEC API"""
    r = requests.get(SEC_TICKER_API, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    mapping = {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in data.values()}
    return {t: mapping.get(t) for t in TICKERS if mapping.get(t)}


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
    cik_map = get_cik_mapping()
    all_data = []

    for ticker, cik in cik_map.items():
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
        time.sleep(1)  # Be polite to SEC servers

    if not all_data:
        print("❌ No holdings extracted")
        return

    final_df = pd.concat(all_data)
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(final_df)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    extract_and_save()
