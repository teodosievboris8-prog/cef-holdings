#!/usr/bin/env python3
"""
nport_holdings_cef.py

Fetches N-PORT holdings from SEC public S3 mirror for selected tickers
and writes output/selected_holdings.csv.
"""

import os
import io
import gzip
import json
import pandas as pd
from tqdm import tqdm
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import requests
import base64
import datetime

# -------- CONFIGURE HERE: your tickers (already set) ----------
TICKERS = {
    "DFP","FFC","FLC","FPF","HPF","HPI","HPS","JPC","LDP","NPFD",
    "PDT","PFD","PFO","PSF","PTA"
}

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "selected_holdings.csv")

# Public S3 bucket where SEC mirrors datasets
BUCKET = "sec-edgar"
PREFIX = "forms/nport-p/"

# Use unsigned (no AWS credentials needed)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

def list_nport_files():
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl.gz"):
                keys.append(key)
    return sorted(keys)

def stream_s3_jsonl(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    # decompress gzip stream
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
        for line in gz:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def extract_and_save():
    files = list_nport_files()
    print(f"Found {len(files)} N-PORT files to scan.")
    rows = []

    for key in files:
        print(f"Scanning: {key}")
        for obj in tqdm(stream_s3_jsonl(BUCKET, key), desc=key, unit="lines"):
            # find ticker in object (sometimes top-level or inside series)
            ticker = (obj.get("ticker") or obj.get("series", {}).get("ticker") or "").upper()
            if ticker not in TICKERS:
                continue

            series = obj.get("series", {})
            holdings = series.get("holdings", [])
            for h in holdings:
                rows.append({
                    "ticker": ticker,
                    "seriesName": series.get("name") or obj.get("seriesName"),
                    "holding_name": h.get("name"),
                    "cusip": h.get("cusip"),
                    "isin": h.get("isin"),
                    "valueUSD": h.get("valueUSD"),
                    "pctOfNAV": h.get("pctOfNAV"),
                    "country": h.get("country"),
                    "assetCategory": h.get("assetCategory"),
                    "positionType": h.get("positionType") or h.get("type"),
                    "file_source": key
                })

    if not rows:
        print("No holdings found for the selected tickers.")
        return False

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved {len(rows)} rows to {OUTPUT_FILE}")
    return True

if __name__ == "__main__":
    extract_and_save()
