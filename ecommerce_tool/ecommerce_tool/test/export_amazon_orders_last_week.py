import os
import json
import time
import requests
import pandas as pd
from datetime import datetime

from ecommerce_tool.settings import (
    AMAZON_API_KEY,
    AMAZON_SECRET_KEY,
    REFRESH_TOKEN,
    MARKETPLACE_ID
)

# Step 1: Get access token
def get_amazon_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": AMAZON_API_KEY,
        "client_secret": AMAZON_SECRET_KEY,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        print(f"❌ Failed to get access token: {e}")
        return None

# Step 2: Request report generation
def create_order_report(access_token, start_time, end_time):
    url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    body = {
        "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",  # Changed this line
        "marketplaceIds": [MARKETPLACE_ID],
        "dataStartTime": start_time,
        "dataEndTime": end_time
    }

    response = requests.post(url, headers=headers, data=json.dumps(body))
    response.raise_for_status()
    return response.json().get("reportId")

# Step 3: Poll until report is done
def poll_report(access_token, report_id):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": access_token}

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        status = response.json().get("processingStatus")
        print(f"⏳ Report status: {status}")
        if status == "DONE":
            return response.json().get("reportDocumentId")
        elif status in ["CANCELLED", "FATAL"]:
            print(f"❌ Report generation failed with status: {status}")
            return None
        time.sleep(30)

def load_report_to_dataframe(file_path, start_date=None, end_date=None):
    df = pd.read_csv(file_path, sep="\t", dtype=str)

    if "purchase-date" in df.columns and start_date and end_date:
        df["purchase-date"] = pd.to_datetime(df["purchase-date"], errors="coerce")
        df = df[
            (df["purchase-date"] >= start_date) &
            (df["purchase-date"] <= end_date)
        ]
        print(f"📄 Filtered report has {len(df)} orders by purchase-date")
    else:
        print(f"📄 Full report loaded with {len(df)} rows")

    return df

# Step 4: Download the report file
def download_report(access_token, document_id, output_filename):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}"
    headers = {"x-amz-access-token": access_token}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    doc_info = response.json()

    download_url = doc_info["url"]
    file_response = requests.get(download_url)

    with open(output_filename, "wb") as f:
        f.write(file_response.content)
    print(f"✅ Report downloaded to {output_filename}")

    return output_filename

# Main execution
def get_amazon_orders_report(start_date: datetime, end_date: datetime):
    access_token = get_amazon_access_token()
    if not access_token:
        return

    iso_start = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    iso_end = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    print(f"📦 Requesting Amazon orders report from {iso_start} to {iso_end}")
    report_id = create_order_report(access_token, iso_start, iso_end)
    if not report_id:
        return

    document_id = poll_report(access_token, report_id)
    if not document_id:
        return

    filename = f"amazon_orders_{start_date.strftime('%Y-%m-%d')}.tsv"
    download_report(access_token, document_id, filename)

    df = load_report_to_dataframe(filename, start_date, end_date)

    # Optional: save as Excel
    df.to_excel(filename.replace(".tsv", ".xlsx"), index=False)
    print(f"📁 Excel saved: {filename.replace('.tsv', '.xlsx')}")
    return df

if __name__ == "__main__":
    for date_str in ["2025-07-13"]:
        start = datetime.strptime(date_str, "%Y-%m-%d")
        end = start.replace(hour=23, minute=59, second=59)
        get_amazon_orders_report(start, end)