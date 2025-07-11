import os
import pandas as pd
import requests
import django
from datetime import datetime, timedelta
from ecommerce_tool.settings import AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN, MARKETPLACE_ID
from omnisight.models import Order, Marketplace  # Assuming these are your models
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ecommerce_tool.settings")  # Update if settings module is 
from ecommerce_tool.util.santize_input import sanitize_value

django.setup()

TOKEN_URL = "https://api.amazon.com/auth/o2/token"
ORDERS_API_URL = "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"

def get_amazon_access_token():
    """Retrieves a new Amazon SP-API access token using the refresh token."""
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": AMAZON_API_KEY,
        "client_secret": AMAZON_SECRET_KEY,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        response = requests.post(TOKEN_URL, data=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to get access token: {e}")
        return None

def fetch_orders_from_amazon(created_after, created_before, marketplace_ids=[MARKETPLACE_ID]):
    """Fetches orders from Amazon SP-API with pagination support."""
    access_token = get_amazon_access_token()
    if not access_token:
        return None

    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    params = {
        "CreatedAfter": created_after,
        "CreatedBefore": created_before,
        "MarketplaceIds": marketplace_ids,
        "OrderStatuses": ["Shipped", "Unshipped", "PartiallyShipped",'Canceled'],
        "MaxResultsPerPage": 100
    }

    all_orders = []
    next_token = None

    while True:
        current_params = params.copy()
        if next_token:
            current_params["NextToken"] = next_token

        try:
            response = requests.get(ORDERS_API_URL, headers=headers, params=current_params)
            if response.status_code != 200:
                print(f"❌ Error fetching orders: {response.status_code} - {response.text}")
                return None

            data = response.json()
            orders = data.get("payload", {}).get("Orders", [])
            all_orders.extend(orders)

            next_token = data.get("payload", {}).get("NextToken")
            if not next_token:
                break

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return None

    return all_orders

def get_amazon_orders_yesterday(start_date,end_date:datetime,utput_file=None):
    """
    Fetch Amazon orders from yesterday and export them to an Excel file.
    No database operations are performed.
    """
    # Define time range for yesterday
    # yesterday = datetime.utcnow() - timedelta(days=1)
    # start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    # end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    created_after = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    created_before = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')

    print(f"📦 Fetching Amazon orders from {created_after} to {created_before}")

    # Fetch orders via your API
    orders = fetch_orders_from_amazon(created_after, created_before)
    print(f"Total orders fetched from API: {len(orders)}")
    if not orders:
        print("⚠️ No Amazon orders returned for yesterday.")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(orders)
    print(f"DataFrame shape before export: {df.shape}")  # Debugging line
    
    # Optional: sanitize/clean data
    # df['AmazonOrderId'] = df['AmazonOrderId'].apply(lambda x: sanitize_value(x, value_type=str))

    # Export to Excel
    if output_file is None:
        output_file = f"amazon_orders_yesterday_{start_date.strftime('%Y-%m-%d')}.xlsx"
    try:
        df.to_excel(output_file, index=False, sheet_name="Orders")
        print(f"✅ Successfully exported orders to {output_file}")
    except Exception as e:
        print(f"❌ Failed to export to Excel: {e}")

    return df
if __name__ == "__main__":
    start=datetime(2024,7,9)
    end = datetime(2025, 7, 10, 23, 59, 59)

    get_amazon_orders_yesterday(start,end)
    
    # Then export last week's orders to Excel files