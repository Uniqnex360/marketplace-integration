# from datetime import datetime, timedelta
# import pytz
# import math
# from mongoengine import connect
# from omnisight.models import Order, Product, Fee  # adjust import paths as needed

# # Connect to MongoDB
# connect(
#     db='ecommerce_db',
#     host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/',
#     port=27017
# )

# # Get today's date range in US/Pacific timezone
# pacific = pytz.timezone("US/Pacific")
# now_pacific = datetime.now(pacific)
# start_of_day = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
# end_of_day = now_pacific.replace(hour=23, minute=59, second=59, microsecond=999999)

# # Convert to UTC for MongoDB queries
# start_utc = start_of_day.astimezone(pytz.utc)
# end_utc = end_of_day.astimezone(pytz.utc)

# print(f"\n📅 Calculating for: {start_of_day.strftime('%Y-%m-%d')} (Pacific Time)")
# print(f"🔍 UTC Query Range: {start_utc} → {end_utc}\n")

# # ---------------------------------------
# # 📦 Step 1: Fetch Orders for Today
# # ---------------------------------------
# orders_today = Order.objects(order_date__gte=start_utc, order_date__lte=end_utc)
# print(f"🧾 Orders Found: {orders_today.count()}")

# # ---------------------------------------
# # 🧾 Step 2: Collect Items and SKUs
# # ---------------------------------------
# sku_qty_map = {}  # sku: total_quantity

# for order in orders_today:
#     # Access order_items which contains OrderItems references
#     for order_item in order.order_items:
#         # Access the ProductDetails embedded document
#         if order_item.ProductDetails:
#             sku = order_item.ProductDetails.SKU
#             qty = order_item.ProductDetails.QuantityOrdered or 0
            
#             if sku in sku_qty_map:
#                 sku_qty_map[sku] += qty
#             else:
#                 sku_qty_map[sku] = qty

# # ---------------------------------------
# # 📦 Step 3: Fetch Products by SKU
# # ---------------------------------------
# products = Product.objects(sku__in=list(sku_qty_map.keys()))

# # ---------------------------------------
# # 💰 Step 4: Calculate Expenses
# # ---------------------------------------
# total_cogs = 0
# manual_shipping = 0
# amazon_shipping = 0
# walmart_shipping = 0

# for product in products:
#     sku = product.sku
#     qty = sku_qty_map.get(sku, 0)

#     # Use the correct field names from your schema and handle None values
#     total_cogs += (product.total_cogs if product.total_cogs is not None else 0) * qty
#     manual_shipping += (product.shipping_cost if product.shipping_cost is not None else 0) * qty
#     amazon_shipping += (product.a_shipping_cost if product.a_shipping_cost is not None else 0) * qty
#     walmart_shipping += (product.w_shiping_cost if product.w_shiping_cost is not None else 0) * qty  # Note: keeping the typo as in schema

# # ---------------------------------------
# # 💸 Step 5: Fetch Fees for the Day
# # ---------------------------------------
# fees_today = Fee.objects(date__gte=start_utc, date__lte=end_utc)
# total_fees = sum(f.amount if f.amount is not None else 0 for f in fees_today)

# # ---------------------------------------
# # 📊 Step 6: Output Summary
# # ---------------------------------------
# print(f"\n🧮 Total COGS (from Orders):       ${total_cogs:,.2f}")
# print(f"🚚 Manual Shipping (Product):      ${manual_shipping:,.2f}")
# print(f"🚚 Amazon Shipping (Product):      ${amazon_shipping:,.2f}")
# print(f"🚚 Walmart Shipping (Product):     ${walmart_shipping:,.2f}")
# print(f"💸 Total Fees (from Fee logs):     ${total_fees:,.2f}")

# grand_total = total_cogs + manual_shipping + amazon_shipping + walmart_shipping + total_fees
# print(f"\n💰 Grand Total Expense Today:      ${grand_total:,.2f}")

# # ---------------------------------------
# # 🔍 Debug Information
# # ---------------------------------------
# print(f"\n🔍 Debug Info:")
# print(f"   • SKUs found in orders: {len(sku_qty_map)}")
# print(f"   • Products found in database: {products.count()}")
# print(f"   • Fee records found: {fees_today.count()}")
# if sku_qty_map:
#     print(f"   • Sample SKU quantities: {dict(list(sku_qty_map.items())[:3])}")
import os
import json
import time
import requests
import pandas as pd
from datetime import datetime
import pytz
from ecommerce_tool.settings import (
    AMAZON_API_KEY,
    AMAZON_SECRET_KEY,
    REFRESH_TOKEN,
    MARKETPLACE_ID
)
pacific = pytz.timezone("US/Pacific")

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
        "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
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

# Optional: Load to pandas DataFrame
def load_report_to_dataframe(file_path):
    df = pd.read_csv(file_path, sep="\t", dtype=str)
    print(f"📄 Report loaded with {len(df)} rows")
    return df

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

    df = load_report_to_dataframe(filename)
    # Optional: save as Excel
    df.to_excel(filename.replace(".tsv", ".xlsx"), index=False)
    print(f"📁 Excel saved: {filename.replace('.tsv', '.xlsx')}")
    return df
if __name__ == "__main__":
    # Example: Fetch for July 9 and 10, 2025
    for date_str in ["2025-07-28"]:
        start = datetime.strptime(date_str, "%Y-%m-%d")
        end = start.replace(hour=23, minute=59, second=59)
        start_pacific=pacific.localize(start)
        end_pacific=pacific.localize(end)
        start_utc=start_pacific.astimezone(pytz.utc)
        end_utc=end_pacific.astimezone(pytz.utc)
        get_amazon_orders_report(start_utc, end_utc)