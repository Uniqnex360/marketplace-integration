# # from datetime import datetime, timedelta
# # import pytz
# # import math
# # from mongoengine import connect
# # from omnisight.models import Order, Product, Fee  # adjust import paths as needed

# # # Connect to MongoDB
# # connect(
# #     db='ecommerce_db',
# #     host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/',
# #     port=27017
# # )

# # # Get today's date range in US/Pacific timezone
# # pacific = pytz.timezone("US/Pacific")
# # now_pacific = datetime.now(pacific)
# # start_of_day = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
# # end_of_day = now_pacific.replace(hour=23, minute=59, second=59, microsecond=999999)

# # # Convert to UTC for MongoDB queries
# # start_utc = start_of_day.astimezone(pytz.utc)
# # end_utc = end_of_day.astimezone(pytz.utc)

# # print(f"\n📅 Calculating for: {start_of_day.strftime('%Y-%m-%d')} (Pacific Time)")
# # print(f"🔍 UTC Query Range: {start_utc} → {end_utc}\n")

# # # ---------------------------------------
# # # 📦 Step 1: Fetch Orders for Today
# # # ---------------------------------------
# # orders_today = Order.objects(order_date__gte=start_utc, order_date__lte=end_utc)
# # print(f"🧾 Orders Found: {orders_today.count()}")

# # # ---------------------------------------
# # # 🧾 Step 2: Collect Items and SKUs
# # # ---------------------------------------
# # sku_qty_map = {}  # sku: total_quantity

# # for order in orders_today:
# #     # Access order_items which contains OrderItems references
# #     for order_item in order.order_items:
# #         # Access the ProductDetails embedded document
# #         if order_item.ProductDetails:
# #             sku = order_item.ProductDetails.SKU
# #             qty = order_item.ProductDetails.QuantityOrdered or 0
            
# #             if sku in sku_qty_map:
# #                 sku_qty_map[sku] += qty
# #             else:
# #                 sku_qty_map[sku] = qty

# # # ---------------------------------------
# # # 📦 Step 3: Fetch Products by SKU
# # # ---------------------------------------
# # products = Product.objects(sku__in=list(sku_qty_map.keys()))

# # # ---------------------------------------
# # # 💰 Step 4: Calculate Expenses
# # # ---------------------------------------
# # total_cogs = 0
# # manual_shipping = 0
# # amazon_shipping = 0
# # walmart_shipping = 0

# # for product in products:
# #     sku = product.sku
# #     qty = sku_qty_map.get(sku, 0)

# #     # Use the correct field names from your schema and handle None values
# #     total_cogs += (product.total_cogs if product.total_cogs is not None else 0) * qty
# #     manual_shipping += (product.shipping_cost if product.shipping_cost is not None else 0) * qty
# #     amazon_shipping += (product.a_shipping_cost if product.a_shipping_cost is not None else 0) * qty
# #     walmart_shipping += (product.w_shiping_cost if product.w_shiping_cost is not None else 0) * qty  # Note: keeping the typo as in schema

# # # ---------------------------------------
# # # 💸 Step 5: Fetch Fees for the Day
# # # ---------------------------------------
# # fees_today = Fee.objects(date__gte=start_utc, date__lte=end_utc)
# # total_fees = sum(f.amount if f.amount is not None else 0 for f in fees_today)

# # # ---------------------------------------
# # # 📊 Step 6: Output Summary
# # # ---------------------------------------
# # print(f"\n🧮 Total COGS (from Orders):       ${total_cogs:,.2f}")
# # print(f"🚚 Manual Shipping (Product):      ${manual_shipping:,.2f}")
# # print(f"🚚 Amazon Shipping (Product):      ${amazon_shipping:,.2f}")
# # print(f"🚚 Walmart Shipping (Product):     ${walmart_shipping:,.2f}")
# # print(f"💸 Total Fees (from Fee logs):     ${total_fees:,.2f}")

# # grand_total = total_cogs + manual_shipping + amazon_shipping + walmart_shipping + total_fees
# # print(f"\n💰 Grand Total Expense Today:      ${grand_total:,.2f}")

# # # ---------------------------------------
# # # 🔍 Debug Information
# # # ---------------------------------------
# # print(f"\n🔍 Debug Info:")
# # print(f"   • SKUs found in orders: {len(sku_qty_map)}")
# # print(f"   • Products found in database: {products.count()}")
# # print(f"   • Fee records found: {fees_today.count()}")
# # if sku_qty_map:
# #     print(f"   • Sample SKU quantities: {dict(list(sku_qty_map.items())[:3])}")
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

# # Step 1: Get access token
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

# # Step 2: Request report generation
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

# # Main execution
def create_order_report(access_token, start_date, end_date):
    """Create an order report with all available fields"""
    url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
    
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    
    # Request GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL report type
    # This includes shipping costs and fees
    payload = {
        "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
        "dataStartTime": start_date,
        "dataEndTime": end_date,
        "marketplaceIds": ["ATVPDKIKX0DER"]  # US marketplace
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 202:
        report_id = response.json()["reportId"]
        print(f"✅ Report created with ID: {report_id}")
        return report_id
    else:
        print(f"❌ Failed to create report: {response.status_code}")
        print(response.text)
        return None

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
    if df is None:
        return

    # Filter for today's orders in US/Pacific
    import pytz
    from datetime import datetime

    pacific = pytz.timezone("US/Pacific")
    today_pacific = datetime.now(pacific).date()

    def is_today_pacific(purchase_date_str):
        try:
            dt_utc = datetime.strptime(purchase_date_str, "%Y-%m-%dT%H:%M:%S%z")
            dt_pacific = dt_utc.astimezone(pacific)
            return dt_pacific.date() == today_pacific
        except Exception:
            return False

    if 'purchase-date' in df.columns:
        df_today = df[df['purchase-date'].apply(is_today_pacific)]
        
        # Print available columns to see what fields we have
        print("\n📊 Available columns in the report:")
        print(df.columns.tolist())
        
        # Key fields to look for:
        # - shipping-price: Shipping charged to customer
        # - shipping-tax: Tax on shipping
        # - item-price: Product price
        # - item-tax: Tax on product
        # - item-promotion-discount: Promotional discounts
        # - ship-promotion-discount: Shipping promotional discounts
        # - commission: Amazon's commission fee
        # - fba-fees: Fulfillment by Amazon fees
        
        # Save with all columns
        df_today.to_excel(filename.replace(".tsv", "_today.xlsx"), index=False)
        print(f"📁 Excel saved with all fields: {filename.replace('.tsv', '_today.xlsx')}")
        
        # Create a summary with key financial fields
        financial_columns = [
            'amazon-order-id', 'purchase-date', 'order-status',
            'sku', 'product-name', 'quantity-purchased',
            'item-price', 'item-tax', 'shipping-price', 'shipping-tax',
            'item-promotion-discount', 'ship-promotion-discount',
            'currency'
        ]
        
        # Only include columns that exist in the dataframe
        available_financial_cols = [col for col in financial_columns if col in df_today.columns]
        df_summary = df_today[available_financial_cols].copy()
        
        # Calculate total per order if price columns exist
        if 'item-price' in df_summary.columns and 'shipping-price' in df_summary.columns:
            df_summary['total-charged'] = (
                df_summary['item-price'].fillna(0) + 
                df_summary['shipping-price'].fillna(0) +
                df_summary['item-tax'].fillna(0) +
                df_summary['shipping-tax'].fillna(0) -
                df_summary['item-promotion-discount'].fillna(0) -
                df_summary['ship-promotion-discount'].fillna(0)
            )
        
        df_summary.to_excel(filename.replace(".tsv", "_financial_summary.xlsx"), index=False)
        print(f"📁 Financial summary saved: {filename.replace('.tsv', '_financial_summary.xlsx')}")
        
        return df_today
    else:
        print("❌ 'purchase-date' column not found in report.")
        return df
# if __name__ == "__main__":
    # Example: Fetch for July 9 and 10, 2025
    for date_str in ["2025-07-28"]:
        start = datetime.strptime(date_str, "%Y-%m-%d")
        end = start.replace(hour=23, minute=59, second=59)
        start_pacific=pacific.localize(start)
        end_pacific=pacific.localize(end)
        start_utc=start_pacific.astimezone(pytz.utc)
        end_utc=end_pacific.astimezone(pytz.utc)
        get_amazon_orders_report(start_utc, end_utc)
import os
import json
import time
import gzip
import requests
import pandas as pd
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from ecommerce_tool.settings import (
    AMAZON_API_KEY,
    AMAZON_SECRET_KEY,
    REFRESH_TOKEN,
    MARKETPLACE_ID
)

pacific = pytz.timezone("US/Pacific")

# Existing functions (get_amazon_access_token, create_report, poll_report, download_report, load_report_to_dataframe)
# def get_amazon_access_token():
#     url = "https://api.amazon.com/auth/o2/token"
#     payload = {
#         "grant_type": "refresh_token",
#         "refresh_token": REFRESH_TOKEN,
#         "client_id": AMAZON_API_KEY,
#         "client_secret": AMAZON_SECRET_KEY,
#     }
#     headers = {"Content-Type": "application/x-www-form-urlencoded"}

#     try:
#         response = requests.post(url, data=payload, headers=headers)
#         response.raise_for_status()
#         return response.json().get("access_token")
#     except Exception as e:
#         print(f"❌ Failed to get access token: {e}")
#         return None

def create_report(access_token, report_type, marketplace_ids, data_start_time=None, data_end_time=None):
    url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    body = {
        "reportType": report_type,
        "marketplaceIds": marketplace_ids,
    }
    if data_start_time:
        body["dataStartTime"] = data_start_time
    if data_end_time:
        body["dataEndTime"] = data_end_time

    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
        return response.json().get("reportId")
    except Exception as e:
        print(f"❌ Failed to create report of type {report_type}: {e}")
        return None

def poll_report(access_token, report_id):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}"
    headers = {"x-amz-access-token": access_token}

    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json().get("processingStatus")
            print(f"⏳ Report status: {status}")
            if status == "DONE":
                return response.json().get("reportDocumentId")
            elif status in ["CANCELLED", "FATAL"]:
                print(f"❌ Report generation failed with status: {status}. Response: {response.json()}")
                return None
            time.sleep(30)
        except Exception as e:
            print(f"❌ Error polling report {report_id}: {e}")
            return None

def download_report(access_token, document_id, output_filename):
    url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}"
    headers = {"x-amz-access-token": access_token}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        doc_info = response.json()

        download_url = doc_info["url"]
        file_response = requests.get(download_url)
        file_response.raise_for_status()

        content = file_response.content
        if doc_info.get('compressionAlgorithm') == 'GZIP':
            print("Detected GZIP compression, decompressing...")
            content = gzip.decompress(content)

        with open(output_filename, "wb") as f:
            f.write(content)
        print(f"✅ Report downloaded and decompressed to {output_filename}")
        return output_filename
    except Exception as e:
        print(f"❌ Failed to download report document {document_id}: {e}")
        return None

def load_report_to_dataframe(file_path):
    try:
        encodings_to_try = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
        
        df = None
        successful_encoding = None
        
        for encoding in encodings_to_try:
            try:
                print(f"🔍 Trying encoding: {encoding}")
                df = pd.read_csv(file_path, sep="\t", dtype=str, encoding=encoding)
                successful_encoding = encoding
                print(f"✅ Successfully loaded with encoding: {encoding}")
                break
            except UnicodeDecodeError as e:
                print(f"❌ Failed with {encoding}: {e}")
                continue
            except Exception as e:
                print(f"❌ Other error with {encoding}: {e}")
                continue
        
        if df is not None:
            print(f"📄 Report loaded with {len(df)} rows using {successful_encoding} encoding")
            return df
        else:
            print("❌ Failed to load report with any of the attempted encodings")
            return None
            
    except Exception as e:
        print(f"❌ Failed to load report to DataFrame from {file_path}: {e}")
        return None

# NEW: Function to get product category information using Catalog Items API
def get_product_category(access_token, asin, marketplace_id):
    """
    Get product category information for a specific ASIN using Catalog Items API
    """
    url = f"https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items/{asin}"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    params = {
        "marketplaceIds": marketplace_id,
        "includedData": "attributes,categories,productTypes"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            
            # Extract category information
            categories = []
            product_types = []
            
            # Get categories if available
            if "categories" in data:
                for category in data["categories"]:
                    if "displayName" in category:
                        categories.append(category["displayName"])
            
            # Get product types if available
            if "productTypes" in data:
                for pt in data["productTypes"]:
                    if "displayName" in pt:
                        product_types.append(pt["displayName"])
            
            return {
                "categories": " | ".join(categories) if categories else "",
                "product_types": " | ".join(product_types) if product_types else "",
                "primary_category": categories[0] if categories else ""
            }
    except Exception as e:
        print(f"❌ Error getting category for ASIN {asin}: {e}")
    
    return {
        "categories": "",
        "product_types": "",
        "primary_category": ""
    }

# NEW: Function to get categories for multiple products with rate limiting
def get_categories_for_products(access_token, asins, marketplace_id, max_workers=5):
    """
    Get category information for multiple ASINs with threading and rate limiting
    """
    print(f"🔍 Fetching category information for {len(asins)} products...")
    
    category_data = {}
    
    def fetch_category(asin):
        time.sleep(0.5)  # Rate limiting - 2 requests per second
        return asin, get_product_category(access_token, asin, marketplace_id)
    
    # Use ThreadPoolExecutor for parallel requests (with rate limiting)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_asin = {executor.submit(fetch_category, asin): asin for asin in asins}
        
        completed = 0
        for future in as_completed(future_to_asin):
            asin, category_info = future.result()
            category_data[asin] = category_info
            completed += 1
            
            if completed % 10 == 0:  # Progress update every 10 items
                print(f"📈 Progress: {completed}/{len(asins)} products processed")
    
    print(f"✅ Category data fetched for {len(category_data)} products")
    return category_data

# ENHANCED: Main execution for products with categories
def get_amazon_products_report_with_categories():
    access_token = get_amazon_access_token()
    if not access_token:
        return None

    # First, get the basic product listing report
    report_type = "GET_MERCHANT_LISTINGS_ALL_DATA"
    print(f"📦 Requesting Amazon products report ({report_type})")
    
    report_id = create_report(access_token, report_type, [MARKETPLACE_ID])
    if not report_id:
        return None

    document_id = poll_report(access_token, report_id)
    if not document_id:
        return None

    filename = f"amazon_products_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.tsv"
    downloaded_path = download_report(access_token, document_id, filename)

    if not downloaded_path:
        return None

    df = load_report_to_dataframe(downloaded_path)
    if df is None:
        return None

    # Check if ASIN column exists (it might be named differently)
    asin_column = None
    possible_asin_columns = ['asin', 'ASIN', 'asin1', 'ASIN1', 'item-name']
    
    for col in possible_asin_columns:
        if col in df.columns:
            asin_column = col
            break
    
    if not asin_column:
        print("❌ Could not find ASIN column in the report. Available columns:")
        print(df.columns.tolist())
        print("📋 Saving report without category information...")
    else:
        print(f"✅ Found ASIN column: {asin_column}")
        
        # Get unique ASINs (remove any empty/null values)
        unique_asins = df[asin_column].dropna().unique()
        unique_asins = [asin for asin in unique_asins if asin and str(asin).strip()]
        
        if len(unique_asins) > 0:
            print(f"🎯 Found {len(unique_asins)} unique ASINs to process")
            
            # Get category information for all ASINs
            category_data = get_categories_for_products(access_token, unique_asins, MARKETPLACE_ID)
            
            # Add category columns to the dataframe
            df['primary_category'] = df[asin_column].map(lambda x: category_data.get(x, {}).get('primary_category', ''))
            df['all_categories'] = df[asin_column].map(lambda x: category_data.get(x, {}).get('categories', ''))
            df['product_types'] = df[asin_column].map(lambda x: category_data.get(x, {}).get('product_types', ''))
            
            print("✅ Category information added to the dataframe")
        else:
            print("❌ No valid ASINs found in the report")

    # Save as Excel
    excel_filename = downloaded_path.replace(".tsv", ".xlsx")
    df.to_excel(excel_filename, index=False)
    print(f"📁 Excel saved with category information: {excel_filename}")
    
    return df

# ALTERNATIVE: Use a different report type that might include more category info
def get_amazon_products_report_alternative():
    """
    Try alternative report types that might include category information
    """
    access_token = get_amazon_access_token()
    if not access_token:
        return None

    # Try different report types
    report_types_to_try = [
        "GET_MERCHANT_LISTINGS_DATA",  # Might have more detailed info
        "GET_FLAT_FILE_OPEN_LISTINGS_DATA",  # Alternative listing report
        "GET_FBA_MYI_ALL_INVENTORY_DATA"  # FBA inventory report (if using FBA)
    ]
    
    for report_type in report_types_to_try:
        print(f"📦 Trying report type: {report_type}")
        
        report_id = create_report(access_token, report_type, [MARKETPLACE_ID])
        if report_id:
            document_id = poll_report(access_token, report_id)
            if document_id:
                filename = f"amazon_products_{report_type}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.tsv"
                downloaded_path = download_report(access_token, document_id, filename)
                
                if downloaded_path:
                    df = load_report_to_dataframe(downloaded_path)
                    if df is not None:
                        print(f"✅ Successfully got report with {report_type}")
                        print("Available columns:", df.columns.tolist())
                        
                        # Save as Excel
                        excel_filename = downloaded_path.replace(".tsv", ".xlsx")
                        df.to_excel(excel_filename, index=False)
                        print(f"📁 Excel saved: {excel_filename}")
                        return df
        
        print(f"❌ Failed to get report with {report_type}, trying next...")
    
    print("❌ All alternative report types failed")
    return None


def get_fba_shipping_and_fee_report(start_date: datetime, end_date: datetime):
    access_token = get_amazon_access_token()
    if not access_token:
        return None

    report_type = "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_SALES_DATA"
    marketplace_ids = [MARKETPLACE_ID]
    
    print(f"📦 Requesting FBA shipping/fee report from {start_date} to {end_date}")
    report_id = create_report(
        access_token,
        report_type,
        marketplace_ids,
        data_start_time=start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        data_end_time=end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    )
    
    if not report_id:
        return None

    document_id = poll_report(access_token, report_id)
    if not document_id:
        return None

    filename = f"fba_fees_{start_date.strftime('%Y-%m-%d')}.tsv"
    downloaded_path = download_report(access_token, document_id, filename)
    if not downloaded_path:
        return None

    df = load_report_to_dataframe(downloaded_path)
    if df is None:
        return None

    # Optional: Save to Excel
    excel_filename = downloaded_path.replace(".tsv", ".xlsx")
    df.to_excel(excel_filename, index=False)
    print(f"📁 Excel saved: {excel_filename}")

    # Show a preview
    print(f"\n📊 FBA Fees Report Loaded: {df.shape[0]} rows")
    print("📋 Columns:", df.columns.tolist())
    print(df.head())

    return df
# if __name__ == "__main__":
#     from datetime import timedelta

#     # Yesterday's report
#     end_date = datetime.now(pytz.utc).replace(hour=23, minute=59, second=59)
#     start_date = end_date - timedelta(days=1)

#     get_fba_shipping_and_fee_report(start_date, end_date)
if __name__ == "__main__":
    import pytz
    from datetime import datetime

    pacific = pytz.timezone("US/Pacific")
    today = datetime.now(pacific)
    start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = today.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_utc = start_of_day.astimezone(pytz.utc)
    end_utc = end_of_day.astimezone(pytz.utc)

    get_amazon_orders_report(start_utc, end_utc)