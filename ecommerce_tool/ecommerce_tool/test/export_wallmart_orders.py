import requests
import uuid
import json
import pandas as pd
from datetime import datetime, timedelta
from omnisight.operations.walmart_utils import oauthFunction
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import Marketplace
import os


def fetchYesterdayWalmartOrders():
    """
    Fetches Walmart orders from yesterday and saves them as JSON and Excel files.
    Returns the fetched orders data.
    """
    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    # Calculate yesterday's date range
    yesterday = datetime.utcnow() - timedelta(days=1)
    start_date = yesterday.strftime("%Y-%m-%dT00:00:00Z")
    end_date = yesterday.strftime("%Y-%m-%dT23:59:59Z")
    
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    # Initial URL with date filters
    url = f"{base_url}?createdStartDate={start_date}&createdEndDate={end_date}&limit=100"
    
    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"📅 Fetching Walmart orders for {yesterday.strftime('%Y-%m-%d')}...")

    while True:
        # Use cursor for pagination if available
        paged_url = f"{base_url}{next_cursor}" if next_cursor else url
        
        print(f"📡 Fetching page {page}...")
        response = requests.get(paged_url, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            new_orders = result.get('list', {}).get('elements', {}).get('order', [])
            
            if not new_orders:
                print("✅ No more orders found")
                break
                
            fetched_orders.extend(new_orders)
            print(f"📦 Found {len(new_orders)} orders on page {page}")
            
            # Check for next page
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                print("✅ Reached last page")
                break
                
            page += 1
            
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break

    print(f"📋 Total orders fetched: {len(fetched_orders)}")
    
    if fetched_orders:
        # Save as JSON file
        json_filename = f"walmart_orders_{yesterday.strftime('%Y%m%d')}.json"
        saveOrdersAsJSON(fetched_orders, json_filename)
        
        # Save as Excel file
        excel_filename = f"walmart_orders_{yesterday.strftime('%Y%m%d')}.xlsx"
        saveOrdersAsExcel(fetched_orders, excel_filename)
        
        print(f"✅ Orders saved as {json_filename} and {excel_filename}")
    else:
        print("📭 No orders found for yesterday")
    
    return fetched_orders


def saveOrdersAsJSON(orders, filename):
    """
    Saves orders data as a JSON file.
    """
    try:
        # Create downloads directory if it doesn't exist
        os.makedirs("downloads", exist_ok=True)
        filepath = os.path.join("downloads", filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(orders, f, indent=2, default=str)
        
        print(f"💾 JSON file saved: {filepath}")
        
    except Exception as e:
        print(f"❌ Error saving JSON file: {e}")


def saveOrdersAsExcel(orders, filename):
    """
    Saves orders data as an Excel file with one row per order (not per order line).
    """
    try:
        os.makedirs("downloads", exist_ok=True)
        filepath = os.path.join("downloads", filename)
        
        order_rows = []
        for order in orders:
            # Gather all SKUs and product names in this order
            order_lines = order.get('orderLines', {}).get('orderLine', [])
            skus = [line.get('item', {}).get('sku', '') for line in order_lines]
            product_names = [line.get('item', {}).get('productName', '') for line in order_lines]
            total_quantity = sum(int(line.get('orderLineQuantity', {}).get('amount', 0)) for line in order_lines)
            
            row = {
                'purchase_order_id': order.get('purchaseOrderId', ''),
                'customer_order_id': order.get('customerOrderId', ''),
                'customer_email_id': order.get('customerEmailId', ''),
                'order_date': datetime.fromtimestamp(int(order.get('orderDate', 0)) / 1000).strftime('%Y-%m-%d %H:%M:%S') if order.get('orderDate') else '',
                'ship_node_type': order.get('shipNode', {}).get('type', ''),
                'ship_node_id': order.get('shipNode', {}).get('shipNodeId', ''),
                'shipping_phone': order.get('shippingInfo', {}).get('phone', ''),
                'shipping_estimated_delivery': order.get('shippingInfo', {}).get('estimatedDeliveryDate', ''),
                'shipping_method': order.get('shippingInfo', {}).get('methodCode', ''),
                'shipping_postal_address': str(order.get('shippingInfo', {}).get('postalAddress', {})),
                'all_skus': ', '.join(skus),
                'all_product_names': ', '.join(product_names),
                'total_quantity': total_quantity,
                'number_of_items': len(order_lines),
            }
            order_rows.append(row)
        
        df = pd.DataFrame(order_rows)
        df.to_excel(filepath, index=False, engine='openpyxl')
        print(f"📊 Excel file saved: {filepath}")
        print(f"📈 Total orders: {len(order_rows)}")
        
    except Exception as e:
        print(f"❌ Error saving Excel file: {e}")
def downloadYesterdayOrdersReport():
    """
    Main function to download yesterday's Walmart orders report.
    Can be called directly or from a scheduled job.
    """
    try:
        print("🚀 Starting Walmart Yesterday Orders Download...")
        orders = fetchYesterdayWalmartOrders()
        
        if orders:
            print(f"✅ Successfully downloaded {len(orders)} orders from yesterday")
            return True
        else:
            print("📭 No orders found for yesterday")
            return False
            
    except Exception as e:
        print(f"❌ Error in downloadYesterdayOrdersReport: {e}")
        return False


def fetchWalmartOrdersByDateRange(start_date, end_date):
    """
    Fetches Walmart orders for a custom date range.
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
    
    Returns:
        list: List of orders
    """
    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    # Format dates for API
    start_date_str = f"{start_date}T00:00:00Z"
    end_date_str = f"{end_date}T23:59:59Z"
    
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    url = f"{base_url}?createdStartDate={start_date_str}&createdEndDate={end_date_str}&limit=100"
    
    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"📅 Fetching Walmart orders from {start_date} to {end_date}...")

    while True:
        paged_url = f"{base_url}{next_cursor}" if next_cursor else url
        
        print(f"📡 Fetching page {page}...")
        response = requests.get(paged_url, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            new_orders = result.get('list', {}).get('elements', {}).get('order', [])
            
            if not new_orders:
                break
                
            fetched_orders.extend(new_orders)
            print(f"📦 Found {len(new_orders)} orders on page {page}")
            
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                break
                
            page += 1
            
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break

    print(f"📋 Total orders fetched: {len(fetched_orders)}")
    
    if fetched_orders:
        # Save files with date range in filename
        json_filename = f"walmart_orders_{start_date}_to_{end_date}.json"
        excel_filename = f"walmart_orders_{start_date}_to_{end_date}.xlsx"
        
        saveOrdersAsJSON(fetched_orders, json_filename)
        saveOrdersAsExcel(fetched_orders, excel_filename)
        
        print(f"✅ Orders saved as {json_filename} and {excel_filename}")
    
    return fetched_orders


# Example usage:
if __name__ == "__main__":
    # Download yesterday's orders
    # downloadYesterdayOrdersReport()
    
    # Or download orders for a specific date range
    fetchWalmartOrdersByDateRange("2025-07-13", "2025-07-13")

