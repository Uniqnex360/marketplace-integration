import requests
import uuid
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz
from omnisight.operations.walmart_utils import oauthFunction
import os

def getWalmartOrderDetails(purchase_order_id):
    """
    Fetches and displays details of a Walmart order by its purchaseOrderId.
    """
    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    url = f"https://marketplace.walmartapis.com/v3/orders/{purchase_order_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        order = response.json().get('order', {})
        print(f"🧾 Order Details for: {purchase_order_id}\n")
        print(json.dumps(order, indent=2))  # Pretty print full order
        
        # Fixed: Properly nested loops to check all charges
        total_shipping = 0
        for line in order.get('orderLines', {}).get('orderLine', []):
            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:  # Now properly indented within the line loop
                charge_type = charge.get('chargeType')
                amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                if charge_type == 'SHIPPING':
                    total_shipping += amount
                    print(f"🚚 Shipping Charge (Line): ${amount}")
        
        # Display total shipping if any found
        if total_shipping > 0:
            print(f"🚚 Total Shipping Charge: ${total_shipping}")
        else:
            print("ℹ️ No shipping charges found")

        return order
    else:
        print(f"❌ Error fetching order: [HTTP {response.status_code}] {response.text}")
        return None

def fetchTodayWalmartOrdersCount(exclude_cancelled=True, min_order_value=0):
    """
    Fetches Walmart orders for today (US/Pacific), filters them, and exports to Excel with shipping prices.
    """
    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    # Timezone and date range setup
    pacific = pytz.timezone("US/Pacific")
    now_pacific = datetime.now(pacific)
    start_of_day = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now_pacific.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Convert to UTC for Walmart API
    start_date_utc = start_of_day.astimezone(pytz.utc)
    end_date_utc = end_of_day.astimezone(pytz.utc)
    start_date = start_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = end_date_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    url = f"{base_url}?createdStartDate={start_date}&createdEndDate={end_date}&limit=100"
    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"📦 Fetching Walmart orders for today")
    print(f"📅 Pacific Time: {start_of_day.strftime('%Y-%m-%d %H:%M:%S')} → {end_of_day.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 UTC Time: {start_date} → {end_date}")

    while True:
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
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                print("✅ Reached last page")
                break

            page += 1
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break

    print(f"\n📊 Initial orders fetched: {len(fetched_orders)}")

    # Filter orders
    filtered_orders = []
    cancelled_count = 0
    zero_value_count = 0

    for order in fetched_orders:
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        if not order_lines:
            continue

        all_cancelled = True
        order_total = 0

        for line in order_lines:
            statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            current_status = ''
            if statuses:
                first_status = statuses[0]
                status_val = first_status.get('status', '') if isinstance(first_status, dict) else str(first_status)
                current_status = status_val if isinstance(status_val, str) else status_val.get('status', '')

            if current_status.upper() not in ['CANCELLED', 'CANCELED']:
                all_cancelled = False

            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    price = float(charge.get('chargeAmount', {}).get('amount', 0))
                    qty = int(line.get('orderLineQuantity', {}).get('amount', 0))
                    order_total += price * qty

        if exclude_cancelled and all_cancelled:
            cancelled_count += 1
            continue
        if order_total <= min_order_value:
            zero_value_count += 1
            continue

        filtered_orders.append(order)

    # Summary counts
    total_orders = len(filtered_orders)
    total_order_items = 0
    total_revenue = 0
    rows = []

    for order in filtered_orders:
        order_id = order.get("purchaseOrderId")
        customer_id = order.get("customerOrderId")
        order_date_ms = int(order.get("orderDate", 0))
        order_date = datetime.fromtimestamp(order_date_ms / 1000).astimezone(pacific).strftime("%Y-%m-%d %H:%M:%S")

        for line in order.get("orderLines", {}).get("orderLine", []):
            sku = line.get("item", {}).get("sku")
            name = line.get("item", {}).get("productName")
            qty = int(line.get("orderLineQuantity", {}).get("amount", 0))
            total_order_items += qty

            product_price = 0
            shipping_price = 0
            charges = line.get("charges", {}).get("charge", [])
            for charge in charges:
                if charge.get("chargeType") == "PRODUCT":
                    product_price = float(charge.get("chargeAmount", {}).get("amount", 0))
                elif charge.get("chargeType") == "SHIPPING":
                    shipping_price = float(charge.get("chargeAmount", {}).get("amount", 0))

            statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            line_status = 'Unknown'
            if statuses:
                status_val = statuses[0].get('status', '') if isinstance(statuses[0], dict) else str(statuses[0])
                line_status = status_val if isinstance(status_val, str) else status_val.get('status', '')

            total_revenue += product_price * qty

            rows.append({
                "Order ID": order_id,
                "Customer ID": customer_id,
                "Order Date (Pacific)": order_date,
                "SKU": sku,
                "Product Name": name,
                "Quantity": qty,
                "Unit Price": product_price,
                "Shipping Price": shipping_price,
                "Line Total": qty * product_price,
                "Line Status": line_status
            })

    # Export to Excel
    df = pd.DataFrame(rows)
    filename = f"walmart_orders_{now_pacific.strftime('%Y-%m-%d')}.xlsx"
    df.to_excel(filename, index=False)
    print(f"\n📁 Exported to Excel: {filename}")

    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

    print(f"\n📊 Total Orders: {total_orders}")
    print(f"📦 Total Order Items: {total_order_items}")
    print(f"💰 Total Revenue: ${total_revenue:,.2f}")
    print(f"💵 Average Order Value: ${avg_order_value:,.2f}")

    return {
        "total_orders": total_orders,
        "total_order_items": total_order_items,
        "total_revenue": total_revenue,
        "average_order_value": avg_order_value,
        "filtered_orders": filtered_orders,
        "excel_file": filename
    }


def countWalmartOrdersByDateRange(start_date, end_date, timezone="US/Pacific", exclude_cancelled=True, min_order_value=0):
    """
    Fetches and counts Walmart orders for a custom date range with filtering options.
    
    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'
        timezone (str): Timezone for the date range (default: US/Pacific)
        exclude_cancelled (bool): Whether to exclude cancelled orders
        min_order_value (float): Minimum order value to include
    
    Returns:
        dict: Dictionary with order counts and details
    """
    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    # Parse dates and set timezone
    tz = pytz.timezone(timezone)
    start_dt = tz.localize(datetime.strptime(start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0))
    end_dt = tz.localize(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59))
    
    # Convert to UTC for API
    start_date_utc = start_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_utc = end_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    url = f"{base_url}?createdStartDate={start_date_utc}&createdEndDate={end_date_utc}&limit=100"
    
    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"📅 Fetching Walmart orders from {start_date} to {end_date} ({timezone})...")

    while True:
        paged_url = f"{base_url}{next_cursor}" if next_cursor else url
        
        response = requests.get(paged_url, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            new_orders = result.get('list', {}).get('elements', {}).get('order', [])
            
            if not new_orders:
                break
                
            fetched_orders.extend(new_orders)
            
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                break
                
            page += 1
            
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break

    # Apply the same filtering logic as fetchTodayWalmartOrdersCount
    filtered_orders = []
    cancelled_count = 0
    zero_value_count = 0
    
    for order in fetched_orders:
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        if not order_lines:
            continue
            
        all_cancelled = True
        order_total = 0
        
        for line in order_lines:
            line_statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            
            current_status = '' # Default value
            if line_statuses:
                first_status_entry = line_statuses[0]
                if isinstance(first_status_entry, dict):
                    status_value = first_status_entry.get('status', '')
                    if isinstance(status_value, dict):
                        current_status = status_value.get('status', '')
                    else:
                        current_status = str(status_value)
                else:
                    current_status = str(first_status_entry)
            
            if current_status.upper() not in ['CANCELLED', 'CANCELED']:
                all_cancelled = False
            # No 'else' here
            
            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    charge_amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                    quantity = int(line.get('orderLineQuantity', {}).get('amount', 0))
                    order_total += charge_amount * quantity
        
        if exclude_cancelled and all_cancelled:
            cancelled_count += 1
            continue
            
        if order_total <= min_order_value:
            zero_value_count += 1
            continue
            
        filtered_orders.append(order)

    # Count orders and order items
    total_orders = len(filtered_orders)
    total_order_items = 0
    total_revenue = 0
    
    for order in filtered_orders:
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        for line in order_lines:
            quantity = int(line.get('orderLineQuantity', {}).get('amount', 0))
            total_order_items += quantity
            
            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    charge_amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                    total_revenue += charge_amount * quantity
    
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    print(f"\n📊 Total Orders: {total_orders}")
    print(f"📦 Total Order Items: {total_order_items}")
    print(f"💰 Total Revenue: ${total_revenue:,.2f}")
    print(f"💵 Average Order Value: ${avg_order_value:,.2f}")
    
    return {
        "total_orders": total_orders,
        "total_order_items": total_order_items,
        "total_revenue": total_revenue,
        "average_order_value": avg_order_value,
        "filtered_orders": filtered_orders
    }


if __name__ == "__main__":
    fetchTodayWalmartOrdersCount()
