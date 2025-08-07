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
    Fetches Walmart orders for today (US/Pacific) and displays count of orders and order items.
    Filters out cancelled orders and orders with total <= min_order_value.
    """
    access_token = oauthFunction()
    if not access_token:
        print("❌ Failed to get access token")
        return None

    # Get today's date range in US/Pacific
    pacific = pytz.timezone("US/Pacific")
    now_pacific = datetime.now(pacific)
    start_of_day = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = now_pacific.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Convert to UTC for API
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

    # Initial URL with date filters
    url = f"{base_url}?createdStartDate={start_date}&createdEndDate={end_date}&limit=100"
    
    fetched_orders = []
    next_cursor = None
    page = 1

    print(f"📦 Fetching Walmart orders for today")
    print(f"📅 Pacific Time: {start_of_day.strftime('%Y-%m-%d %H:%M:%S')} → {end_of_day.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 UTC Time: {start_date} → {end_date}")

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

    print(f"\n📊 Initial orders fetched: {len(fetched_orders)}")

    # Filter orders
    filtered_orders = []
    cancelled_count = 0
    zero_value_count = 0
    
    for order in fetched_orders:
        # Check if order is cancelled
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        
        # Skip if no order lines
        if not order_lines:
            continue
            
        # Check order status - skip if all lines are cancelled
        all_cancelled = True # Assume all lines are cancelled until proven otherwise
        order_total = 0
        
        for line in order_lines:
            # Get line status
            line_statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            
            current_status = '' # Default value if nothing found or invalid structure

            if line_statuses:
                first_status_entry = line_statuses[0]
                
                if isinstance(first_status_entry, dict):
                    # Attempt to get the status field. This might be a string or another dict.
                    status_value = first_status_entry.get('status', '') # Default to empty string if 'status' key is missing
                    
                    if isinstance(status_value, dict):
                        # If status_value is a dict, get the nested 'status'
                        current_status = status_value.get('status', '')
                    else:
                        # If status_value is already a string, use it directly
                        current_status = str(status_value)
                else:
                    # If first_status_entry itself is a string, use it directly
                    current_status = str(first_status_entry)
            
            # Now, current_status should always be a string.
            if current_status.upper() not in ['CANCELLED', 'CANCELED']:
                all_cancelled = False
            # No 'else' here, `all_cancelled` should only be set to False if a non-cancelled line is found.
            
            # Calculate line total
            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    charge_amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                    quantity = int(line.get('orderLineQuantity', {}).get('amount', 0))
                    order_total += charge_amount * quantity
        
        # Apply filters
        if exclude_cancelled and all_cancelled:
            cancelled_count += 1
            continue
            
        if order_total <= min_order_value:
            zero_value_count += 1
            continue
            
        filtered_orders.append(order)
    
    if exclude_cancelled:
        print(f"✅ Filtered out {cancelled_count} cancelled orders")
    if min_order_value > 0:
        print(f"✅ Filtered out {zero_value_count} orders with total <= ${min_order_value}")

    # Count filtered orders and items
    total_orders = len(filtered_orders)
    total_order_items = 0
    total_revenue = 0
    
    # Collect order details for summary
    order_status_count = {}
    
    for order in filtered_orders:
        order_lines = order.get('orderLines', {}).get('orderLine', [])
        
        for line in order_lines:
            # Count items
            quantity = int(line.get('orderLineQuantity', {}).get('amount', 0))
            total_order_items += quantity
            
            # Calculate revenue
            charges = line.get('charges', {}).get('charge', [])
            for charge in charges:
                if charge.get('chargeType') == 'PRODUCT':
                    charge_amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                    total_revenue += charge_amount * quantity
            
            # Track order status
            line_statuses = line.get('orderLineStatuses', {}).get('orderLineStatus', [])
            status = 'Unknown' # Default
            if line_statuses:
                first_status_entry = line_statuses[0]
                if isinstance(first_status_entry, dict):
                    status_value = first_status_entry.get('status', '')
                    if isinstance(status_value, dict):
                        status = status_value.get('status', 'Unknown')
                    else:
                        status = str(status_value)
                else:
                    status = str(first_status_entry)
            
            order_status_count[status] = order_status_count.get(status, 0) + 1
    
    # Calculate average order value
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0
    
    print(f"\n📅 Date: {now_pacific.strftime('%Y-%m-%d')} (US/Pacific)")
    print(f"📊 Total Orders Today: {total_orders}")
    print(f"📦 Total Order Items Today: {total_order_items}")
    print(f"💰 Total Revenue Today: ${total_revenue:,.2f}")
    print(f"💵 Average Order Value: ${avg_order_value:,.2f}")
    
    # Show order status breakdown
    if order_status_count:
        print(f"\n📊 Order Status Breakdown:")
        for status, count in order_status_count.items():
            print(f"  - {status}: {count} items")
    
    # Show sample orders if available
    if filtered_orders:
        print(f"\n🔍 Sample orders (first 5):")
        for i, order in enumerate(filtered_orders[:5]):
            order_date = datetime.fromtimestamp(int(order.get('orderDate', 0)) / 1000)
            order_date_pacific = pytz.utc.localize(order_date).astimezone(pacific)
            
            # Get order total for this order
            order_total = 0
            order_lines = order.get('orderLines', {}).get('orderLine', [])
            for line in order_lines:
                charges = line.get('charges', {}).get('charge', [])
                for charge in charges:
                    if charge.get('chargeType') == 'PRODUCT':
                        charge_amount = float(charge.get('chargeAmount', {}).get('amount', 0))
                        quantity = int(line.get('orderLineQuantity', {}).get('amount', 0))
                        order_total += charge_amount * quantity
            
            print(f"  Order {i+1}: {order.get('purchaseOrderId')} - {order_date_pacific.strftime('%Y-%m-%d %H:%M:%S PST')} - ${order_total:.2f}")
    else:
        print("\n⚠️  No orders found for today (after filtering)")
    
    return {
        "total_orders": total_orders,
        "total_order_items": total_order_items,
        "total_revenue": total_revenue,
        "average_order_value": avg_order_value,
        "filtered_orders": filtered_orders
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
    # Fetch order details
    getWalmartOrderDetails("129021990100140")
