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
        "OrderStatuses": ["Shipped", "Unshipped", "PartiallyShipped"],
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

def get_amazon_orders_current_month():
    """
    Fetches Amazon orders from the 1st of current month until today.
    Updates existing orders with new data if they already exist.
    Returns DataFrame of processed orders.
    """
    # Calculate date range with 3 minute buffer for Amazon API
    today = datetime.utcnow() - timedelta(minutes=3)
    first_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    created_after = first_of_month.strftime('%Y-%m-%dT%H:%M:%SZ')
    created_before = today.strftime('%Y-%m-%dT%H:%M:%SZ')

    print(f"📦 Fetching orders from {created_after} to {created_before}")
    orders = fetch_orders_from_amazon(created_after, created_before)

    if orders is None:
        print("⚠️ No data returned due to fetch error.")
        return pd.DataFrame()

    df = pd.DataFrame(orders)
    
    # Get marketplace
    marketplace = Marketplace.objects(name="Amazon").first()
    if not marketplace:
        print("❌ Amazon marketplace not found in database")
        return df
    
    # Process each order with sanitization
    for _, order_data in df.iterrows():
        try:
            # Helper function for date conversion with sanitization
            def convert_date(date_str):
                try:
                    date_str = sanitize_value(date_str, value_type=str)
                    return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ') if date_str else None
                except (ValueError, TypeError):
                    return None

            # Process order total with sanitization
            order_total_data = sanitize_value(order_data.get('OrderTotal'), default={}, value_type=dict)
            order_total = sanitize_value(order_total_data.get('Amount'), default=0.0, value_type=float)
            currency = sanitize_value(order_total_data.get('CurrencyCode'), default="USD", value_type=str)

            # Check for existing order
            order_id = sanitize_value(order_data['AmazonOrderId'], value_type=str)
            existing_order = Order.objects(purchase_order_id=order_id).first()
            
            # Build order updates with sanitized values
            order_updates = {
                'order_date': convert_date(order_data.get('PurchaseDate')),
                'earliest_ship_date': convert_date(order_data.get('EarliestShipDate')),
                'latest_ship_date': convert_date(order_data.get('LatestShipDate')),
                'last_update_date': convert_date(order_data.get('LastUpdateDate')),
                'order_status': sanitize_value(order_data.get('OrderStatus'), default="", value_type=str),
                'fulfillment_channel': sanitize_value(order_data.get('FulfillmentChannel'), default="", value_type=str),
                'sales_channel': sanitize_value(order_data.get('SalesChannel'), default="", value_type=str),
                'order_type': sanitize_value(order_data.get('OrderType'), default="", value_type=str),
                'number_of_items_shipped': sanitize_value(order_data.get('NumberOfItemsShipped'), default=0, value_type=int),
                'number_of_items_unshipped': sanitize_value(order_data.get('NumberOfItemsUnshipped'), default=0, value_type=int),
                'payment_method': sanitize_value(order_data.get('PaymentMethod'), default="", value_type=str),
                'payment_method_details': sanitize_value(order_data.get('PaymentMethodDetails', [''])[0], default="", value_type=str),
                'is_prime': sanitize_value(order_data.get('IsPrime'), default=False, value_type=bool),
                'is_business_order': sanitize_value(order_data.get('IsBusinessOrder'), default=False, value_type=bool),
                'is_premium_order': sanitize_value(order_data.get('IsPremiumOrder'), default=False, value_type=bool),
                'shipping_information': sanitize_value(order_data.get('ShippingAddress'), default={}, value_type=dict),
                'order_total': order_total,
                'currency': currency,
            }

            if existing_order:
                # Update existing order
                existing_order.update(**order_updates)
                print(f"🔄 Updated order {order_id}")
            else:
                # Create new order with additional required fields
                order_updates.update({
                    'marketplace_id': marketplace.id,
                    'purchase_order_id': order_id,
                    'created_at': datetime.utcnow()
                })
                Order(**order_updates).save()
                print(f"✅ Created new order {order_id}")

        except Exception as e:
            print(f"❌ Error processing order {order_data.get('AmazonOrderId')}: {str(e)}")
            continue

    return df

if __name__ == "__main__":
    # First sync current month's orders to database
    get_amazon_orders_current_month()
    
    # Then export last week's orders to Excel files