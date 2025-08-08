import requests
from ecommerce_tool.settings import SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET

def get_shipping_price_from_shipstation(order_number):
    try:
        
        shipments_url = "https://ssapi.shipstation.com/shipments/{order_number}``"
        response = requests.get(
            shipments_url,
            auth=(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET),
            params={"customField2": order_number, "pageSize": 1},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        print('data', data)
        shipments = data.get("shipments", [])
        if shipments:
            
            print("Available fields in shipment:", list(shipments[0].keys()))
            print("Looking for customField2 in shipment...")
            
            
            first_shipment = shipments[0]
            custom_field_2 = first_shipment.get("customField2")
            print(f"customField2 value: {custom_field_2}")
            
            if custom_field_2 == order_number:
                shipment_cost = first_shipment.get("shipmentCost", 0)
                print(f"✅ Found matching shipment with cost: ${shipment_cost}")
                return float(shipment_cost)
            else:
                print(f"customField2 '{custom_field_2}' doesn't match order '{order_number}'")
        
        print("No matching shipment found, trying orders endpoint...")

        
        orders_url = "https://ssapi.shipstation.com/orders"
        response = requests.get(
            orders_url,
            auth=(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET),
            params={"customField2": order_number, "pageSize": 1},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        orders = data.get("orders", [])
        if orders:
            shipping_amount = orders[0].get("shippingAmount", 0)
            print(f"✅ Found order with customer shipping: ${shipping_amount}")
            return float(shipping_amount)

        print(f"No shipment or order found with customField2: {order_number}")
        return 0.0

    except requests.exceptions.ConnectionError as e:
        if "Failed to resolve" in str(e):
            print(f" DNS/Network error - cannot reach ShipStation API: {e}")
        else:
            print(f"🔌 Connection error to ShipStation: {e}")
        return 0.0
    except requests.exceptions.Timeout:
        print(f" ShipStation API timeout for order {order_number}")
        return 0.0
    except requests.exceptions.HTTPError as e:
        print(f" ShipStation API HTTP error for order {order_number}: {e}")
        return 0.0
    except Exception as e:
        print(f"Unexpected ShipStation error for order {order_number}: {e}")
        return 0.0

def get_shipping_price(order, item_data):
    fulfillment_channel = order.get("fulfillment_channel", "").upper()
    shipping_price = float(item_data.get("shipping_price", 0) or 0)
    a_shipping_cost = float(item_data.get("a_shipping_cost", 0) or 0)
    w_shipping_cost = float(item_data.get("w_shipping_cost", 0) or 0)

    if fulfillment_channel == "FBM":
        if shipping_price == 0:
            order_number = order.get("purchase_order_id") or order.get("order_number")
            if order_number:
                shipping_price = get_shipping_price_from_shipstation(order_number)
        return shipping_price
    elif fulfillment_channel == "FBA":
        return a_shipping_cost
    else:
        return w_shipping_cost