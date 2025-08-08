import requests
from ecommerce_tool.settings import SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET

def get_shipping_price_from_shipstation(order_number):
    try:
        # Step 1: Try to get shipment cost using orderNumber
        shipments_url = "https://ssapi.shipstation.com/shipments"
        response = requests.get(
            shipments_url,
            auth=(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET),
            params={"orderNumber": order_number, "pageSize": 1},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        print('📦 Shipment data:', data)
        shipments = data.get("shipments", [])
        if shipments:
            shipment_cost = shipments[0].get("shipmentCost", 0)
            print(f"✅ Found shipment with cost: ${shipment_cost}")
            return float(shipment_cost)

        print("🚫 No shipment found, trying orders endpoint...")

        # Step 2: Try to get customer shipping amount from order
        orders_url = "https://ssapi.shipstation.com/orders"
        response = requests.get(
            orders_url,
            auth=(SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET),
            params={"orderNumber": order_number, "pageSize": 1},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        print('📄 Order data:', data)
        orders = data.get("orders", [])
        if orders:
            amount = orders[0].get("shippingAmount", 0)
            print(f"✅ Found order with customer shipping: ${amount}")
            return float(amount)

        print(f"❌ No shipment or order found for orderNumber: {order_number}")
        return 0.0

    except requests.exceptions.ConnectionError as e:
        print(f"🔌 Connection error to ShipStation: {e}")
        return 0.0
    except requests.exceptions.Timeout:
        print(f"⏱️ ShipStation API timeout for order {order_number}")
        return 0.0
    except requests.exceptions.HTTPError as e:
        print(f"❗ ShipStation API HTTP error for order {order_number}: {e}")
        return 0.0
    except Exception as e:
        print(f"⚠️ Unexpected ShipStation error for order {order_number}: {e}")
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
