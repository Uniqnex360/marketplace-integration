import requests
from django.conf import settings

def get_shipping_price_from_shipstation(order_number):
    import requests
    from django.conf import settings
    url = f"https://ssapi.shipstation.com/orders?orderNumber={order_number}"
    try:
        response = requests.get(
            url,
            auth=(settings.SHIPSTATION_API_KEY, settings.SHIPSTATION_API_SECRET),
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        orders = data.get("orders", [])
        if not orders:
            return 0.0
        return float(orders[0].get("shippingAmount", 0.0))
    except Exception as e:
        print(f"ShipStation error for order {order_number}: {e}")
        return 0.0

def get_shipping_price(order, item_data):
    fulfillment_channel = order.get("fulfillment_channel", "").upper()
    shipping_price = float(item_data.get("shipping_price", 0) or 0)
    a_shipping_cost = float(item_data.get("a_shipping_cost", 0) or 0)
    w_shiping_cost = float(item_data.get("w_shiping_cost", 0) or 0)

    if fulfillment_channel == "FBM":
        if shipping_price == 0:
            order_number = order.get("purchase_order_id") or order.get("order_number")
            if order_number:
                shipping_price = get_shipping_price_from_shipstation(order_number)
        return shipping_price
    elif fulfillment_channel == "FBA":
        return a_shipping_cost
    else:
        return w_shiping_cost
