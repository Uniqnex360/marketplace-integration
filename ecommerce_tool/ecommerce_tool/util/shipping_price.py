
import requests
import json
from ecommerce_tool.settings import SHIPSTATION_API_KEY, SHIPSTATION_API_SECRET

def get_full_order_and_shipping_details(order_number):
    order_url = f"https://ssapi.shipstation.com/orders?orderNumber={order_number}"
    shipment_url = f"https://ssapi.shipstation.com/shipments?orderNumber={order_number}"
    
    auth = (SHIPSTATION_API_KEY,SHIPSTATION_API_SECRET)

    try:
        # Fetch order details
        order_resp = requests.get(order_url, auth=auth, timeout=30)
        order_resp.raise_for_status()
        order_data = order_resp.json()
        order = order_data.get("orders", [None])[0]
        if not order:
            print(f"No order found for order number {order_number}")
            return None

        # Fetch shipment details
        shipment_resp = requests.get(shipment_url, auth=auth, timeout=30)
        shipment_resp.raise_for_status()
        shipment_data = shipment_resp.json()
        shipments = shipment_data.get("shipments", [])
        
        # Add shipmentCost(s) to result
        shipment_costs = []
        for shipment in shipments:
            shipment_costs.append({
                "shipmentId": shipment.get("shipmentId"),
                "carrierCode": shipment.get("carrierCode"),
                "serviceCode": shipment.get("serviceCode"),
                "shipmentCost": shipment.get("shipmentCost"),
                "shipDate": shipment.get("shipDate"),
                "trackingNumber": shipment.get("trackingNumber"),
            })

        # Attach to the order object for unified view
        order["shipments"] = shipment_costs

        return order

    except Exception as e:
        print(f"Error fetching details for order {order_number}: {e}")
        return None
