import requests
import base64
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from ecommerce_tool.settings import AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN, SELLER_ID
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import access_token, Marketplace
from datetime import datetime, timedelta
from bson import ObjectId
import json


def get_access_token():
    TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    """Retrieve access token using refresh token."""
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": AMAZON_API_KEY,
        "client_secret": AMAZON_SECRET_KEY
    }
    
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(TOKEN_URL, data=payload, headers=headers)
    
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print("Error getting access token:", response.text)
        return None
    

def getAccesstoken(user_id):
    marketplace_id = DatabaseModel.get_document(Marketplace.objects,{"name" : "Amazon"},['id']).id
    exist_access_token_obj = DatabaseModel.get_document(access_token.objects,{"user_id" : user_id,"marketplace_id" : marketplace_id},['access_token_str','updation_time'])
    if exist_access_token_obj != None:
        # Get the current time
        current_time = datetime.now()

        # Get the creation time of the access token
        creation_time = exist_access_token_obj.updation_time

        # Check if the current time is greater than the creation time plus 14 minutes
        if current_time < creation_time + timedelta(minutes=59):
            access_token_str = exist_access_token_obj.access_token_str
        else:
            access_token_str = get_access_token()
            if access_token_str != None:
                DatabaseModel.update_documents(access_token.objects,{"id" : exist_access_token_obj.id},{"access_token_str" : access_token_str,"updation_time" : datetime.now()})
    else:
        access_token_str = get_access_token()
        DatabaseModel.save_documents(access_token,{"user_id" : ObjectId(user_id),"access_token_str" : access_token_str,"marketplace_id" : marketplace_id})
    return access_token_str


# import xml.etree.ElementTree as ET

# def generate_product_update_xml(sku, title):
#     # Create XML structure
#     envelope = ET.Element("AmazonEnvelope")
#     envelope.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
#     envelope.set("xsi:noNamespaceSchemaLocation", "amzn-envelope.xsd")

#     header = ET.SubElement(envelope, "Header")
#     ET.SubElement(header, "DocumentVersion").text = "1.01"
#     ET.SubElement(header, "MerchantIdentifier").text = "YOUR_MERCHANT_ID"

#     ET.SubElement(envelope, "MessageType").text = "Product"

#     message = ET.SubElement(envelope, "Message")
#     ET.SubElement(message, "MessageID").text = "1"

#     product = ET.SubElement(message, "Product")
#     ET.SubElement(product, "SKU").text = sku

#     desc_data = ET.SubElement(product, "DescriptionData")
#     ET.SubElement(desc_data, "Title").text = title

#     # Convert XML to string
#     xml_str = ET.tostring(envelope, encoding="utf-8", method="xml").decode()
#     return xml_str

# # Example usage
# sku = "1100"
# title = "COVERGIRL Exhibitionist Lipstick Metallic, Rendezvous 535, 0.123 OunceS"

# xml_content = generate_product_update_xml(sku, title)

# # Save XML to a file
# with open("product_update.xml", "w") as f:
#     f.write(xml_content)

# print("XML file generated successfully!")





# # Your Amazon SP-API credentials
# ACCESS_TOKEN = get_access_token()
# BASE_URL = "https://sellingpartnerapi-na.amazon.com"

# headers = {
#     "x-amz-access-token": ACCESS_TOKEN,
#     "Content-Type": "application/json"
# }

# # Step 1: Request a feed document
# def create_feed_document():
#     url = f"{BASE_URL}/feeds/2021-06-30/documents"
#     payload = {"contentType": "text/xml; charset=UTF-8"}
    
#     response = requests.post(url, headers=headers, json=payload)
#     response_data = response.json()

#     if "feedDocumentId" in response_data and "url" in response_data:
#         print("1111111111111111111111111111111111111111")
#         return response_data["feedDocumentId"], response_data["url"]
#     else:
#         print("Error creating feed document:", response_data)
#         return None, None

# feed_document_id, upload_url = create_feed_document()
# print("feed_document_id..............",feed_document_id)
# print("upload_url..............",upload_url)

# feed_document_id.............. amzn1.tortuga.4.na.9287ed4d-35ce-4ac0-8338-8c4f927bad94.T3MJ6UXMVBOTHP
# upload_url.............. https://tortuga-prod-na.s3-external-1.amazonaws.com/9287ed4d-35ce-4ac0-8338-8c4f927bad94.amzn1.tortuga.4.na.T3MJ6UXMVBOTHP?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20250317T121133Z&X-Amz-SignedHeaders=content-type%3Bhost&X-Amz-Expires=300&X-Amz-Credential=AKIA5U6MO6RABSBDCMUJ%2F20250317%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=f61a1c41dbc79bcd172e0b50924fdf8c6d190b97290a5de360b08320c0480658


# file_path = "product_update.xml"


# headers = {
#     "Content-Type": "text/xml"  # Correct Content-Type
# }

# with open(file_path, "rb") as f:
#     response = requests.put(upload_url, data=f, headers=headers)

# if response.status_code == 200:
#     print("✅ File uploaded successfully!")
# else:
#     print(f"❌ File upload failed: {response.text}")






# def submit_feed(feed_document_id):
#     url = f"{BASE_URL}/feeds/2021-06-30/feeds"
    
#     payload = {
#         "feedType": "POST_PRODUCT_DATA",
#         "marketplaceIds": ["ATVPDKIKX0DER"],  # Replace with your marketplace ID
#         "inputFeedDocumentId": feed_document_id
#     }

#     response = requests.post(url, headers=headers, json=payload)
#     response_data = response.json()

#     if "feedId" in response_data:
#         return response_data["feedId"]
#     else:
#         print("Error submitting feed:", response_data)
#         return None

# feed_id = submit_feed(feed_document_id)

# if feed_id:
#     print("Feed submitted successfully! Feed ID:", feed_id)
#     import time

#     def check_feed_status(feed_id):
#         url = f"{BASE_URL}/feeds/2021-06-30/feeds/{feed_id}"

#         while True:
#             response = requests.get(url, headers=headers)
#             response_data = response.json()

#             if "processingStatus" in response_data:
#                 status = response_data["processingStatus"]
#                 print(f"Feed Status: {status}")

#                 if status in ["DONE", "CANCELLED", "DONE_NO_DATA"]:
#                     break  # Stop checking once processing is complete
#             else:
#                 print("Error fetching feed status:", response_data)
#                 break

#             time.sleep(10)  # Wait 10 seconds before checking again

#     check_feed_status(feed_id)





#     def get_feed_document(feed_id):
#         url = f"{BASE_URL}/feeds/2021-06-30/feeds/{feed_id}"
#         response = requests.get(url, headers=headers)
#         response_data = response.json()

#         if "resultFeedDocumentId" in response_data:
#             report_id = response_data["resultFeedDocumentId"]

#             report_url = f"{BASE_URL}/feeds/2021-06-30/documents/{report_id}"
#             report_response = requests.get(report_url, headers=headers)
#             report_data = report_response.json()

#             if "url" in report_data:
#                 print("Download Report:", report_data["url"])
#             else:
#                 print("Error fetching report:", report_data)
#         else:
#             print("No error report available.")

#     get_feed_document(feed_id)


# import requests

# ACCESS_TOKEN = get_access_token()  

# def get_marketplace_ids():
#     url = "https://sellingpartnerapi-na.amazon.com/sellers/v1/marketplaceParticipations"
#     headers = {
#         "x-amz-access-token": ACCESS_TOKEN,
#         "Content-Type": "application/json",
#         "Accept": "application/json"
#     }
    
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         marketplaces = response.json()
#         print("✅ Available Marketplaces:", marketplaces)
#     else:
#         print("❌ Error fetching marketplaces:", response.text)


# import requests
# import json


# # Valid Amazon marketplace IDs
# MARKETPLACE_IDS = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2", "A1AM78C64UM0Y8", "A2Q3Y263D00KWC"]

# # ✅ Ensure marketplace IDs are correct
# MARKETPLACE_IDS = ["ATVPDKIKX0DER"]  # Start with only the US marketplace

# def update_inventory(sku, quantity):
#     headers = {
#         "x-amz-access-token": ACCESS_TOKEN,
#         "Content-Type": "application/json",
#         "Accept": "application/json"
#     }
#     listing= f"https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/items/{SELLER_ID}/{sku}"
#     response1 = requests.get(listing, headers=headers)
#     print(response1.json())

#     for marketplace_id in MARKETPLACE_IDS:
#         url = f"https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/items/{SELLER_ID}/{sku}"

#         payload = {
#             "marketplaceIds": [marketplace_id],  # ✅ Correct format
#             "productType": "PRODUCT",
#             "attributes": {
#                 "fulfillmentAvailability": [
#                     {
#                         "fulfillmentChannelCode": "MFN",  # ✅ Ensure product is MFN
#                         "quantity": quantity
#                     }
#                 ]
#             }
#         }

#         response = requests.put(url, headers=headers, json=payload)

#         if response.status_code in [200, 202]:
#             print(f"✅ Inventory updated for SKU: {sku} in {marketplace_id}")
#         else:
#             print(f"❌ Failed to update {sku} in {marketplace_id}: {response.text}")

# # Example Usage
# update_inventory("1100", 10)



# import requests

# MARKETPLACE_ID = "ATVPDKIKX0DER"  # Amazon US


# def update_product_title(sku, new_title):
#     url = f"https://sellingpartnerapi-na.amazon.com/listings/2021-08-01/items/{SELLER_ID}/{sku}"

#     headers = {
#         "x-amz-access-token": ACCESS_TOKEN,
#         "Content-Type": "application/json",
#         "Accept": "application/json"
#     }

#     # JSON payload with updated title
#     payload = {
#         "productType": "PRODUCT",  # Ensure this matches your product category
#         "attributes": {
#             "title": new_title
#         },
#         "marketplaceIds": [MARKETPLACE_ID]
#     }

#     # Send the request
#     response = requests.put(url, headers=headers, json=payload)

#     # Handle response
#     if response.status_code in [200, 202]:
#         print(f"✅ Title updated for SKU: {sku}")
#     else:
#         print(f"❌ Failed to update title for SKU {sku}: {response.text}")

# # Example usage
# # update_product_title("AVE106-16", "COVERGIRL Exhibitionist Lipstick Metallic, Rendezvous 535, 0.123 Ounce11")


# def get_order(order_id, access_token, region):
#     base_url = f"https://sellingpartnerapi-{region}.amazon.com/orders/v0/orders/{order_id}"
#     headers = {
#         "x-amz-access-token": access_token,
#         "Content-Type": "application/json",
#     }
    
#     response = requests.get(base_url, headers=headers)
    
#     if response.status_code == 200:
#         order_data = response.json()
#         return order_data
#     else:
#         print(f"Error {response.status_code}: {response.text}")
#         return None

# ORDER_ID = "114-5600414-0798653"  # Replace with your actual order ID
# ACCESS_TOKEN = get_access_token()  # Replace with your valid SP-API access token
# REGION = "na"  # Change based on your region (na, eu, fe)

# order_details = get_order(ORDER_ID, ACCESS_TOKEN, REGION)
# if order_details:
#     print(json.dumps(order_details, indent=4))
#     tracking_info = order_details.get("payload", {}).get("FulfillmentShipments", [])
#     if tracking_info:
#         print("\n✅ Tracking Information:")
#         for shipment in tracking_info:
#             print(f"Tracking ID: {shipment.get('TrackingNumber', 'N/A')}")
#             print(f"Carrier: {shipment.get('CarrierCode', 'N/A')}")
#     else:
#         print("\n❌ No tracking information found.")