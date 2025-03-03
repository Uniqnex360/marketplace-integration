import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.walmart_utils import getAccesstoken


# Function to fetch all products with pagination
def fetchAllProducts(request):
    data = dict()
    user_id = request.GET.get('user_id')
    offset = int(request.GET.get('skip'))
    limit = int(request.GET.get('limit'))

    ACCESS_TOKEN = getAccesstoken(user_id)

    # Walmart API Base URL
    ALL_PRODUCTS_URL = "https://marketplace.walmartapis.com/v3/items"

    # Headers for authentication
    headers = {
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  # Unique request ID
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }
    all_products = []
    total_items = 0

   
    print("Fetching Products...")
    # Build the request URL with offset pagination
    url = f"{ALL_PRODUCTS_URL}?limit={limit}&offset={offset}"

    # Send GET request
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()

        # Append items to list
        all_products.extend(data.get("ItemResponse", []))
        print(len(all_products), "products fetched")

        # Get total items (first response only)
        if total_items is None:
            total_items = data.get("totalItems", 0)
    else:
        print(f"❌ Failed to Fetch Products. Status Code: {response.status_code}")
        print("Response:", response.text)
    data = {
        "total_items" : total_items,
        "all_products" : all_products
    }
    return data