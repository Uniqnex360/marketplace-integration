import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.walmart_utils import getAccesstoken
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId




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
        if total_items == 0:
            total_items = data.get("totalItems", 0)
    else:
        print(f"❌ Failed to Fetch Products. Status Code: {response.status_code}")
        print("Response:", response.text)
    data = {
        "total_items" : total_items,
        "all_products" : all_products
    }
    return data


def fetchProductDetails(request):
    data = dict()
    user_id = request.GET.get('user_id')
    sku = request.GET.get('sku')

    ACCESS_TOKEN = getAccesstoken(user_id)

    # Walmart API URL for product details
    PRODUCT_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/items/{sku}?include=images,attributes"

    # Headers for authentication
    headers = {
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  # Unique request ID
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }

    print(f"Fetching details for SKU: {sku}...")

    STOCK_URL = f"https://marketplace.walmartapis.com/v3/inventory/?sku={sku}"
    
    # Send GET request
    response = requests.get(PRODUCT_DETAILS_URL, headers=headers)
    # Headers for authentication
    headers1 = {
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  # Unique request ID
        "WM_SVC.NAME": "Walmart Marketplace",
        "Accept": "application/json"
    }
    response1 = requests.get(STOCK_URL, headers=headers1)

    if response.status_code == 200:
        data['product_details'] = response.json()
        print("Product details fetched successfully")
    else:
        print(f"❌ Failed to Fetch Product Details. Status Code: {response.status_code}")
        print("Response:", response.text)

    if response1.status_code == 200:
        data['stock_details'] = response1.json()
        print("Product details fetched successfully")
    else:
        print(f"❌ Failed to Fetch stock Details. Status Code: {response1.status_code}")
        print("Response:", response1.text)

    return data


def saveProductCategory(marketplace_id,name,level,parent_id):

    pipeline = [
    {"$match": {"name": name,
                "marketplace_id" : marketplace_id}},
    {
        "$project": {
            "_id": 1
        }
        },
        {
            "$limit" : 1
        }
    
    ]
    product_category_obj = list(Category.objects.aggregate(*pipeline))
    if product_category_obj != []:
        product_category_id = product_category_obj[0]['_id']
    if product_category_obj == []:
        product_category_obj = DatabaseModel.save_documents(
            Category, {
                "name": name,
                "level": level,
                "marketplace_id" : marketplace_id,
            }
        )
        product_category_id = product_category_obj.id
    if parent_id != None:
        DatabaseModel.update_documents(Category.objects,{"id" : product_category_id},{"parent_category_id" : ObjectId(parent_id)})
    return product_category_id



# Function to process the Excel file
def process_excel(file_path):
    df = pd.read_excel(file_path)
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')

    for index, row in df.iterrows():
        
        # Process the breadcrumb path
        if pd.notnull(row['shelf']):
            breadcrumb_path = eval(row['shelf'])  # Convert string representation of list to actual list
            parent_category = None

            for level, category_name in enumerate(breadcrumb_path):
                s=saveProductCategory(marketplace_id,category_name,level,parent_category)
                parent_category = s

            # The last level category
            last_level_category = breadcrumb_path[-1]
        else:
            last_level_category =""

        # Create the product
        price_data = eval(row['price'])  # Convert string representation of dict to actual dict
        print("111111111111111111111111",type(row['unpublishedReasons']))
        # Check if 'unpublishedReasons' is not NaN and change the format
        if pd.notnull(row['shelf']):
            shelf_path = ' > '.join(breadcrumb_path)
            print("3333333333333333333",shelf_path)
        else:
            shelf_path = ""

        product = Product(
            marketplace_id=marketplace_id,
            sku=str(row['sku']) if pd.notnull(row['sku']) else "",
            wpid=str(row['wpid']) if pd.notnull(row['wpid']) else "",
            upc=str(int(row['upc'])) if pd.notnull(row['upc']) else "",
            gtin=str(int(row['gtin'])) if pd.notnull(row['gtin']) else "",
            product_name=row['productName'] if pd.notnull(row['productName']) else "",
            category=last_level_category if pd.notnull(last_level_category) else "",
            shelf_path=shelf_path,
            product_type=row['productType'] if pd.notnull(row['productType']) else "",
            condition=row['condition'] if pd.notnull(row['condition']) else "",
            availability=row['availability'] if pd.notnull(row['availability']) else "",
            price=Price(currency=price_data['currency'], amount=price_data['amount']),
            published_status=row['publishedStatus'] if pd.notnull(row['publishedStatus']) else "",
            unpublished_reasons=row['unpublishedReasons'] if pd.notnull(row['unpublishedReasons']) else "",
            lifecycle_status=row['lifecycleStatus'] if pd.notnull(row['lifecycleStatus']) else "",
            is_duplicate=bool(row['isDuplicate']),
        )
        product.save()

# file_path = "/home/lexicon/walmart/Walmart-high-level-products.xlsx"
# process_excel(file_path)
