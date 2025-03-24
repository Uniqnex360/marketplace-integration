import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.walmart_utils import getAccesstoken, oauthFunction
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId
import json
from datetime import datetime, timedelta





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
    PRODUCT_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/items/{sku}?include=images,attributes,fulfillment,variants,productType"

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
        print("✅ Product details fetched successfully")
    else:
        print(f"❌ Failed to Fetch Product Details. Status Code: {response.status_code}")
        print("Response:", response.text)

    if response1.status_code == 200:
        data['stock_details'] = response1.json()
        print("✅Stock Details fetched successfully")
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
def process_excel_for_walmart(file_path):
    df = pd.read_excel(file_path)
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')

    for index, row in df.iterrows():
        print(f"Processing row {index + 1}...")
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
        # Check if 'unpublishedReasons' is not NaN and change the format
        if pd.notnull(row['shelf']):
            shelf_path = ' > '.join(breadcrumb_path)
        else:
            shelf_path = ""

        product = Product(
            marketplace_id=marketplace_id,
            sku=str(row['sku']) if pd.notnull(row['sku']) else "",
            product_id=str(row['wpid']) if pd.notnull(row['wpid']) else "",
            upc=str(int(row['upc'])) if pd.notnull(row['upc']) else "",
            gtin=str(int(row['gtin'])) if pd.notnull(row['gtin']) else "",
            product_title=row['productName'] if pd.notnull(row['productName']) else "",
            category=last_level_category if pd.notnull(last_level_category) else "",
            shelf_path=shelf_path,
            product_type=row['productType'] if pd.notnull(row['productType']) else "",
            item_condition=row['condition'] if pd.notnull(row['condition']) else "",
            availability=row['availability'] if pd.notnull(row['availability']) else "",
            price=price_data['amount'] if pd.notnull(price_data['amount']) else 0.0,
            currency=price_data['currency'] if pd.notnull(price_data['currency']) else "",
            published_status=row['publishedStatus'] if pd.notnull(row['publishedStatus']) else "",
            unpublished_reasons=row['unpublishedReasons'] if pd.notnull(row['unpublishedReasons']) else "",
            lifecycle_status=row['lifecycleStatus'] if pd.notnull(row['lifecycleStatus']) else "",
            is_duplicate=bool(row['isDuplicate']),
        )
        product.save()



# file_path1 = "/home/lexicon/walmart/Walmart-high-level-products.xlsx"
# process_excel_for_walmart(file_path1)



def fetchAllorders1(request):
    orders = []
    user_id = request.GET.get('user_id')
    access_token = getAccesstoken(user_id)
    limit = int(request.GET.get('limit', 100))  # Default limit = 100 if not provided
    skip = int(request.GET.get('skip', 0))  # Default skip = 0 if not provided
    
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,  
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
        "WM_SVC.NAME": "Walmart Marketplace",  
        "Accept": "application/json"
    }

    total_fetched = 0
    next_cursor = None  # Pagination cursor

    while total_fetched < (skip + limit):
        print(skip ,limit)
        # Construct the URL with cursor if available
        url = f"{base_url}?createdStartDate=2024-01-01T00:00:00Z&limit=100"
        if next_cursor:
            url = f"{base_url}{next_cursor}"  # Use the nextCursor provided by Walmart

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            result = response.json()
            fetched_orders = result.get('list', {}).get('elements', {}).get('order', [])
            total_fetched += len(fetched_orders)

            # Skip the first `skip` orders
            if total_fetched >= skip:
                orders.extend(fetched_orders)

            # Stop if we have enough orders
            if len(orders) >= limit:
                break

            # Get the nextCursor for pagination
            next_cursor = result.get("list", {}).get("meta", {}).get("nextCursor")
            if not next_cursor:
                break  # No more pages left
            
        else:
            print(f"❌ Error fetching orders: [HTTP {response.status_code}] {response.text}")
            break
    return orders[:limit]  # Return the exact number of requested orders



def fetchOrderDetails(request):
    data = dict()
    user_id = request.GET.get('user_id')
    purchase_order_id = request.GET.get('purchaseId')

    ACCESS_TOKEN = getAccesstoken(user_id)

    ORDER_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/orders/{purchase_order_id}"
    """
    Fetch order details from Walmart API using the generated access token and purchase order ID.
    """
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,  # Required token header
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  # Generate a unique UUID for tracking
        "WM_SVC.NAME": "Walmart Marketplace",  # Walmart Service Name
        "Accept": "application/json"
    }

    response = requests.get(ORDER_DETAILS_URL, headers=headers)

    if response.status_code == 200:
        order_details = response.json()
        data = order_details['order']
        print("✅ Order Details Fetched Successfully!")
    else:
        print(f"❌ Error fetching order details: [HTTP {response.status_code}] {response.text}")

    return data



def fetchBrand(request):
    data = dict()
    user_id = request.GET.get('user_id')
    sku = request.GET.get('sku')

    ACCESS_TOKEN = getAccesstoken(user_id)

    ORDER_DETAILS_URL = f"https://marketplace.walmartapis.com/v3/catalog/items/{sku}"
    """
    Fetch order details from Walmart API using the generated access token and purchase order ID.
    """
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "WM_SEC.ACCESS_TOKEN": ACCESS_TOKEN,  # Required token header
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  # Generate a unique UUID for tracking
        "WM_SVC.NAME": "Walmart Marketplace",  # Walmart Service Name
        "Accept": "application/json",
        "Content-Type": "application/json"

    }

    response = requests.get(ORDER_DETAILS_URL, headers=headers)

    if response.status_code == 200:
        order_details = response.json()
        data = order_details
        print("✅ Order Details Fetched Successfully!")
    else:
        print(f"❌ Error fetching order details: [HTTP {response.status_code}] {response.text}")

    return data


# Function to process the Excel file
def process_excel_for_walmartorders(file_path):
    df = pd.read_excel(file_path)
    marketplace = "Walmart"
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')

    for index, row in df.iterrows():
        # print(f"Processing row {index + 1}...",row)
        print(f"Processing row {index}...")
        shipNode = eval(row['shipNode']) if pd.notnull(row['shipNode']) else {}
        order_details = eval(row['orderLines']) if pd.notnull(row['orderLines']) else []
        order_total = 0
        currency = "USD"
        order_status = ""
        for order_line_ins in order_details['orderLine']:
            for charge_ins in order_line_ins['charges']['charge']:
                tax =0
                if charge_ins['tax'] != None:
                    tax = float(charge_ins['tax']['taxAmount']['amount'])
                order_total += float(charge_ins['chargeAmount']['amount']) + tax
                currency = charge_ins['chargeAmount']['currency']

            order_status = order_line_ins['orderLineStatuses']['orderLineStatus'][0]['status']

        order_date = row['orderDate'] if pd.notnull(row['orderDate']) else ""
        if order_date != "":
            order_date = datetime.fromtimestamp(int(order_date)/1000)
        

        order_obj = DatabaseModel.get_document(Order.objects,{"purchase_order_id" : str(row['purchaseOrderId'])})
        if order_obj != None:
            print(f"Order with purchase order ID {row['purchaseOrderId']} already exists. Skipping...")
            DatabaseModel.update_documents(Order.objects,{"purchase_order_id" : str(row['purchaseOrderId'])},{"order_status" : order_status,"currency" : currency,"order_total" : order_total})     
            
        else:
            print(f"Creating order with purchase order ID {row['purchaseOrderId']}...")
            order = Order(
                marketplace_id=marketplace_id,
                purchase_order_id=str(row['purchaseOrderId']) if pd.notnull(row['purchaseOrderId']) else "",
                customer_order_id=str(row['customerOrderId']) if pd.notnull(row['customerOrderId']) else "",
                customer_email_id=str(row['customerEmailId']) if pd.notnull(row['customerEmailId']) else "",
                order_date = order_date,
                shipping_information = eval(row['shippingInfo']) if pd.notnull(row['shippingInfo']) else "",
                fulfillment_channel = shipNode['type'],
                order_details = order_details['orderLine'],
                order_total = order_total,
                currency = currency,
                order_status = order_status,
            )
            order.save()



file_path1 = "/home/lexicon/walmart/WALMARTORDER@orders.xlsx"
# process_excel_for_walmartorders(file_path1)



def saveBrand(marketplace_id,name):
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
    brand_obj = list(Brand.objects.aggregate(*pipeline))
    if brand_obj != []:
        brand_id = brand_obj[0]['_id']
    if brand_obj == []:
        brand_obj = DatabaseModel.save_documents(
            Brand, {
                "name": name,
                "marketplace_id" : marketplace_id,
            }
        )
        brand_id = brand_obj.id
    return brand_id


def update_product_images_from_csv(file_path):
    df = pd.read_csv(file_path)
    marketplace_id = ObjectId('67c9460fa5194f500892c0d2')

    for index, row in df.iterrows():
        print(f"Processing row {index + 1}...")
        sku = str(row['SKU']) if pd.notnull(row['SKU']) else ""
        quantity = row['Input Quantity'] if pd.notnull(row['Input Quantity']) else ""
        # try:
        #     brand_name = brand_name = row['Brand'] if pd.notnull(row['Brand']) else ""
        #     brand_id = saveBrand(marketplace_id,brand_name)
        # except:
        #     brand_name = ""
        #     brand_id = None

        if sku and quantity:
            product = DatabaseModel.get_document(Product.objects, {"sku": sku, "marketplace_id": marketplace_id})
            if product:
                # product.image_url = image_url
                # product.brand_id = brand_id
                # product.brand_name = brand_name
                product.quantity = quantity
                product.save()
                print(f"✅ Updated image for SKU: {sku}")
            else:
                print(f"❌ Product with SKU: {sku} not found")
        else:
            print(f"❌ Invalid data in row {index + 1}")

# # Example usage
# file_path = "/home/lexicon/walmart/InventoryReport_10001414684_2025-03-13T032956.541000.csv"
# update_product_images_from_csv(file_path)


from datetime import datetime

def process_walmart_order(json_data):
    """Processes a single Walmart order item and saves it to the OrderItems collection."""
    try:
        product = DatabaseModel.get_document(Product.objects, {"product_title": json_data.get("item", {}).get("productName", "")}, ["id"])
        product_id = product.id if product else None
    except:
        product_id = None
    try:
        product_price = {
            "CurrencyCode": json_data['charges']['charge'][0]['chargeAmount']['currency'],
            "Amount": float(json_data['charges']['charge'][0]['chargeAmount']['amount'])
        }
    except:
        product_price = {
            "CurrencyCode": "USD",
            "Amount": 0.0
        }

    try:
        tax_price ={
            "CurrencyCode": json_data['charges']['charge'][0]['tax']['taxAmount']['currency'],
            "Amount": float(json_data['charges']['charge'][0]['tax']['taxAmount']['amount'])
        }
    except:
        tax_price =  {
            "CurrencyCode": "USD",
            "Amount": 0.0
        }

    # Extract necessary fields safely
    order_line_statuses = json_data.get("orderLineStatuses", {}).get("orderLineStatus", [])
    order_line_status = order_line_statuses[0] if order_line_statuses else {}

    tracking_info = order_line_status.get("trackingInfo", {})

    order_item = OrderItems(
        OrderId=json_data.get("lineNumber", ""),
        Platform="Walmart",
        ProductDetails=ProductDetails(
            product_id= product_id,
            Title=json_data.get("item", {}).get("productName", "Unknown Product"),
            SKU=json_data.get("item", {}).get("sku", "Unknown SKU"),
            Condition=json_data.get("item", {}).get("condition", "Unknown Condition"),
            QuantityOrdered=int(json_data.get("orderLineQuantity", {}).get("amount", 0)),
            QuantityShipped=int(order_line_status.get("statusQuantity", {}).get("amount", 0)),
        ),
        Pricing=Pricing(
            ItemPrice=Money(**product_price),
            ItemTax=Money(**tax_price)
        ),
        Fulfillment=Fulfillment(
            FulfillmentOption=json_data.get("fulfillment", {}).get("fulfillmentOption", "Unknown"),
            ShipMethod=json_data.get("fulfillment", {}).get("shipMethod", "Unknown"),
            Carrier=tracking_info.get("carrierName", {}).get("carrier", "Unknown"),
            TrackingNumber=tracking_info.get("trackingNumber", "Unknown"),
            TrackingURL=tracking_info.get("trackingURL", "Unknown"),
            ShipDateTime=datetime.fromtimestamp(tracking_info.get("shipDateTime", 0) / 1000) if tracking_info.get("shipDateTime") else None
        ),
        OrderStatus=OrderStatus(
            Status=order_line_status.get("status", "Unknown"),
            StatusDate=datetime.fromtimestamp(json_data.get("statusDate", 0) / 1000) if json_data.get("statusDate") else None
        ),
        TaxCollection=TaxCollection(
            Model="MarketplaceFacilitator",
            ResponsibleParty="Walmart"
        ),
        IsGift=False,
        BuyerInfo=None
    )

    order_item.save()  # Save to MongoDB

    return order_item  # Return reference to saved OrderItems document



def updateOrdersItemsDetails(request):
    """Updates order items details for Walmart orders in the database."""
    marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Walmart"}).id
    order_list = DatabaseModel.list_documents(Order.objects, {"marketplace_id": marketplace_id}, ["id", "order_details"])

    for ins in order_list:
        order_items = []
        for item in ins.order_details:
            order_item = process_walmart_order(item)
            order_items.append(order_item)

        DatabaseModel.update_documents(Order.objects, {"id": ins.id}, {"order_items": order_items})

    return True




def syncRecentWalmartOrders():
    orders = []
    access_token = oauthFunction()
    marketplace_id = DatabaseModel.get_document(Marketplace.objects,{'name' : "Walmart"},['id']).id
    
    base_url = "https://marketplace.walmartapis.com/v3/orders"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "WM_SEC.ACCESS_TOKEN": access_token,  
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  
        "WM_SVC.NAME": "Walmart Marketplace",  
        "Accept": "application/json"
    }
    # Get today's date
    today = datetime.utcnow()
    # Fetch last 30 days of orders
    start_date = (today - timedelta(days=180)).strftime("%Y-%m-%dT00:00:00Z")
    end_date = today.strftime("%Y-%m-%dT23:59:59Z")

    url = f"{base_url}?createdStartDate={start_date}&createdEndDate={end_date}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        fetched_orders = result.get('list', {}).get('elements', {}).get('order', [])
        # total_fetched += len(fetched_orders)

       
        orders.extend(fetched_orders)
        for row in orders:
            order_obj = DatabaseModel.get_document(Order.objects, {"purchase_order_id": str(row.get('purchaseOrderId', ""))})
            if order_obj is not None:
                print(f"Order with purchase order ID {row['purchaseOrderId']} already exists. Skipping...")
            else:
                print(f"Creating order with purchase order ID {row['purchaseOrderId']}...")
                order_items = list()
                shipNode = eval(str(row['shipNode'])) if row.get('shipNode') else {}
                order_details = eval(str(row['orderLines'])) if row.get('orderLines') else []
                order_total = 0
                currency = "USD"
                order_status = ""
                for order_line_ins in order_details.get('orderLine', []):
                    for charge_ins in order_line_ins.get('charges', {}).get('charge', []):
                        tax = 0
                        if charge_ins.get('tax') is not None:
                            tax = float(charge_ins['tax']['taxAmount']['amount'])
                        order_total += float(charge_ins['chargeAmount']['amount']) + tax
                        currency = charge_ins['chargeAmount']['currency']
                    order_items.append(process_walmart_order(order_line_ins))

                order_status = order_line_ins.get('orderLineStatuses', {}).get('orderLineStatus', [{}])[0].get('status', "")

                order_date = row.get('orderDate', "")
                if order_date:
                    order_date = datetime.fromtimestamp(int(order_date) / 1000)
                order = Order(
                    marketplace_id=marketplace_id,
                    purchase_order_id=str(row.get('purchaseOrderId', "")),
                    customer_order_id=str(row.get('customerOrderId', "")),
                    customer_email_id=str(row.get('customerEmailId', "")),
                    order_date=order_date,
                    shipping_information=eval(row['shippingInfo']) if row.get('shippingInfo') else "",
                    fulfillment_channel=shipNode.get('type', ""),
                    order_details=order_details.get('orderLine', []),
                    order_items = order_items,
                    order_total=order_total,
                    currency=currency,
                    order_status=order_status,
                )
                order.save()
    return orders