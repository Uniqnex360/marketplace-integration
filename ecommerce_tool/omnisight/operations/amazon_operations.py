import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.amazon_utils import getAccesstoken, get_access_token
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId
import json
from ecommerce_tool.settings import MARKETPLACE_ID
import ast
from datetime import datetime, timedelta



def process_excel_for_amazon(file_path):
    df = pd.read_excel(file_path)
    marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

    for index, row in df.iterrows():
        print(f"Processing row {index + 1}...")
        try:
            price=float(row['price']) if pd.notnull(row['price']) else 0.0
        except:
            price = 0.0

        try:
            quantity=int(row['quantity']) if pd.notnull(row['quantity']) else 0
        except:
            quantity = 0
        # Create the product
        try:
            open_date = pd.to_datetime(row['open-date']) if pd.notnull(row['open-date']) else None
        except:
            open_date = None
        
        product = Product(
            marketplace_id=marketplace_id,
            sku=str(row['seller-sku']) if pd.notnull(row['seller-sku']) else "",
            product_title=row['item-name'] if pd.notnull(row['item-name']) else "",
            product_description=str(row['item-description']) if pd.notnull(row['item-description']) else "",
            price=price,
            quantity=quantity,
            open_date=open_date,
            image_url=str(row['image-url']) if pd.notnull(row['image-url']) else "",
            zshop_shipping_fee=str(row['zshop-shipping-fee']) if pd.notnull(row['zshop-shipping-fee']) else "",
            item_note=str(row['item-note']) if pd.notnull(row['item-note']) else "",
            item_condition=str(row['item-condition']) if pd.notnull(row['item-condition']) else "",
            zshop_category=str(row['zshop-category1']) if pd.notnull(row['zshop-category1']) else "",
            zshop_browse_path=str(row['zshop-browse-path']) if pd.notnull(row['zshop-browse-path']) else "",
            zshop_storefront_feature=row['zshop-storefront-feature'] if pd.notnull(row['zshop-storefront-feature']) else "",
            asin=str(row['asin1']) if pd.notnull(row['asin1']) else (str(row['asin2']) if pd.notnull(row['asin2']) else (str(row['asin3']) if pd.notnull(row['asin3']) else "")),
            will_ship_internationally=bool(row['will-ship-internationally']),
            expedited_shipping=bool(row['expedited-shipping']),
            zshop_boldface=bool(row['zshop-boldface']),
            product_id=str(row['product-id']) if pd.notnull(row['product-id']) else "",
            bid_for_featured_placement=row['bid-for-featured-placement'] if pd.notnull(row['bid-for-featured-placement']) else "",
            add_delete=row['add-delete'] if pd.notnull(row['add-delete']) else "",
            pending_quantity=0,
            delivery_partner=str(row['fulfillment-channel']) if pd.notnull(row['fulfillment-channel']) else "",
            published_status=str(row['status']) if pd.notnull(row['status']) else "",
        )
        product.save()

file_path2 = "/home/lexicon/Documents/newww.xlsx"
# process_excel_for_amazon(file_path2)



def new_process_excel_for_amazon(file_path):
    df = pd.read_excel(file_path)
    marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

    for index, row in df.iterrows():
        print(f"Processing row {index}...{row['asin1']}")
        # try:
        #     price=float(row['price']) if pd.notnull(row['price']) else 0.0
        # except:
        #     price = 0.0

        # try:
        #     quantity=int(row['quantity']) if pd.notnull(row['quantity']) else 0
        # except:
        #     quantity = 0
        # # Create the product
        # try:
        #     open_date = pd.to_datetime(row['open-date']) if pd.notnull(row['open-date']) else None
        # except:
        #     open_date = None
        product_obj = DatabaseModel.get_document(Product.objects,{"asin" : row['asin1']}) 
        if product_obj != None:
            print(f"✅ Updated image for SKU: {row['asin1']}")

            product_title = str(row['item-name']) if pd.notnull(row['item-name']) else ""
            price = float(row['price']) if pd.notnull(row['price']) else 0.0
            sku = str(row['seller-sku']) if pd.notnull(row['seller-sku']) else ""
            product_id = str(row['product-id']) if pd.notnull(row['product-id']) else ""
            DatabaseModel.update_documents(Product.objects,{"asin" : row['asin1']},{"product_title" : product_title,"price" : price,"sku":sku,"product_id" : product_id})
        # else:
        #     product = Product(
        #         marketplace_id=marketplace_id,
        #         sku=str(row['seller-sku']) if pd.notnull(row['seller-sku']) else "",
        #         product_title=row['item-name'] if pd.notnull(row['item-name']) else "",
        #         product_description=str(row['item-description']) if pd.notnull(row['item-description']) else "",
        #         price=price,
        #         quantity=quantity,
        #         open_date=open_date,
        #         image_url=str(row['image-url']) if pd.notnull(row['image-url']) else "",
        #         zshop_shipping_fee=str(row['zshop-shipping-fee']) if pd.notnull(row['zshop-shipping-fee']) else "",
        #         item_note=str(row['item-note']) if pd.notnull(row['item-note']) else "",
        #         item_condition=str(row['item-condition']) if pd.notnull(row['item-condition']) else "",
        #         zshop_category=str(row['zshop-category1']) if pd.notnull(row['zshop-category1']) else "",
        #         zshop_browse_path=str(row['zshop-browse-path']) if pd.notnull(row['zshop-browse-path']) else "",
        #         zshop_storefront_feature=row['zshop-storefront-feature'] if pd.notnull(row['zshop-storefront-feature']) else "",
        #         asin=str(row['asin1']) if pd.notnull(row['asin1']) else (str(row['asin2']) if pd.notnull(row['asin2']) else (str(row['asin3']) if pd.notnull(row['asin3']) else "")),
        #         will_ship_internationally=bool(row['will-ship-internationally']),
        #         expedited_shipping=bool(row['expedited-shipping']),
        #         zshop_boldface=bool(row['zshop-boldface']),
        #         product_id=str(row['product-id']) if pd.notnull(row['product-id']) else "",
        #         bid_for_featured_placement=row['bid-for-featured-placement'] if pd.notnull(row['bid-for-featured-placement']) else "",
        #         add_delete=row['add-delete'] if pd.notnull(row['add-delete']) else "",
        #         pending_quantity=0,
        #         delivery_partner=str(row['fulfillment-channel']) if pd.notnull(row['fulfillment-channel']) else "",
        #         published_status=str(row['status']) if pd.notnull(row['status']) else "",
        #     )
        #     product.save()

file_path2 = "/home/lexicon/Documents/newww.xlsx"
# new_process_excel_for_amazon(file_path2)

def saveProductCategory(marketplace_id,name):
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
                "level": 1,
                "marketplace_id" : marketplace_id,
                "end_level" : True
            }
        )
        product_category_id = product_category_obj.id
    return product_category_id


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


def saveManufacturer(marketplace_id,name):
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
    Manufacturer_obj = list(Manufacturer.objects.aggregate(*pipeline))
    if Manufacturer_obj != []:
        Manufacturer_id = Manufacturer_obj[0]['_id']
    if Manufacturer_obj == []:
        Manufacturer_obj = DatabaseModel.save_documents(
            Manufacturer, {
                "name": name,
                "marketplace_id" : marketplace_id,
            }
        )
        Manufacturer_id = Manufacturer_obj.id
    return Manufacturer_id


def processImage(image_list):
    main_image = ""
    images = []

    main_image_candidates = [image for image in image_list if image['variant'] == "MAIN"]
    other_image_candidates = [image for image in image_list if image['variant'] != "MAIN"]

    def select_image(candidates):
        for height in [1000, 500, 75]:
            for image in candidates:
                if image['height'] >= height:
                    return image['link']
        return ""

    main_image = select_image(main_image_candidates)
    images = [select_image([image]) for image in other_image_candidates if select_image([image])]

    return main_image, images




def updateAmazonProductsBasedonAsins(request):
    # user_id = request.GET.get('user_id')
    marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ["id"]).id
    access_token = "Atza|IwEBIL0fPh7bGrkBS8Bo8UyjBeoyreB9C76S8jL8LUhEMqmkPvgjfQla3z5_WMsV8Sd2P7YU36YSxhKDQ9SgAUbVLqYpFnrgCPkc6oviGpK5JfwA4u4n0qDICD-BSNzIZ9uE6lctaNSbQCLy2r2QZN7eZSL7QLKMgmBfICs6uJ3UMmjJWXuCU847r2GwbMnRAONZYM2KbnUTp1nOvURQV_vsVHTvB0hxMxd5R4qsF1_4VUZ7FBEF1uzY7qmvS1Htdo6-Ex478taZAWWKY7aA9RAKa_YuzbzTPfCWJIyaO8xtqYsI6QtIz4M3wcLr1B_3atF_FJndfnLhE8pMncMz9mpRND9F"
    pipeline = [
        {
            "$match" : {
                "marketplace_id" : marketplace_id
            }
        },
        {
            "$project" : {
                "_id" : 1,
                "asin" : {"$ifNull" : ["$asin",""]},  # If asin is null, replace with empty string
            }
        }
    ]
    product_list = list(Product.objects.aggregate(*(pipeline)))
    i=0
    for product_ins in product_list:
        print(f"Processing product {i}...")
        i+=1
        if product_ins['asin'] != "":
            print("ASIN IRUKU................................................")
            PRODUCTS_URL = "https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items"
            """Fetch product details including images & specifications using ASIN."""
            
            if not access_token:
                return None
            
            headers = {
                "x-amz-access-token": access_token,
                "Content-Type": "application/json"
            }

            # Include `includedData` to fetch images and attributes
            url = f"{PRODUCTS_URL}/{product_ins['asin']}?marketplaceIds={MARKETPLACE_ID}&includedData=attributes,images,summaries"

            response = requests.get(url, headers=headers)

            if response.status_code == 200:

                product_data = response.json()
                # print(product_data['summaries'][0])

                try:
                    category_name = product_data['summaries'][0]['browseClassification']['displayName']
                    saveProductCategory(marketplace_id,category_name)
                except:
                    category_name = ""

                try:
                    manufacturer_name = product_data['summaries'][0]['manufacturer']
                    saveManufacturer(marketplace_id,manufacturer_name)
                except:
                    manufacturer_name = ""

                try:
                    features=[feature['value'] for feature in product_data['attributes']['bullet_point']]
                    del product_data['attributes']['bullet_point']

                except:
                    features = []

                try:
                    price = product_data['attributes']['list_price'][0]['value']
                    currency = product_data['attributes']['list_price'][0]['currency']
                    del product_data['attributes']['list_price']
                except:
                    price = 0.0
                    currency = "USD"

                try:
                    model_number = product_data['summaries'][0]['modelNumber']
                except:
                    model_number = ""

                try:
                    brand_name = product_data['summaries'][0]['brand']
                    brand_id = saveBrand(marketplace_id,brand_name)
                except:
                    brand_name = ""
                    brand_id = None
                print("processImage(product_data['images'][0]['images'])",product_data['images'][0]['images'])
                
                main_image, images = processImage(product_data['images'][0]['images'])

                
                
                

                product = Product.objects.get(id=product_ins['_id'])
                product.product_title = product_data['summaries'][0]['itemName']
                product.category = category_name    
                product.brand_name = brand_name
                product.brand_id = brand_id
                product.manufacturer_name = manufacturer_name
                product.model_number = model_number
                product.image_url = main_image
                product.image_urls = images
                product.features = features
                product.price = price
                product.currency = currency
                product.attributes = product_data['attributes']
                product.save()
                print(f"✅ Updated product {product.product_title}")
            else:
                print("ASIN IRUKU.................ANA DATA ILLA...............................")
                print("Error getting access token:", response.text)

    return True


def converttime(iso_string):
    converted_datetime = datetime.fromisoformat(iso_string[:-1] + "+00:00")
    return converted_datetime

            
def process_excel_for_amazonOrders(file_path):
    df = pd.read_excel(file_path)
    marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

    for index, row in df.iterrows():
        currency = "USD"
        order_total = 0.0
        BuyerEmail = ""
        OrderTotal = ast.literal_eval(row['OrderTotal']) if pd.notnull(row['OrderTotal']) else {}
        customer_email_id = ast.literal_eval(row['BuyerInfo']) if pd.notnull(row['BuyerInfo']) else {}
        if customer_email_id != {}:
            BuyerEmail = customer_email_id['BuyerEmail']

        if OrderTotal != {}:
            currency = OrderTotal['CurrencyCode']
            order_total = OrderTotal['Amount']

        order_obj = DatabaseModel.get_document(Order.objects,{"purchase_order_id" : str(row['AmazonOrderId'])})
        if order_obj != None:
            print(f"Order with purchase order ID {row['AmazonOrderId']} already exists. Skipping...")

            access_token= "Atza|IwEBIM_q3I6A40putZMxZ1nkcckzdK7IfMAGKWv0tiAzCa9po4oW38FyEdSPjW0GfNm7Xbh6QYVGjIGp9Y93tMxosHpYiF5PygyMICL7vP14BhNNBMvZmEnGFSyCj7ScRDTA5dJfgSLSvsEEjCyIBGzOa9sZH2DClyuzYCSPn2BttUHHyxYOOGyFeanyel1H0xsBVs3hHboU878MUmsejZEJ_cjGYUXgzlJ5GiYi_DVy4RGCM39Ylwlq2sAxxMpJdlaSBCJITpsU0ZSetiMBmYLRPRLz5dawUEAull0KvR21U3bzncVlfER3kAxIjFf6A44nsrCY68BGe6iy91urByXOme7e"
            url = f"https://sellingpartnerapi-na.amazon.com/orders/v0/orders/{str(row['AmazonOrderId'])}/orderItems"

            # Headers
            headers = {
                "x-amz-access-token": access_token,
                "Content-Type": "application/json"
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                report_url = response.json().get("payload")
                DatabaseModel.update_documents(Order.objects,{"purchase_order_id" : str(row['AmazonOrderId'])},{"order_details" : report_url['OrderItems']})
        else:
            order = Order(
                marketplace_id=marketplace_id,
                customer_email_id = BuyerEmail,
                purchase_order_id=str(row['AmazonOrderId']) if pd.notnull(row['AmazonOrderId']) else "",
                earliest_ship_date=converttime(row['EarliestShipDate']) if pd.notnull(row['EarliestShipDate']) else "",
                sales_channel=str(row['SalesChannel']) if pd.notnull(row['SalesChannel']) else "",
                number_of_items_shipped=int(row['NumberOfItemsShipped']) if pd.notnull(row['NumberOfItemsShipped']) else "",
                order_type=str(row['OrderType']) if pd.notnull(row['OrderType']) else "",
                is_premium_order=True if int(row['IsPremiumOrder'])==1 else False if pd.notnull(row['IsPremiumOrder']) else "",
                is_prime=True if int(row['IsPrime'])==1 else False if pd.notnull(row['IsPrime']) else "",
                fulfillment_channel = str(row['FulfillmentChannel']) if pd.notnull(row['FulfillmentChannel']) else "",
                number_of_items_unshipped = int(row['NumberOfItemsUnshipped']) if pd.notnull(row['NumberOfItemsUnshipped']) else "",
                has_regulated_items=True if int(row['HasRegulatedItems'])==1 else False if pd.notnull(row['HasRegulatedItems']) else "",
                is_replacement_order=False if row['HasRegulatedItems']=="false" else True if pd.notnull(row['HasRegulatedItems']) else "",
                is_sold_by_ab=True if int(row['HasRegulatedItems'])==1 else False if pd.notnull(row['HasRegulatedItems']) else "",
                latest_ship_date = converttime(row['LatestShipDate']) if pd.notnull(row['LatestShipDate']) else "",
                ship_service_level=str(row['ShipServiceLevel']) if pd.notnull(row['ShipServiceLevel']) else "",
                order_date=converttime(row['PurchaseDate']) if pd.notnull(row['PurchaseDate']) else "",
                is_ispu=True if int(row['IsISPU'])==1 else False if pd.notnull(row['IsISPU']) else "",
                order_status = str(row['OrderStatus']) if pd.notnull(row['OrderStatus']) else "",
                shipping_information = ast.literal_eval(row['ShippingAddress']) if pd.notnull(row['ShippingAddress']) else {},
                is_access_point_order=True if int(row['IsAccessPointOrder'])==1 else False if pd.notnull(row['IsAccessPointOrder']) else "",
                seller_order_id = str(row['SellerOrderId']) if pd.notnull(row['SellerOrderId']) else "",
                payment_method = str(row['PaymentMethod']) if pd.notnull(row['PaymentMethod']) else "",
                is_business_order = True if int(row['IsBusinessOrder'])==1 else False if pd.notnull(row['IsBusinessOrder']) else "",
                order_total = order_total,
                currency = currency,
                payment_method_details = eval(row['PaymentMethodDetails'])[0] if pd.notnull(row['PaymentMethodDetails']) else "",
                is_global_express_enabled = True if int(row['IsGlobalExpressEnabled'])==1 else False if pd.notnull(row['IsGlobalExpressEnabled']) else "",
                last_update_date = converttime(row['LastUpdateDate']) if pd.notnull(row['LastUpdateDate']) else "",
                shipment_service_level_category = str(row['ShipmentServiceLevelCategory']) if pd.notnull(row['ShipmentServiceLevelCategory']) else "",
                automated_shipping_settings = ast.literal_eval(row['AutomatedShippingSettings']) if pd.notnull(row['AutomatedShippingSettings']) else {},
                
            )
            order.save()

file_path2 = "/home/lexicon/walmart/Amazonorders.xlsx"
# process_excel_for_amazonOrders(file_path2)


def process_amazon_order(json_data,order_date=None):
    """Processes a single Amazon order item and saves it to the OrderItems collection."""
    try:
        product = DatabaseModel.get_document(Product.objects, {"product_title": json_data.get("Title", "")}, ["id"])
        product_id = product.id if product else None
    except:
        product_id = None

    # Helper function to extract money values safely
    def get_money(field_name):
        return {
            "CurrencyCode": json_data.get(field_name, {}).get("CurrencyCode", "USD"),
            "Amount": float(json_data.get(field_name, {}).get("Amount", 0.0))
        }

    order_item = OrderItems(
        OrderId=json_data.get("OrderItemId", ""),
        Platform="Amazon",
        created_date=order_date if order_date else datetime.now(),
        ProductDetails=ProductDetails(
            product_id = product_id,
            Title=json_data.get("Title", "Unknown Product"),
            SKU=json_data.get("SellerSKU", "Unknown SKU"),
            ASIN=json_data.get("ASIN", "Unknown ASIN"),
            QuantityOrdered=int(json_data.get("QuantityOrdered", 0)),
            QuantityShipped=int(json_data.get("QuantityShipped", 0)),
        ),
        Pricing=Pricing(
            ItemPrice=Money(**get_money("ItemPrice")),
            ItemTax=Money(**get_money("ItemTax")),
            PromotionDiscount=Money(**get_money("PromotionDiscount"))
        ),
        TaxCollection=TaxCollection(
            Model=json_data.get("TaxCollection", {}).get("Model", "Unknown"),
            ResponsibleParty=json_data.get("TaxCollection", {}).get("ResponsibleParty", "Unknown")
        ),
        IsGift=json_data.get("IsGift", "false") == "true",
        BuyerInfo=json_data.get("BuyerInfo", None)
    )
    order_item.save()  # Save to MongoDB

    return order_item  # Return reference to the saved OrderItems document


def updateOrdersItemsDetailsAmazon(request):
    """Fetches Amazon orders, processes each item, and updates the Order collection."""
    marketplace = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ["id"])
    if not marketplace:
        print("Amazon Marketplace not found!")
        return False

    marketplace_id = marketplace.id
    order_list = DatabaseModel.list_documents(Order.objects, {"marketplace_id": marketplace_id}, ["id", "order_details"])

    for order in order_list:
        order_items_refs = []  # Store references to OrderItems documents

        for item in order.order_details:
            print(item)
            processed_item = process_amazon_order(item)
            order_items_refs.append(processed_item)  # Append reference

        # Update Order document with references to OrderItems
        DatabaseModel.update_documents(Order.objects, {"id": order.id}, {"order_items": order_items_refs})

    print("Amazon orders updated successfully!")
    return True



def syncRecentAmazonOrders():
    access_token = get_access_token()
    if not access_token:
        return None

    url = "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"
    
    # Amazon only allows retrieving orders from the last 180 days
    created_after = (datetime.utcnow() - timedelta(days=18000)).isoformat()

    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    params = {
        "MarketplaceIds": MARKETPLACE_ID,
        "CreatedAfter": created_after,
        "MaxResultsPerPage": 100  # Get max orders per page
    }

    orders = []
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        orders_data = response.json()
        orders = orders_data.get("payload").get('Orders')
        # Check for more pages
        marketplace_id = DatabaseModel.get_document(Marketplace.objects,{"name" : "Amazon"},['id']).id

        for row in orders:
            order_obj = DatabaseModel.get_document(Order.objects, {"purchase_order_id": str(row.get('AmazonOrderId', ""))})
            if order_obj:
                print(f"Order with purchase order ID {row.get('AmazonOrderId', '')} already exists. Skipping...")
            else:
                order_details = list()
                order_items = list()
                currency = "USD"
                order_total = 0.0
                BuyerEmail = ""
                OrderTotal = row.get('OrderTotal', {})
                customer_email_id = row.get('BuyerInfo', {})
                if customer_email_id:
                    BuyerEmail = customer_email_id.get('BuyerEmail', "")

                if OrderTotal:
                    currency = OrderTotal.get('CurrencyCode', "USD")
                order_total = OrderTotal.get('Amount', 0.0)
                url = f"https://sellingpartnerapi-na.amazon.com/orders/v0/orders/{str(row.get('AmazonOrderId', ''))}/orderItems"

                # Headers
                headers = {
                    "x-amz-access-token": access_token,
                    "Content-Type": "application/json"
                }

                response = requests.get(url, headers=headers)
                order_date = converttime(row.get('PurchaseDate', "")) if row.get('PurchaseDate') else ""
                if response.status_code == 200:
                    report_url = response.json().get("payload", {})
                    order_details =  report_url.get('OrderItems', [])
                    for order_ins in order_details:
                        order_items.append(process_amazon_order(order_ins),order_date)


                order = Order(
                    marketplace_id=marketplace_id,
                    customer_email_id=BuyerEmail,
                    purchase_order_id=str(row.get('AmazonOrderId', "")),
                    earliest_ship_date=converttime(row.get('EarliestShipDate', "")) if row.get('EarliestShipDate') else "",
                    sales_channel=str(row.get('SalesChannel', "")),
                    number_of_items_shipped=int(row.get('NumberOfItemsShipped', 0)),
                    order_type=str(row.get('OrderType', "")),
                    is_premium_order=row.get('IsPremiumOrder', False),
                    is_prime=row.get('IsPrime', False),
                    fulfillment_channel=str(row.get('FulfillmentChannel', "")),
                    number_of_items_unshipped=int(row.get('NumberOfItemsUnshipped', 0)),
                    has_regulated_items=row.get('HasRegulatedItems', False),
                    is_replacement_order=row.get('IsReplacementOrder', False),
                    is_sold_by_ab=row.get('IsSoldByAB', False),
                    latest_ship_date=converttime(row.get('LatestShipDate', "")) if row.get('LatestShipDate') else "",
                    ship_service_level=str(row.get('ShipServiceLevel', "")),
                    order_date=order_date,
                    is_ispu=row.get('IsISPU', False),
                    order_status=str(row.get('OrderStatus', "")),
                    shipping_information=row.get('ShippingAddress', {}),
                    is_access_point_order=row.get('IsAccessPointOrder', False),
                    seller_order_id=str(row.get('SellerOrderId', "")),
                    payment_method=str(row.get('PaymentMethod', "")),
                    is_business_order=row.get('IsBusinessOrder', False),
                    order_total=order_total,
                    currency=currency,
                    payment_method_details=row.get('PaymentMethodDetails', [])[0] if row.get('PaymentMethodDetails') else "",
                    is_global_express_enabled=row.get('IsGlobalExpressEnabled', False),
                    last_update_date=converttime(row.get('LastUpdateDate', "")) if row.get('LastUpdateDate') else "",
                    shipment_service_level_category=str(row.get('ShipmentServiceLevelCategory', "")),
                    automated_shipping_settings=row.get('AutomatedShippingSettings', {}),
                    order_details =order_details,
                    order_items = order_items
                )
                order.save()
           
    return orders



def ProcessAmazonProductAttributes():
    marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ['id']).id
    pipeline = [
        {
            "$match": {
                "marketplace_id": marketplace_id
            }
        },
        {
            "$project": {
                "_id": 1,
                "attributes": {"$ifNull": ["$attributes", {}]},  # If attributes is null, replace with empty dict
            }
        },
    ]
    product_list = list(Product.objects.aggregate(*(pipeline)))
    for product_ins in product_list:
        new_dict = {}
        for key, value in product_ins['attributes'].items():
            if isinstance(value, list) and len(value) == 1:  # Check if the field contains a single-item list
                single_value = value[0]
                filtered_value = {k: v for k, v in single_value.items() if k not in ["language_tag", "marketplace_id"]}
                new_dict[key] = filtered_value
            elif isinstance(value, list):  # Handle multi-item lists if needed
                new_dict[key] = [
                    {k: v for k, v in item.items() if k not in ["language_tag", "marketplace_id"]}
                    for item in value
                ]
        print(f"Processed attributes for product {product_ins['_id']}: {len(new_dict)}")
        DatabaseModel.update_documents(Product.objects,{"id" : product_ins['_id']},{"attributes" : new_dict,"old_attributes" : product_ins['attributes']})


# ProcessAmazonProductAttributes()