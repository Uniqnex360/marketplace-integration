import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.amazon_utils import getAccesstoken
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId
import json
from ecommerce_tool.settings import MARKETPLACE_ID
import ast



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

# file_path2 = "/home/lexicon/Documents/amazon_products.xlsx"
# process_excel_for_amazon(file_path2)

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

    for image in image_list:
        if image['height'] > 1000:
            if image['variant'] == "MAIN":
                main_image = image['link']
            else:
                images.append(image['link'])

    return main_image, images




def updateAmazonProductsBasedonAsins(request):
    # user_id = request.GET.get('user_id')
    marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ["id"]).id
    access_token = "Atza|IwEBIO-JJcX7ZC4FM4CUZVsijnQiBSQI94df81jHXQbMhsVLmybpuDUz2tdkvP4-6MdpzimIkjsCLYO107lhPeFNKROyfcxURqaC0lrCEEd5feErYyqGHL-PfRe2ywlWXq3rT8Cst_TAVy37nROHdaw51BcrLUpvFnY31oFmIKZwxTOfIBL_bhmks-RwrbB6C5MJFcTG0sxXXKMsOdCw1081aaPXVk2Xo_G87agQpbQRGiQwsszQZ0bfc4BxGSfZZzuhFhpGrv5YDWCdsQLptJawZSRBi889C-5tUi1G0-Vu1ZD4wqsemOOnDDmEZM77clC_EQKVm3E-q9QuLVTk1yxn_WUw"
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
                print(product_data['summaries'][0])

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