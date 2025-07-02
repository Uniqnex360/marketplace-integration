# import requests
# import uuid
# import xml.etree.ElementTree as ET  # Import XML parser
# from omnisight.operations.amazon_utils import getAccesstoken, get_access_token
# from omnisight.models import *
# import pandas as pd
# from ecommerce_tool.crud import DatabaseModel
# from bson import ObjectId
# import json
# from ecommerce_tool.settings import MARKETPLACE_ID,SELLERCLOUD_USERNAME, SELLERCLOUD_PASSWORD, Role_ARN, Acccess_Key, Secret_Access_Key,AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN, SELLER_ID
# import ast
# from datetime import datetime, timedelta
# import sys
# import math
# import requests
# import boto3
# import time
# import gzip
# import io
# import threading
# from queue import Queue
# # Time range for orders report (last 4 hours)
# from io import StringIO, BytesIO
# import sys # For environment diagnostics
# from sp_api.api import Reports
# from sp_api.base import Marketplaces, SellingApiException
# from forex_python.converter import CurrencyRates



# def process_excel_for_amazon(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         print(f"Processing row {index + 1}...")
#         try:
#             price=float(row['price']) if pd.notnull(row['price']) else 0.0
#         except:
#             price = 0.0

#         try:
#             quantity=int(row['quantity']) if pd.notnull(row['quantity']) else 0
#         except:
#             quantity = 0
#         # Create the product
#         try:
#             open_date = pd.to_datetime(row['open-date']) if pd.notnull(row['open-date']) else None
#         except:
#             open_date = None
        
#         product = Product(
#             marketplace_id=marketplace_id,
#             sku=str(row['seller-sku']) if pd.notnull(row['seller-sku']) else "",
#             product_title=row['item-name'] if pd.notnull(row['item-name']) else "",
#             product_description=str(row['item-description']) if pd.notnull(row['item-description']) else "",
#             price=price,
#             quantity=quantity,
#             open_date=open_date,
#             image_url=str(row['image-url']) if pd.notnull(row['image-url']) else "",
#             zshop_shipping_fee=str(row['zshop-shipping-fee']) if pd.notnull(row['zshop-shipping-fee']) else "",
#             item_note=str(row['item-note']) if pd.notnull(row['item-note']) else "",
#             item_condition=str(row['item-condition']) if pd.notnull(row['item-condition']) else "",
#             zshop_category=str(row['zshop-category1']) if pd.notnull(row['zshop-category1']) else "",
#             zshop_browse_path=str(row['zshop-browse-path']) if pd.notnull(row['zshop-browse-path']) else "",
#             zshop_storefront_feature=row['zshop-storefront-feature'] if pd.notnull(row['zshop-storefront-feature']) else "",
#             asin=str(row['asin1']) if pd.notnull(row['asin1']) else (str(row['asin2']) if pd.notnull(row['asin2']) else (str(row['asin3']) if pd.notnull(row['asin3']) else "")),
#             will_ship_internationally=bool(row['will-ship-internationally']),
#             expedited_shipping=bool(row['expedited-shipping']),
#             zshop_boldface=bool(row['zshop-boldface']),
#             product_id=str(row['product-id']) if pd.notnull(row['product-id']) else "",
#             bid_for_featured_placement=row['bid-for-featured-placement'] if pd.notnull(row['bid-for-featured-placement']) else "",
#             add_delete=row['add-delete'] if pd.notnull(row['add-delete']) else "",
#             pending_quantity=0,
#             delivery_partner=str(row['fulfillment-channel']) if pd.notnull(row['fulfillment-channel']) else "",
#             published_status=str(row['status']) if pd.notnull(row['status']) else "",
#         )
#         product.save()

# file_path2 = "/home/lexicon/Documents/newww.xlsx"
# # process_excel_for_amazon(file_path2)



# def new_process_excel_for_amazon(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         print(f"Processing row {index}...{row['asin1']}")
#         # try:
#         #     price=float(row['price']) if pd.notnull(row['price']) else 0.0
#         # except:
#         #     price = 0.0

#         # try:
#         #     quantity=int(row['quantity']) if pd.notnull(row['quantity']) else 0
#         # except:
#         #     quantity = 0
#         # # Create the product
#         # try:
#         #     open_date = pd.to_datetime(row['open-date']) if pd.notnull(row['open-date']) else None
#         # except:
#         #     open_date = None
#         product_obj = DatabaseModel.get_document(Product.objects,{"asin" : row['asin1']}) 
#         if product_obj != None:
#             print(f"✅ Updated image for SKU: {row['asin1']}")

#             product_title = str(row['item-name']) if pd.notnull(row['item-name']) else ""
#             price = float(row['price']) if pd.notnull(row['price']) else 0.0
#             sku = str(row['seller-sku']) if pd.notnull(row['seller-sku']) else ""
#             product_id = str(row['product-id']) if pd.notnull(row['product-id']) else ""
#             DatabaseModel.update_documents(Product.objects,{"asin" : row['asin1']},{"product_title" : product_title,"price" : price,"sku":sku,"product_id" : product_id})
#         # else:
#         #     product = Product(
#         #         marketplace_id=marketplace_id,
#         #         sku=str(row['seller-sku']) if pd.notnull(row['seller-sku']) else "",
#         #         product_title=row['item-name'] if pd.notnull(row['item-name']) else "",
#         #         product_description=str(row['item-description']) if pd.notnull(row['item-description']) else "",
#         #         price=price,
#         #         quantity=quantity,
#         #         open_date=open_date,
#         #         image_url=str(row['image-url']) if pd.notnull(row['image-url']) else "",
#         #         zshop_shipping_fee=str(row['zshop-shipping-fee']) if pd.notnull(row['zshop-shipping-fee']) else "",
#         #         item_note=str(row['item-note']) if pd.notnull(row['item-note']) else "",
#         #         item_condition=str(row['item-condition']) if pd.notnull(row['item-condition']) else "",
#         #         zshop_category=str(row['zshop-category1']) if pd.notnull(row['zshop-category1']) else "",
#         #         zshop_browse_path=str(row['zshop-browse-path']) if pd.notnull(row['zshop-browse-path']) else "",
#         #         zshop_storefront_feature=row['zshop-storefront-feature'] if pd.notnull(row['zshop-storefront-feature']) else "",
#         #         asin=str(row['asin1']) if pd.notnull(row['asin1']) else (str(row['asin2']) if pd.notnull(row['asin2']) else (str(row['asin3']) if pd.notnull(row['asin3']) else "")),
#         #         will_ship_internationally=bool(row['will-ship-internationally']),
#         #         expedited_shipping=bool(row['expedited-shipping']),
#         #         zshop_boldface=bool(row['zshop-boldface']),
#         #         product_id=str(row['product-id']) if pd.notnull(row['product-id']) else "",
#         #         bid_for_featured_placement=row['bid-for-featured-placement'] if pd.notnull(row['bid-for-featured-placement']) else "",
#         #         add_delete=row['add-delete'] if pd.notnull(row['add-delete']) else "",
#         #         pending_quantity=0,
#         #         delivery_partner=str(row['fulfillment-channel']) if pd.notnull(row['fulfillment-channel']) else "",
#         #         published_status=str(row['status']) if pd.notnull(row['status']) else "",
#         #     )
#         #     product.save()

# file_path2 = "/home/lexicon/Documents/newww.xlsx"
# # new_process_excel_for_amazon(file_path2)

# def saveProductCategory(marketplace_id,name):
#     pipeline = [
#     {"$match": {"name": name,
#                 "marketplace_id" : marketplace_id}},
#     {
#         "$project": {
#             "_id": 1
#         }
#         },
#         {
#             "$limit" : 1
#         }
    
#     ]
#     product_category_obj = list(Category.objects.aggregate(*pipeline))
#     if product_category_obj != []:
#         product_category_id = product_category_obj[0]['_id']
#     if product_category_obj == []:
#         product_category_obj = DatabaseModel.save_documents(
#             Category, {
#                 "name": name,
#                 "level": 1,
#                 "marketplace_id" : marketplace_id,
#                 "end_level" : True
#             }
#         )
#         product_category_id = product_category_obj.id
#     return product_category_id


# def saveBrand(marketplace_id,name):
#     pipeline = [
#     {"$match": {"name": name,
#                 "marketplace_id" : marketplace_id}},
#     {
#         "$project": {
#             "_id": 1
#         }
#         },
#         {
#             "$limit" : 1
#         }
    
#     ]
#     brand_obj = list(Brand.objects.aggregate(*pipeline))
#     if brand_obj != []:
#         brand_id = brand_obj[0]['_id']
#     if brand_obj == []:
#         brand_obj = DatabaseModel.save_documents(
#             Brand, {
#                 "name": name,
#                 "marketplace_id" : marketplace_id,
#             }
#         )
#         brand_id = brand_obj.id
#     return brand_id


# def saveManufacturer(marketplace_id,name):
#     pipeline = [
#     {"$match": {"name": name,
#                 "marketplace_id" : marketplace_id}},
#     {
#         "$project": {
#             "_id": 1
#         }
#         },
#         {
#             "$limit" : 1
#         }
    
#     ]
#     Manufacturer_obj = list(Manufacturer.objects.aggregate(*pipeline))
#     if Manufacturer_obj != []:
#         Manufacturer_id = Manufacturer_obj[0]['_id']
#     if Manufacturer_obj == []:
#         Manufacturer_obj = DatabaseModel.save_documents(
#             Manufacturer, {
#                 "name": name,
#                 "marketplace_id" : marketplace_id,
#             }
#         )
#         Manufacturer_id = Manufacturer_obj.id
#     return Manufacturer_id


# def processImage(image_list):
#     main_image = ""
#     images = []

#     main_image_candidates = [image for image in image_list if image['variant'] == "MAIN"]
#     other_image_candidates = [image for image in image_list if image['variant'] != "MAIN"]

#     def select_image(candidates):
#         for height in [1000, 500, 75]:
#             for image in candidates:
#                 if image['height'] >= height:
#                     return image['link']
#         return ""

#     main_image = select_image(main_image_candidates)
#     images = [select_image([image]) for image in other_image_candidates if select_image([image])]

#     return main_image, images




# def updateAmazonProductsBasedonAsins(request):
#     # user_id = request.GET.get('user_id')
#     marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ["id"]).id
#     access_token = "Atza|IwEBIL0fPh7bGrkBS8Bo8UyjBeoyreB9C76S8jL8LUhEMqmkPvgjfQla3z5_WMsV8Sd2P7YU36YSxhKDQ9SgAUbVLqYpFnrgCPkc6oviGpK5JfwA4u4n0qDICD-BSNzIZ9uE6lctaNSbQCLy2r2QZN7eZSL7QLKMgmBfICs6uJ3UMmjJWXuCU847r2GwbMnRAONZYM2KbnUTp1nOvURQV_vsVHTvB0hxMxd5R4qsF1_4VUZ7FBEF1uzY7qmvS1Htdo6-Ex478taZAWWKY7aA9RAKa_YuzbzTPfCWJIyaO8xtqYsI6QtIz4M3wcLr1B_3atF_FJndfnLhE8pMncMz9mpRND9F"
#     pipeline = [
#         {
#             "$match" : {
#                 "marketplace_id" : marketplace_id
#             }
#         },
#         {
#             "$project" : {
#                 "_id" : 1,
#                 "asin" : {"$ifNull" : ["$asin",""]},  # If asin is null, replace with empty string
#             }
#         }
#     ]
#     product_list = list(Product.objects.aggregate(*(pipeline)))
#     i=0
#     for product_ins in product_list:
#         print(f"Processing product {i}...")
#         i+=1
#         if product_ins['asin'] != "":
#             print("ASIN IRUKU................................................")
#             PRODUCTS_URL = "https://sellingpartnerapi-na.amazon.com/catalog/2022-04-01/items"
#             """Fetch product details including images & specifications using ASIN."""
            
#             if not access_token:
#                 return None
            
#             headers = {
#                 "x-amz-access-token": access_token,
#                 "Content-Type": "application/json"
#             }

#             # Include `includedData` to fetch images and attributes
#             url = f"{PRODUCTS_URL}/{product_ins['asin']}?marketplaceIds={MARKETPLACE_ID}&includedData=attributes,images,summaries"

#             response = requests.get(url, headers=headers)

#             if response.status_code == 200:

#                 product_data = response.json()
#                 # print(product_data['summaries'][0])

#                 try:
#                     category_name = product_data['summaries'][0]['browseClassification']['displayName']
#                     saveProductCategory(marketplace_id,category_name)
#                 except:
#                     category_name = ""

#                 try:
#                     manufacturer_name = product_data['summaries'][0]['manufacturer']
#                     saveManufacturer(marketplace_id,manufacturer_name)
#                 except:
#                     manufacturer_name = ""

#                 try:
#                     features=[feature['value'] for feature in product_data['attributes']['bullet_point']]
#                     del product_data['attributes']['bullet_point']

#                 except:
#                     features = []

#                 try:
#                     price = product_data['attributes']['list_price'][0]['value']
#                     currency = product_data['attributes']['list_price'][0]['currency']
#                     del product_data['attributes']['list_price']
#                 except:
#                     price = 0.0
#                     currency = "USD"

#                 try:
#                     model_number = product_data['summaries'][0]['modelNumber']
#                 except:
#                     model_number = ""

#                 try:
#                     brand_name = product_data['summaries'][0]['brand']
#                     brand_id = saveBrand(marketplace_id,brand_name)
#                 except:
#                     brand_name = ""
#                     brand_id = None
#                 print("processImage(product_data['images'][0]['images'])",product_data['images'][0]['images'])
                
#                 main_image, images = processImage(product_data['images'][0]['images'])

                
                
                

#                 product = Product.objects.get(id=product_ins['_id'])
#                 product.product_title = product_data['summaries'][0]['itemName']
#                 product.category = category_name    
#                 product.brand_name = brand_name
#                 product.brand_id = brand_id
#                 product.manufacturer_name = manufacturer_name
#                 product.model_number = model_number
#                 product.image_url = main_image
#                 product.image_urls = images
#                 product.features = features
#                 product.price = price
#                 product.currency = currency
#                 product.attributes = product_data['attributes']
#                 product.save()
#                 print(f"✅ Updated product {product.product_title}")
#             else:
#                 print("ASIN IRUKU.................ANA DATA ILLA...............................")
#                 print("Error getting access token:", response.text)

#     return True


# def converttime(iso_string):
#     converted_datetime = datetime.fromisoformat(iso_string[:-1] + "+00:00")
#     return converted_datetime

            
# def process_excel_for_amazonOrders(file_path):
#     df = pd.read_excel(file_path)
#     marketplace_id = ObjectId('67ce8f51ab471ccbb9f5d9ff')

#     for index, row in df.iterrows():
#         currency = "USD"
#         order_total = 0.0
#         BuyerEmail = ""
#         OrderTotal = ast.literal_eval(row['OrderTotal']) if pd.notnull(row['OrderTotal']) else {}
#         customer_email_id = ast.literal_eval(row['BuyerInfo']) if pd.notnull(row['BuyerInfo']) else {}
#         if customer_email_id != {}:
#             BuyerEmail = customer_email_id['BuyerEmail']

#         if OrderTotal != {}:
#             currency = OrderTotal['CurrencyCode']
#             order_total = OrderTotal['Amount']

#         order_obj = DatabaseModel.get_document(Order.objects,{"purchase_order_id" : str(row['AmazonOrderId'])})
#         if order_obj != None:
#             print(f"Order with purchase order ID {row['AmazonOrderId']} already exists. Skipping...")

#             access_token= "Atza|IwEBIM_q3I6A40putZMxZ1nkcckzdK7IfMAGKWv0tiAzCa9po4oW38FyEdSPjW0GfNm7Xbh6QYVGjIGp9Y93tMxosHpYiF5PygyMICL7vP14BhNNBMvZmEnGFSyCj7ScRDTA5dJfgSLSvsEEjCyIBGzOa9sZH2DClyuzYCSPn2BttUHHyxYOOGyFeanyel1H0xsBVs3hHboU878MUmsejZEJ_cjGYUXgzlJ5GiYi_DVy4RGCM39Ylwlq2sAxxMpJdlaSBCJITpsU0ZSetiMBmYLRPRLz5dawUEAull0KvR21U3bzncVlfER3kAxIjFf6A44nsrCY68BGe6iy91urByXOme7e"
#             url = f"https://sellingpartnerapi-na.amazon.com/orders/v0/orders/{str(row['AmazonOrderId'])}/orderItems"

#             # Headers
#             headers = {
#                 "x-amz-access-token": access_token,
#                 "Content-Type": "application/json"
#             }

#             response = requests.get(url, headers=headers)
#             if response.status_code == 200:
#                 report_url = response.json().get("payload")
#                 DatabaseModel.update_documents(Order.objects,{"purchase_order_id" : str(row['AmazonOrderId'])},{"order_details" : report_url['OrderItems']})
#         else:
#             order = Order(
#                 marketplace_id=marketplace_id,
#                 customer_email_id = BuyerEmail,
#                 purchase_order_id=str(row['AmazonOrderId']) if pd.notnull(row['AmazonOrderId']) else "",
#                 earliest_ship_date=converttime(row['EarliestShipDate']) if pd.notnull(row['EarliestShipDate']) else "",
#                 sales_channel=str(row['SalesChannel']) if pd.notnull(row['SalesChannel']) else "",
#                 number_of_items_shipped=int(row['NumberOfItemsShipped']) if pd.notnull(row['NumberOfItemsShipped']) else "",
#                 order_type=str(row['OrderType']) if pd.notnull(row['OrderType']) else "",
#                 is_premium_order=True if int(row['IsPremiumOrder'])==1 else False if pd.notnull(row['IsPremiumOrder']) else "",
#                 is_prime=True if int(row['IsPrime'])==1 else False if pd.notnull(row['IsPrime']) else "",
#                 fulfillment_channel = str(row['FulfillmentChannel']) if pd.notnull(row['FulfillmentChannel']) else "",
#                 number_of_items_unshipped = int(row['NumberOfItemsUnshipped']) if pd.notnull(row['NumberOfItemsUnshipped']) else "",
#                 has_regulated_items=True if int(row['HasRegulatedItems'])==1 else False if pd.notnull(row['HasRegulatedItems']) else "",
#                 is_replacement_order=False if row['HasRegulatedItems']=="false" else True if pd.notnull(row['HasRegulatedItems']) else "",
#                 is_sold_by_ab=True if int(row['HasRegulatedItems'])==1 else False if pd.notnull(row['HasRegulatedItems']) else "",
#                 latest_ship_date = converttime(row['LatestShipDate']) if pd.notnull(row['LatestShipDate']) else "",
#                 ship_service_level=str(row['ShipServiceLevel']) if pd.notnull(row['ShipServiceLevel']) else "",
#                 order_date=converttime(row['PurchaseDate']) if pd.notnull(row['PurchaseDate']) else "",
#                 is_ispu=True if int(row['IsISPU'])==1 else False if pd.notnull(row['IsISPU']) else "",
#                 order_status = str(row['OrderStatus']) if pd.notnull(row['OrderStatus']) else "",
#                 shipping_information = ast.literal_eval(row['ShippingAddress']) if pd.notnull(row['ShippingAddress']) else {},
#                 is_access_point_order=True if int(row['IsAccessPointOrder'])==1 else False if pd.notnull(row['IsAccessPointOrder']) else "",
#                 seller_order_id = str(row['SellerOrderId']) if pd.notnull(row['SellerOrderId']) else "",
#                 payment_method = str(row['PaymentMethod']) if pd.notnull(row['PaymentMethod']) else "",
#                 is_business_order = True if int(row['IsBusinessOrder'])==1 else False if pd.notnull(row['IsBusinessOrder']) else "",
#                 order_total = order_total,
#                 currency = currency,
#                 payment_method_details = eval(row['PaymentMethodDetails'])[0] if pd.notnull(row['PaymentMethodDetails']) else "",
#                 is_global_express_enabled = True if int(row['IsGlobalExpressEnabled'])==1 else False if pd.notnull(row['IsGlobalExpressEnabled']) else "",
#                 last_update_date = converttime(row['LastUpdateDate']) if pd.notnull(row['LastUpdateDate']) else "",
#                 shipment_service_level_category = str(row['ShipmentServiceLevelCategory']) if pd.notnull(row['ShipmentServiceLevelCategory']) else "",
#                 automated_shipping_settings = ast.literal_eval(row['AutomatedShippingSettings']) if pd.notnull(row['AutomatedShippingSettings']) else {},
                
#             )
#             order.save()

# file_path2 = "/home/lexicon/walmart/Amazonorders.xlsx"
# # process_excel_for_amazonOrders(file_path2)


# def process_amazon_order(json_data,order_date=None):
#     """Processes a single Amazon order item and saves it to the OrderItems collection."""
#     try:
#         product = DatabaseModel.get_document(Product.objects, {"sku": json_data.get("SellerSKU", "")}, ["id"])
#         product_id = product.id if product else None
#     except:
#         product_id = None

#     # Helper function to extract money values safely
#     def get_money(field_name):
#         return {
#             "CurrencyCode": json_data.get(field_name, {}).get("CurrencyCode", "USD"),
#             "Amount": float(json_data.get(field_name, {}).get("Amount", 0.0))
#         }

#     order_item = OrderItems(
#         OrderId=json_data.get("OrderItemId", ""),
#         Platform="Amazon",
#         created_date=order_date if order_date else datetime.now(),
#         ProductDetails=ProductDetails(
#             product_id = product_id,
#             Title=json_data.get("Title", "Unknown Product"),
#             SKU=json_data.get("SellerSKU", "Unknown SKU"),
#             ASIN=json_data.get("ASIN", "Unknown ASIN"),
#             QuantityOrdered=int(json_data.get("QuantityOrdered", 0)),
#             QuantityShipped=int(json_data.get("QuantityShipped", 0)),
#         ),
#         Pricing=Pricing(
#             ItemPrice=Money(**get_money("ItemPrice")),
#             ItemTax=Money(**get_money("ItemTax")),
#             PromotionDiscount=Money(**get_money("PromotionDiscount"))
#         ),
#         TaxCollection=TaxCollection(
#             Model=json_data.get("TaxCollection", {}).get("Model", "Unknown"),
#             ResponsibleParty=json_data.get("TaxCollection", {}).get("ResponsibleParty", "Unknown")
#         ),
#         IsGift=json_data.get("IsGift", "false") == "true",
#         BuyerInfo=json_data.get("BuyerInfo", None)
#     )
#     order_item.save()  # Save to MongoDB

#     return order_item  # Return reference to the saved OrderItems document


# def updateOrdersItemsDetailsAmazon(request):
#     """Fetches Amazon orders, processes each item, and updates the Order collection."""
#     marketplace = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ["id"])
#     if not marketplace:
#         print("Amazon Marketplace not found!")
#         return False

#     marketplace_id = marketplace.id
#     order_list = DatabaseModel.list_documents(Order.objects, {"marketplace_id": marketplace_id}, ["id", "order_details"])

#     for order in order_list:
#         order_items_refs = []  # Store references to OrderItems documents

#         for item in order.order_details:
#             print(item)
#             processed_item = process_amazon_order(item)
#             order_items_refs.append(processed_item)  # Append reference

#         # Update Order document with references to OrderItems
#         DatabaseModel.update_documents(Order.objects, {"id": order.id}, {"order_items": order_items_refs})

#     print("Amazon orders updated successfully!")
#     return True



# # def syncRecentAmazonOrders():
# #     access_token = get_access_token()
# #     if not access_token:
# #         return None

# #     url = "https://sellingpartnerapi-na.amazon.com/orders/v0/orders"
    
# #     # Amazon only allows retrieving orders from the last 180 days
# #     created_after = (datetime.utcnow() - timedelta(days=18000)).isoformat()

# #     headers = {
# #         "x-amz-access-token": access_token,
# #         "Content-Type": "application/json"
# #     }

# #     params = {
# #         "MarketplaceIds": MARKETPLACE_ID,
# #         "CreatedAfter": created_after,
# #         "MaxResultsPerPage": 100  # Get max orders per page
# #     }

# #     orders = []
# #     response = requests.get(url, headers=headers, params=params)

# #     if response.status_code == 200:
# #         orders_data = response.json()
# #         orders = orders_data.get("payload").get('Orders')
# #         # Check for more pages
# #         marketplace_id = DatabaseModel.get_document(Marketplace.objects,{"name" : "Amazon"},['id']).id

# #         for row in orders:
# #             order_obj = DatabaseModel.get_document(Order.objects, {"purchase_order_id": str(row.get('AmazonOrderId', ""))})
# #             if order_obj:
# #                 print(f"Order with purchase order ID {row.get('AmazonOrderId', '')} already exists. Skipping...")
# #             else:
# #                 order_details = list()
# #                 order_items = list()
# #                 currency = "USD"
# #                 order_total = 0.0
# #                 BuyerEmail = ""
# #                 OrderTotal = row.get('OrderTotal', {})
# #                 customer_email_id = row.get('BuyerInfo', {})
# #                 if customer_email_id:
# #                     BuyerEmail = customer_email_id.get('BuyerEmail', "")

# #                 if OrderTotal:
# #                     currency = OrderTotal.get('CurrencyCode', "USD")
# #                 order_total = OrderTotal.get('Amount', 0.0)
# #                 url = f"https://sellingpartnerapi-na.amazon.com/orders/v0/orders/{str(row.get('AmazonOrderId', ''))}/orderItems"

# #                 # Headers
# #                 headers = {
# #                     "x-amz-access-token": access_token,
# #                     "Content-Type": "application/json"
# #                 }

# #                 response = requests.get(url, headers=headers)
# #                 order_date = converttime(row.get('PurchaseDate', "")) if row.get('PurchaseDate') else ""
# #                 if response.status_code == 200:
# #                     report_url = response.json().get("payload", {})
# #                     order_details =  report_url.get('OrderItems', [])
# #                     for order_ins in order_details:
# #                         order_items.append(process_amazon_order(order_ins,order_date))


# #                 order = Order(
# #                     marketplace_id=marketplace_id,
# #                     customer_email_id=BuyerEmail,
# #                     purchase_order_id=str(row.get('AmazonOrderId', "")),
# #                     earliest_ship_date=converttime(row.get('EarliestShipDate', "")) if row.get('EarliestShipDate') else "",
# #                     sales_channel=str(row.get('SalesChannel', "")),
# #                     number_of_items_shipped=int(row.get('NumberOfItemsShipped', 0)),
# #                     order_type=str(row.get('OrderType', "")),
# #                     is_premium_order=row.get('IsPremiumOrder', False),
# #                     is_prime=row.get('IsPrime', False),
# #                     fulfillment_channel=str(row.get('FulfillmentChannel', "")),
# #                     number_of_items_unshipped=int(row.get('NumberOfItemsUnshipped', 0)),
# #                     has_regulated_items=row.get('HasRegulatedItems', False),
# #                     is_replacement_order=row.get('IsReplacementOrder', False),
# #                     is_sold_by_ab=row.get('IsSoldByAB', False),
# #                     latest_ship_date=converttime(row.get('LatestShipDate', "")) if row.get('LatestShipDate') else "",
# #                     ship_service_level=str(row.get('ShipServiceLevel', "")),
# #                     order_date=order_date,
# #                     is_ispu=row.get('IsISPU', False),
# #                     order_status=str(row.get('OrderStatus', "")),
# #                     shipping_information=row.get('ShippingAddress', {}),
# #                     is_access_point_order=row.get('IsAccessPointOrder', False),
# #                     seller_order_id=str(row.get('SellerOrderId', "")),
# #                     payment_method=str(row.get('PaymentMethod', "")),
# #                     is_business_order=row.get('IsBusinessOrder', False),
# #                     order_total=order_total,
# #                     currency=currency,
# #                     payment_method_details=row.get('PaymentMethodDetails', [])[0] if row.get('PaymentMethodDetails') else "",
# #                     is_global_express_enabled=row.get('IsGlobalExpressEnabled', False),
# #                     last_update_date=converttime(row.get('LastUpdateDate', "")) if row.get('LastUpdateDate') else "",
# #                     shipment_service_level_category=str(row.get('ShipmentServiceLevelCategory', "")),
# #                     automated_shipping_settings=row.get('AutomatedShippingSettings', {}),
# #                     order_details =order_details,
# #                     order_items = order_items
# #                 )
# #                 order.save()
           
# #     return orders



# def ProcessAmazonProductAttributes():
#     marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ['id']).id
#     pipeline = [
#         {
#             "$match": {
#                 "marketplace_id": marketplace_id
#             }
#         },
#         {
#             "$project": {
#                 "_id": 1,
#                 "attributes": {"$ifNull": ["$attributes", {}]},  # If attributes is null, replace with empty dict
#             }
#         },
#     ]
#     product_list = list(Product.objects.aggregate(*(pipeline)))
#     for product_ins in product_list:
#         new_dict = {}
#         for key, value in product_ins['attributes'].items():
#             if isinstance(value, list) and len(value) == 1:  # Check if the field contains a single-item list
#                 single_value = value[0]
#                 filtered_value = {k: v for k, v in single_value.items() if k not in ["language_tag", "marketplace_id"]}
#                 new_dict[key] = filtered_value
#             elif isinstance(value, list):  # Handle multi-item lists if needed
#                 new_dict[key] = [
#                     {k: v for k, v in item.items() if k not in ["language_tag", "marketplace_id"]}
#                     for item in value
#                 ]
#         print(f"Processed attributes for product {product_ins['_id']}: {len(new_dict)}")
#         DatabaseModel.update_documents(Product.objects,{"id" : product_ins['_id']},{"attributes" : new_dict,"old_attributes" : product_ins['attributes']})



# import requests
# from requests.auth import HTTPBasicAuth  # ✅ This is the missing import
# from django.conf import settings
# # ProcessAmazonProductAttributes()
# def get_order_shipping_cost_by_order_number(order_number):
#     url = f"https://ssapi.shipstation.com/orders?orderNumber={order_number}"

#     response = requests.get(
#         url,
#         auth=HTTPBasicAuth(settings.SHIPSTATION_API_KEY, settings.SHIPSTATION_API_SECRET)
#     )

#     if response.status_code == 200:
#         data = response.json()
#         orders = data.get("orders", [])
#         if not orders:
#             return {"error": "No order found with this order number"}
        
#         order = orders[0]  # Assuming orderNumber is unique
#         return {
#     "orderId": order.get("orderId"),
#     "orderNumber": order.get("orderNumber"),
#     "shippingAmount": order.get("shippingAmount", 0.0),
#     "currency": order.get("advancedOptions", {}).get("storeCurrencyCode", "USD")  # fallback for currency
# }
#     else:
#         raise Exception(f"ShipStation Error: {response.status_code} - {response.text}")

# # views.py

# from rest_framework.views import APIView
# from rest_framework.response import Response
# from rest_framework import status
# class ShipStationShippingCostAPIView(APIView):
#     def get(self, request):
#         order_number = request.query_params.get("order_number")
#         if not order_number:
#             return Response({"error": "Missing 'order_number' parameter"}, status=status.HTTP_400_BAD_REQUEST)
        
#         try:
#             cost_info = get_order_shipping_cost_by_order_number(order_number)
#             return Response(cost_info, status=status.HTTP_200_OK)
#         except Exception as e:
#             return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        






# # Sellercloud API endpoints
# TOKEN_URL = "https://onetree.api.sellercloud.us/rest/api/token"
# INVENTORY_URL = "https://onetree.api.sellercloud.us/rest/api/Inventory"

# def get_access_token():
#     payload = {
#         "username": SELLERCLOUD_USERNAME,
#         "password": SELLERCLOUD_PASSWORD
#     }
#     headers = {
#         "Content-Type": "application/json"
#     }
#     response = requests.post(TOKEN_URL, json=payload, headers=headers)
#     if response.status_code == 200:
#         return response.json().get("access_token")
#     else:
#         raise Exception(f"Token fetch failed: {response.text}")

# def fetch_all_inventory(token):
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Accept": "application/json"
#     }

#     page = 1
#     page_size = 50
#     all_inventory = []

#     # First call to get total items
#     params = {
#         "pageNumber": page,
#         "pageSize": page_size,
#         "sortBy": "ProductID",         # Fix here
#         "sortDirection": "Ascending"   # Fix here
#     }

#     first_response = requests.get(INVENTORY_URL, headers=headers, params=params)
#     if first_response.status_code != 200:
#         raise Exception(f"Initial request failed: {first_response.text}")

#     first_data = first_response.json()
#     total_results = first_data.get("TotalResults", 0)
#     total_pages = math.ceil(total_results / page_size)

#     print(f"Total items: {total_results} | Total pages: {total_pages}")
#     all_inventory.extend(first_data.get("Items", []))

#     # Fetch rest of the pages
#     for page in range(2, total_pages + 1):
#         print("page",page)
#         params.update({"pageNumber": page})
#         print("params", params)
#         response = requests.get(INVENTORY_URL, headers=headers, params=params)
#         if response.status_code == 200:
#             items = response.json().get("Items", [])
#             all_inventory.extend(items)
#             print(f"Fetched page {page} — {len(items)} items")
#         else:
#             print(f"Failed page {page}: {response.text}")
#             break

#     return all_inventory


# def sync_inventory():
#     print("🚀 Starting Sellercloud inventory sync...")

#     today = datetime.today()
#     start_of_day = today.replace(hour=0, minute=0, second=0, microsecond=0)
#     end_of_day = today.replace(hour=23, minute=59, second=59, microsecond=999999)

#     try:
#         token = get_access_token()
#         inventory = fetch_all_inventory(token)

#         for ins in inventory:
#             sku = str(ins.get('ID'))
#             try:
#                 product_obj = DatabaseModel.get_document(Product.objects, {"sku": sku}, ['id'])
#                 if not product_obj:
#                     print(f"⚠️ Product not found for SKU: {sku}")
#                     continue

#                 # Check if inventory log already exists for today
#                 pipeline = [
#                     {
#                         "$match": {
#                             "product_id": product_obj.id,
#                             "date": {"$gte": start_of_day, "$lte": end_of_day}
#                         }
#                     },
#                     {"$limit": 1}
#                 ]

#                 existing_entry = list(inventry_log.objects.aggregate(*pipeline))
#                 existing_entry = existing_entry[0] if existing_entry else None

#                 if existing_entry:
#                     # Update existing log
#                     DatabaseModel.update_documents(
#                         inventry_log.objects,
#                         {"id": existing_entry['_id']},
#                         {
#                             "available": ins.get('InventoryAvailableQty'),
#                             "reserved": ins.get('ReservedQty')
#                         }
#                     )
#                     print(f"🔁 Updated inventory log for SKU {sku}")
#                 else:
#                     # Create new inventory log
#                     new_inventory_log = inventry_log(
#                         product_id=product_obj,
#                         available=ins.get('InventoryAvailableQty'),
#                         reserved=ins.get('ReservedQty'),
#                         date=datetime.now()
#                     )
#                     new_inventory_log.save()
#                     print(f"✅ Created inventory log for SKU {sku}")

#                 # Update product quantity
#                 DatabaseModel.update_documents(
#                     Product.objects,
#                     {"sku": sku},
#                     {"quantity": ins.get('InventoryAvailableQty')}
#                 )

#             except Exception as inner_err:
#                 print(f"❌ Failed to process SKU {sku}: {inner_err}")

#     except Exception as e:
#         print("🔥 Critical Error in sync_inventory:", e)

# def save_or_update_pageview_session_count(data, today_date):
#     try:
#         # Extract data from the input dictionary
#         product_id = data.get("parentAsin")
#         sales_by_asin = data.get("salesByAsin", {})
#         traffic_by_asin = data.get("trafficByAsin", {})
#         ids = DatabaseModel.list_documents(Product.objects,{"product_id" : product_id},['id'])
#         if ids:
#             ids = [ins.id for ins in ids]



#             # Check if a document with the same ASIN and today's date already exists
#             existing_record = pageview_session_count.objects(asin=product_id, date=today_date).first()

#             if existing_record:
#                 # Update the existing record
#                 existing_record.update(
#                     units_ordered=sales_by_asin.get("unitsOrdered", existing_record.units_ordered),
#                     units_ordered_b2b=sales_by_asin.get("unitsOrderedB2B", existing_record.units_ordered_b2b),
#                     ordered_product_sales_amount=sales_by_asin.get("orderedProductSales", {}).get("amount", existing_record.ordered_product_sales_amount),
#                     ordered_product_sales_currency_code=sales_by_asin.get("orderedProductSales", {}).get("currencyCode", existing_record.ordered_product_sales_currency_code),
#                     ordered_product_sales_b2b_amount=sales_by_asin.get("orderedProductSalesB2B", {}).get("amount", existing_record.ordered_product_sales_b2b_amount),
#                     ordered_product_sales_b2b_currency_code=sales_by_asin.get("orderedProductSalesB2B", {}).get("currencyCode", existing_record.ordered_product_sales_b2b_currency_code),
#                     total_order_items=sales_by_asin.get("totalOrderItems", existing_record.total_order_items),
#                     total_order_items_b2b=sales_by_asin.get("totalOrderItemsB2B", existing_record.total_order_items_b2b),
#                     browser_sessions=traffic_by_asin.get("browserSessions", existing_record.browser_sessions),
#                     browser_sessions_b2b=traffic_by_asin.get("browserSessionsB2B", existing_record.browser_sessions_b2b),
#                     mobile_app_sessions=traffic_by_asin.get("mobileAppSessions", existing_record.mobile_app_sessions),
#                     mobile_app_sessions_b2b=traffic_by_asin.get("mobileAppSessionsB2B", existing_record.mobile_app_sessions_b2b),
#                     sessions=traffic_by_asin.get("sessions", existing_record.sessions),
#                     sessions_b2b=traffic_by_asin.get("sessionsB2B", existing_record.sessions_b2b),
#                     browser_session_percentage=traffic_by_asin.get("browserSessionPercentage", existing_record.browser_session_percentage),
#                     browser_session_percentage_b2b=traffic_by_asin.get("browserSessionPercentageB2B", existing_record.browser_session_percentage_b2b),
#                     mobile_app_session_percentage=traffic_by_asin.get("mobileAppSessionPercentage", existing_record.mobile_app_session_percentage),
#                     mobile_app_session_percentage_b2b=traffic_by_asin.get("mobileAppSessionPercentageB2B", existing_record.mobile_app_session_percentage_b2b),
#                     session_percentage=traffic_by_asin.get("sessionPercentage", existing_record.session_percentage),
#                     session_percentage_b2b=traffic_by_asin.get("sessionPercentageB2B", existing_record.session_percentage_b2b),
#                     browser_page_views=traffic_by_asin.get("browserPageViews", existing_record.browser_page_views),
#                     browser_page_views_b2b=traffic_by_asin.get("browserPageViewsB2B", existing_record.browser_page_views_b2b),
#                     mobile_app_page_views=traffic_by_asin.get("mobileAppPageViews", existing_record.mobile_app_page_views),
#                     mobile_app_page_views_b2b=traffic_by_asin.get("mobileAppPageViewsB2B", existing_record.mobile_app_page_views_b2b),
#                     page_views=traffic_by_asin.get("pageViews", existing_record.page_views),
#                     page_views_b2b=traffic_by_asin.get("pageViewsB2B", existing_record.page_views_b2b),
#                     browser_page_views_percentage=traffic_by_asin.get("browserPageViewsPercentage", existing_record.browser_page_views_percentage),
#                     browser_page_views_percentage_b2b=traffic_by_asin.get("browserPageViewsPercentageB2B", existing_record.browser_page_views_percentage_b2b),
#                     mobile_app_page_views_percentage=traffic_by_asin.get("mobileAppPageViewsPercentage", existing_record.mobile_app_page_views_percentage),
#                     mobile_app_page_views_percentage_b2b=traffic_by_asin.get("mobileAppPageViewsPercentageB2B", existing_record.mobile_app_page_views_percentage_b2b),
#                     page_views_percentage=traffic_by_asin.get("pageViewsPercentage", existing_record.page_views_percentage),
#                     page_views_percentage_b2b=traffic_by_asin.get("pageViewsPercentageB2B", existing_record.page_views_percentage_b2b),
#                     buy_box_percentage=traffic_by_asin.get("buyBoxPercentage", existing_record.buy_box_percentage),
#                     buy_box_percentage_b2b=traffic_by_asin.get("buyBoxPercentageB2B", existing_record.buy_box_percentage_b2b),
#                     unit_session_percentage=traffic_by_asin.get("unitSessionPercentage", existing_record.unit_session_percentage),
#                     unit_session_percentage_b2b=traffic_by_asin.get("unitSessionPercentageB2B", existing_record.unit_session_percentage_b2b),
#                 )
#                 print("Record updated successfully!")
#             else:
#                 # Create a new record
#                 pageview_session = pageview_session_count(
#                     product_id = ids,
#                     asin=product_id,
#                     units_ordered=sales_by_asin.get("unitsOrdered", 0),
#                     units_ordered_b2b=sales_by_asin.get("unitsOrderedB2B", 0),
#                     ordered_product_sales_amount=sales_by_asin.get("orderedProductSales", {}).get("amount", 0.0),
#                     ordered_product_sales_currency_code=sales_by_asin.get("orderedProductSales", {}).get("currencyCode", "USD"),
#                     ordered_product_sales_b2b_amount=sales_by_asin.get("orderedProductSalesB2B", {}).get("amount", 0.0),
#                     ordered_product_sales_b2b_currency_code=sales_by_asin.get("orderedProductSalesB2B", {}).get("currencyCode", "USD"),
#                     total_order_items=sales_by_asin.get("totalOrderItems", 0),
#                     total_order_items_b2b=sales_by_asin.get("totalOrderItemsB2B", 0),
#                     browser_sessions=traffic_by_asin.get("browserSessions", 0),
#                     browser_sessions_b2b=traffic_by_asin.get("browserSessionsB2B", 0),
#                     mobile_app_sessions=traffic_by_asin.get("mobileAppSessions", 0),
#                     mobile_app_sessions_b2b=traffic_by_asin.get("mobileAppSessionsB2B", 0),
#                     sessions=traffic_by_asin.get("sessions", 0),
#                     sessions_b2b=traffic_by_asin.get("sessionsB2B", 0),
#                     browser_session_percentage=traffic_by_asin.get("browserSessionPercentage", 0.0),
#                     browser_session_percentage_b2b=traffic_by_asin.get("browserSessionPercentageB2B", 0.0),
#                     mobile_app_session_percentage=traffic_by_asin.get("mobileAppSessionPercentage", 0.0),
#                     mobile_app_session_percentage_b2b=traffic_by_asin.get("mobileAppSessionPercentageB2B", 0.0),
#                     session_percentage=traffic_by_asin.get("sessionPercentage", 0.0),
#                     session_percentage_b2b=traffic_by_asin.get("sessionPercentageB2B", 0.0),
#                     browser_page_views=traffic_by_asin.get("browserPageViews", 0),
#                     browser_page_views_b2b=traffic_by_asin.get("browserPageViewsB2B", 0),
#                     mobile_app_page_views=traffic_by_asin.get("mobileAppPageViews", 0),
#                     mobile_app_page_views_b2b=traffic_by_asin.get("mobileAppPageViewsB2B", 0),
#                     page_views=traffic_by_asin.get("pageViews", 0),
#                     page_views_b2b=traffic_by_asin.get("pageViewsB2B", 0),
#                     browser_page_views_percentage=traffic_by_asin.get("browserPageViewsPercentage", 0.0),
#                     browser_page_views_percentage_b2b=traffic_by_asin.get("browserPageViewsPercentageB2B", 0.0),
#                     mobile_app_page_views_percentage=traffic_by_asin.get("mobileAppPageViewsPercentage", 0.0),
#                     mobile_app_page_views_percentage_b2b=traffic_by_asin.get("mobileAppPageViewsPercentageB2B", 0.0),
#                     page_views_percentage=traffic_by_asin.get("pageViewsPercentage", 0.0),
#                     page_views_percentage_b2b=traffic_by_asin.get("pageViewsPercentageB2B", 0.0),
#                     buy_box_percentage=traffic_by_asin.get("buyBoxPercentage", 0.0),
#                     buy_box_percentage_b2b=traffic_by_asin.get("buyBoxPercentageB2B", 0.0),
#                     unit_session_percentage=traffic_by_asin.get("unitSessionPercentage", 0.0),
#                     unit_session_percentage_b2b=traffic_by_asin.get("unitSessionPercentageB2B", 0.0),
#                     date=today_date
#                 )
#                 pageview_session.save()
#                 print("New record created successfully!")
#     except Exception as e:
#         print(f"An error occurred: {e}")


# def fetch_sales_traffic_report(aws_access_key_id,aws_secret_access_key,role_arn,client_id,client_secret,refresh_token,report_date,region="us-east-1"):
#     # 1. Assume Role
#     sts_client = boto3.client(
#         'sts',
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#         region_name=region
#     )
#     assumed_role = sts_client.assume_role(
#         RoleArn=role_arn,
#         RoleSessionName="SPAPISession"
#     )['Credentials']

#     # 2. Get LWA Access Token
#     token_url = "https://api.amazon.com/auth/o2/token"
#     token_data = {
#         "grant_type": "refresh_token",
#         "refresh_token": refresh_token,
#         "client_id": client_id,
#         "client_secret": client_secret
#     }
#     lwa_token = requests.post(token_url, data=token_data).json()
#     access_token = lwa_token['access_token']

#     # 3. Create Report
#     create_report_url = "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports"
#     headers = {
#         "x-amz-access-token": access_token,
#         "content-type": "application/json",
#         "host": "sellingpartnerapi-na.amazon.com"
#     }

#     body = {
#         "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
#         "marketplaceIds": ["ATVPDKIKX0DER"],  # Amazon US marketplace
#         "dataStartTime": report_date + "T00:00:00Z",
#         "dataEndTime": report_date + "T23:59:59Z",
#         "reportOptions": {
#         "dateGranularity": "DAY",
#         "asinGranularity": "CHILD"
#     }
#     }

#     report_res = requests.post(create_report_url, headers=headers, data=json.dumps(body))
#     report_id = report_res.json().get("reportId")
#     print("pppppppppppppppppppppppp",report_id)
#     if not report_id:
#         raise Exception("Failed to create report: " + report_res.text)

#     # 4. Poll Report Status
#     status_url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}"
#     while True:
#         status_res = requests.get(status_url, headers=headers)
#         status_data = status_res.json()
#         processing_status = status_data.get("processingStatus")
#         print("1111111111111111111111",processing_status)

#         if processing_status == "DONE":
#             report_document_id = status_data.get("reportDocumentId")
#             break
#         elif processing_status in ("CANCELLED", "FATAL","IN_QUEUE"):
#             print(f"Report failed with status: {processing_status}")
#         time.sleep(30)

#     # 5. Get Report Document URL
#     document_url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{report_document_id}"
#     doc_res = requests.get(document_url, headers=headers)
#     report_url = doc_res.json().get("url")
#     compression_algo = doc_res.json().get("compressionAlgorithm", None)

#     # 6. Download and Parse Report
#     report_file = requests.get(report_url)
#     if compression_algo == "GZIP":
#         buf = gzip.GzipFile(fileobj=io.BytesIO(report_file.content))
#         content = buf.read().decode("utf-8")
#     else:
#         content = report_file.text

#     report_data = json.loads(content)
#     print(report_data)
#     report_start_date = datetime.strptime(report_date, '%Y-%m-%d')
#     for ins in report_data.get('salesAndTrafficByAsin',[]):
#         save_or_update_pageview_session_count(ins, report_start_date)

#     return True  # full raw JSON report

# def syncPageviews():
#     start_date = datetime.today() - timedelta(days=2)
#     report_date = start_date.strftime("%Y-%m-%d")
#     fetch_sales_traffic_report(Acccess_Key, Secret_Access_Key, Role_ARN,AMAZON_API_KEY,AMAZON_SECRET_KEY,REFRESH_TOKEN,report_date,region="us-east-1")
#     return True


# def get_and_download_report(sp_api_client, report_type, start_time, end_time):
#     TARGET_MARKETPLACE = Marketplaces.US
#     try:
#         report_request_payload = {
#             "reportType": report_type,
#             "marketplaceIds": [TARGET_MARKETPLACE.marketplace_id],
#             "dataStartTime": start_time.isoformat().split('.')[0] + 'Z',
#             "dataEndTime": end_time.isoformat().split('.')[0] + 'Z'
#         }

#         print(f"  Data time range: {report_request_payload['dataStartTime']} to {report_request_payload['dataEndTime']}")

#         # 1. Create the report request
#         try:
#             create_report_response = sp_api_client.create_report(**report_request_payload)
#             report_id = create_report_response.payload['reportId']
#             print(f"Report request submitted. Report ID: {report_id}")
#         except SellingApiException as e:
#             print(f"API error submitting report request {report_type}: {e}")
#             return None
#         except Exception as e:
#             print(f"An unexpected error during report submission: {e}")
#             return None

#         # 2. Poll for report completion
#         print("Polling report status...")
#         report_status = None
#         report_document_id = None
#         max_polls = 60
#         poll_interval = 30

#         for i in range(max_polls):
#             time.sleep(poll_interval)
#             try:
#                 get_report_response = sp_api_client.get_report(reportId=report_id)
#                 report_status = get_report_response.payload['processingStatus']

#                 print(f" Poll {i+1}/{max_polls}: Status = {report_status}")

#                 if report_status == 'DONE':
#                     report_document_id = get_report_response.payload['reportDocumentId']
#                     print("Report processing complete.")
#                     break
#                 elif report_status in ['FATAL', 'CANCELLED']:
#                     print(f"Report processing failed with status: {report_status}")
#                     return None
#             except SellingApiException as e:
#                 print(f"API error while polling report {report_id}: {e}")
#             except Exception as e:
#                 print(f"An unexpected error occurred while polling report {report_id}: {e}")

#         if report_status != 'DONE' or not report_document_id:
#             print(f"Report {report_id} did not complete successfully within the polling limit.")
#             return None

#         # 3. Download the report content (as bytes)
#         print(f"Fetching report document {report_document_id}...")
#         try:
#             report_document = sp_api_client.get_report_document(reportDocumentId=report_document_id)
#             report_content_url = report_document.payload['url']
#             response = requests.get(report_content_url, stream=True)
#             if response.status_code == 200:
#                 report_bytes = response.content
#                 print("Report content downloaded successfully.")
#                 return report_bytes
#             else:
#                 print(f"Failed to download report. Status code: {response.status_code}")
#                 return None

#         except Exception as e:
#             print(f"An unexpected error occurred during report download: {e}")
#             return None

#     except Exception as e:
#         print(f"An unexpected error occurred during the overall report process: {e}")
#         return None

# def ordersAmazon():
#     TARGET_MARKETPLACE = Marketplaces.US  # Update for your target

#     ORDERS_REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"
#     end_time = datetime.utcnow()
#     start_time = end_time - timedelta(days=3)
#     print("Starting Amazon SP-API orders retrieval script for last 3 days...")

#     # Initialize the SP-API client credentials
#     credentials = {
#         'lwa_app_id': AMAZON_API_KEY,
#         'lwa_client_secret': AMAZON_SECRET_KEY,
#         'refresh_token': REFRESH_TOKEN,
#         'aws_access_key': Acccess_Key,
#         'aws_secret_key': Secret_Access_Key,
#         'role_arn': Role_ARN
#     }

#     try:
#         reports_client = Reports(
#             credentials=credentials,
#             marketplace=TARGET_MARKETPLACE
#         )
#     except Exception as e:
#         print(f"SP-API client initialization failed: {e}")
#         sys.exit(1)

#     # Get the Orders Report for the last 3 days
#     orders_content = get_and_download_report(
#         reports_client,
#         ORDERS_REPORT_TYPE,
#         start_time=start_time,
#         end_time=end_time
#     )
#     if not orders_content:
#         print("No orders content received.")
#         return None

#     # Decompress and load orders into DataFrame
#     orders_df = None
#     try:
#         try:
#             # Try decompressing (usual case, gzipped)
#             with gzip.GzipFile(fileobj=BytesIO(orders_content)) as gz:
#                 report_content_str = gz.read().decode('utf-8')
#         except OSError:
#             # Not gzipped (rare fallback)
#             report_content_str = orders_content.decode('utf-8')
#         orders_df = pd.read_csv(StringIO(report_content_str), sep='\t', low_memory=False)
#     except pd.errors.EmptyDataError:
#         print("The downloaded report was empty. No orders found in the specified time range.")
#         orders_df = None
#     except Exception as e:
#         print(f"Error reading orders report content into DataFrame: {e}")
#         orders_df = None

#     print(f"Orders DataFrame loaded: {orders_df.shape if orders_df is not None else 'None'}")
#     return orders_df


# def syncRecentAmazonOrders():
#     def safe_convert(from_currency, amount):
#             fallback_rates = {
#                 'MXN': 0.058,  # e.g., 1 MXN = 0.058 USD (update this as needed)
#                 'CAD': 0.73    # e.g., 1 CAD = 0.73 USD (update this as needed)
#             }
#             return amount * fallback_rates.get(from_currency, 1)  # fallback: use predefined rates or return original amount

#     def process_row(row, marketplace_id):
#         s = row
#         if s['sales-channel'] != "Non-Amazon":
#             purchase_order_id = str(s['amazon-order-id']) if pd.notna(s.get('amazon-order-id')) else ""
#             items_order_quantity = int(s['quantity']) if pd.notna(s.get('quantity')) else 0
#             shipping_price = float(s['shipping-price']) if pd.notna(s.get('shipping-price')) else 0.0
#             merchant_order_id = str(s['merchant-order-id']) if pd.notna(s.get('merchant-order-id')) else ""
#             order_status = str(s['order-status']) if pd.notna(s.get('order-status')) else ""
#             sales_channel = str(s['sales-channel']) if pd.notna(s.get('sales-channel')) else ""
            

#             order_obj = Order.objects(purchase_order_id=purchase_order_id).only('id', 'order_items').first()
#             if order_obj:
#                 DatabaseModel.update_documents(Order.objects, {"purchase_order_id": purchase_order_id}, {
#                     "items_order_quantity": items_order_quantity,
#                     "shipping_price": shipping_price,
#                     "sales_channel": sales_channel,
#                     "merchant_order_id": merchant_order_id,
#                     "order_status": order_status
#                 })
#             else:
#                 product_name = str(s['product-name']) if pd.notna(s.get('product-name')) else ""
#                 sku = str(s['sku']) if pd.notna(s.get('sku')) else ""
#                 asin = str(s['asin']) if pd.notna(s.get('asin')) else ""
#                 number_of_items = int(s['number-of-items']) if pd.notna(s.get('number-of-items')) else 0
#                 currency = str(s['currency']) if pd.notna(s.get('currency')) else "USD"
#                 item_price = float(s['item-price']) if pd.notna(s.get('item-price')) else 0.0
#                 item_tax = float(s['item-tax']) if pd.notna(s.get('item-tax')) else 0.0
#                 shipping_tax = float(s['shipping-tax']) if pd.notna(s.get('shipping-tax')) else 0.0
#                 item_promotion_discount = float(s['item-promotion-discount']) if pd.notna(s.get('item-promotion-discount')) else 0.0
#                 item_promotion_discount_tax = float(s['item-promotion-discount-tax']) if pd.notna(s.get('item-promotion-discount-tax')) else 0.0

#                 if currency != "USD":
#                     item_price = safe_convert(currency, item_price)
#                     item_tax = safe_convert(currency, item_tax)
#                     shipping_price = safe_convert(currency, shipping_price)
#                     shipping_tax = safe_convert(currency, shipping_tax)
#                     item_promotion_discount = safe_convert(currency, item_promotion_discount)
#                     item_promotion_discount_tax = safe_convert(currency, item_promotion_discount_tax)

#                 ship_address = {}
#                 if pd.notna(s.get('ship-city')): ship_address['City'] = str(s['ship-city'])
#                 if pd.notna(s.get('ship-state')): ship_address['StateOrRegion'] = str(s['ship-state'])
#                 if pd.notna(s.get('ship-postal-code')): ship_address['PostalCode'] = str(s['ship-postal-code'])
#                 if pd.notna(s.get('ship-country')): ship_address['CountryCode'] = str(s['ship-country'])

                
#                 order_date = datetime.strptime(s['purchase-date'], "%Y-%m-%dT%H:%M:%S%z") if pd.notna(s.get('purchase-date')) else datetime.now()
#                 last_update_date = datetime.strptime(s['last-updated-date'], "%Y-%m-%dT%H:%M:%S%z") if pd.notna(s.get('last-updated-date')) else datetime.now()
#                 fulfillment_channel = "MFN" if s['fulfillment-channel'] == "Merchant" else "AFN"
#                 ship_service_level = str(s['ship-service-level']) if pd.notna(s.get('ship-service-level')) else ""
#                 p_obj = Product.objects(sku=sku).only('id').first()
#                 product_id = p_obj.id if p_obj else None

#                 order_item = OrderItems(
#                     OrderId=purchase_order_id,
#                     Platform="Amazon",
#                     created_date=order_date,
#                     ProductDetails=ProductDetails(
#                         product_id=product_id,
#                         Title=product_name,
#                         SKU=sku,
#                         ASIN=asin,
#                         QuantityOrdered=items_order_quantity,
#                         QuantityShipped=items_order_quantity,
#                     ),
#                     Pricing=Pricing(
#                         ItemPrice=Money(**{"CurrencyCode": "USD", "Amount": item_price}),
#                         ItemTax=Money(**{"CurrencyCode": "USD", "Amount": item_tax}),
#                         PromotionDiscount=Money(**{"CurrencyCode": "USD", "Amount": item_promotion_discount})
#                     )
#                 )
#                 order_item.save()

#                 order = Order(
#                     marketplace_id=marketplace_id,
#                     purchase_order_id=purchase_order_id,
#                     last_update_date=last_update_date,
#                     sales_channel=sales_channel,
#                     items_order_quantity=items_order_quantity,
#                     shipping_price=shipping_price,
#                     number_of_items_shipped=number_of_items,
#                     fulfillment_channel=fulfillment_channel,
#                     ship_service_level=ship_service_level,
#                     order_date=order_date,
#                     order_status=order_status,
#                     shipping_information=ship_address,
#                     merchant_order_id=merchant_order_id,
#                     order_total=item_price + item_tax + shipping_price + shipping_tax - item_promotion_discount - item_promotion_discount_tax,
#                     currency=currency,
#                     order_items=[order_item.id]
#                 )
#                 order.save()

#     df = ordersAmazon()
#     if df is not None and not df.empty:
#         marketplace_id = DatabaseModel.get_document(Marketplace.objects, {"name": "Amazon"}, ['id']).id
#         process_count =0
#         for _, row in df.iterrows():
#             process_count += 1
#             print(f"Processed {process_count} rows...")
#             process_row(row, marketplace_id)
#         print("Amazon orders synced successfully.")

#         return True
    

# syncRecentAmazonOrders()
    

# def FetchProductsDetails():
#     credentials = {
#         'lwa_app_id': AMAZON_API_KEY,
#         'lwa_client_secret': AMAZON_SECRET_KEY,
#         'refresh_token': REFRESH_TOKEN,
#         'aws_access_key': Acccess_Key,
#         'aws_secret_key': Secret_Access_Key,
#         'role_arn': Role_ARN
#     }
syncRecentAmazonOrders()
    

def FetchProductsDetails():
    credentials = {
        'lwa_app_id': AMAZON_API_KEY,
        'lwa_client_secret': AMAZON_SECRET_KEY,
        'refresh_token': REFRESH_TOKEN,
        'aws_access_key': Acccess_Key,
        'aws_secret_key': Secret_Access_Key,
        'role_arn': Role_ARN
    }

#     report = Reports(
#         marketplace=Marketplaces.US,
#         credentials=credentials
#     ).create_report(reportType='GET_MERCHANT_LISTINGS_ALL_DATA')

#     report_id = report.payload['reportId']
#     print(f"Report created with ID: {report_id}")

#     while True:
#         response = Reports(marketplace=Marketplaces.US, credentials=credentials).get_report(report_id)
#         report_status = response.payload['processingStatus']
#         print(f"Report status: {report_status}")

#         if report_status == 'DONE':
#             break
#         elif report_status in ['CANCELLED', 'FATAL']:
#             raise Exception(f"Report {report_id} failed with status: {report_status}")

#         time.sleep(30)

#     document_id = response.payload['reportDocumentId']
#     doc = Reports(marketplace=Marketplaces.US, credentials=credentials).get_report_document(reportDocumentId=document_id)
#     download_url = doc.payload['url']

#     res = requests.get(download_url)
#     res.raise_for_status()

#     if 'compressionAlgorithm' in doc.payload and doc.payload['compressionAlgorithm'] == 'GZIP':
#         buf = BytesIO(res.content)
#         with gzip.GzipFile(fileobj=buf) as f:
#             raw_bytes = f.read()
#     else:
#         raw_bytes = res.content

#     try:
#         content = raw_bytes.decode('utf-8')
#     except UnicodeDecodeError:
#         try:
#             content = raw_bytes.decode('cp1252')
#         except UnicodeDecodeError:
#             content = raw_bytes.decode('latin1')

#     df = pd.read_csv(StringIO(content), sep='\t', low_memory=False)
#     # Print all rows line by line
#     for index, row in df.iterrows():
        
#         row_data = row.to_dict()
#         sku = ""
#         if pd.notna(row_data['seller-sku']):
#             sku = row_data['seller-sku']

#         price = 0.0
#         if pd.notna(row_data['price']):
#             price = row_data['price']

#         published_status = "Active"
#         if pd.notna(row_data['status']):
#             published_status = row_data['status']

#         product_obj = DatabaseModel.get_document(Product.objects,{"sku" : sku},['id','price'])
#         if product_obj:
#             if product_obj.price != price:
#                 DatabaseModel.update_documents(Product.objects, {"sku": sku}, {"published_status": published_status,"price": price})
#                 productPriceChange(
#                     product_id=product_obj.id,
#                     old_price=product_obj.price,
#                     new_price=price,
#                     reason="Price updated from Amazon report"
#                 ).save()

#     return True