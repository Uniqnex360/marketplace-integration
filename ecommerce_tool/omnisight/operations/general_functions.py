import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.walmart_utils import getAccesstoken
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId
import json
from rest_framework.parsers import JSONParser
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime, timedelta



def getMarketplaceList(request):
    pipeline = [
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "name" : 1,
                "image_url" : 1,
            }
        }
    ]
    marketplace_list = list(Marketplace.objects.aggregate(*(pipeline)))
    return marketplace_list

#---------------------------------------------PRODUCT APIS---------------------------------------------------
@csrf_exempt
def getProductList(request):
    data = dict()
    json_request = JSONParser().parse(request)
    print(json_request)
    marketplace_id = json_request.get('marketplace_id')
    skip = int(json_request.get('skip'))
    limit = int(json_request.get('limit'))
    search_query = json_request.get('search_query')   
    marketplace = json_request.get('marketplace')
    category_name = json_request.get('category_name')
    brand_id_list = json_request.get('brand_id_list')
    sort_by = json_request.get('sort_by')
    sort_by_value = json_request.get('sort_by_value')
    pipeline = []
    count_pipeline = []
    match = {}
    if marketplace_id != None and marketplace_id != "":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if category_name != None and category_name != "" and category_name != []:
        match['category'] = {"$in":category_name}
    if brand_id_list != None and brand_id_list != "" and brand_id_list != []:
        match['brand_id'] = {"$in":[ObjectId(brand_id) for brand_id in brand_id_list]}
    if search_query != None and search_query != "":
        search_query = search_query.strip() 
        match["$or"] = [
            {"product_title": {"$regex": search_query, "$options": "i"}},
            {"sku": {"$regex": search_query, "$options": "i"}},
        ]
    if match != {}:
        match_pipeline = {
            "$match" : match}
        print(match_pipeline)
        pipeline.append(match_pipeline)
        count_pipeline.append(match_pipeline)
    pipeline.extend([
        {
            "$lookup" : {
                "from" : "marketplace",
                "localField" : "marketplace_id",
                "foreignField" : "_id",
                "as" : "marketplace"
            }
        },
        {
            "$unwind" : "$marketplace"
        },
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "product_title" : 1,
                "product_id" : 1,
                "sku" : 1,
                "asin" : {"$ifNull" : ["$asin",""]},  # If asin is null, replace with empty string
                "price" : 1,
                "quantity" : 1,
                "published_status" : 1,
                "category" : {"$ifNull" : ["$category",""]},  # If category is null, replace with empty string
                "image_url" : {"$ifNull" : ["$image_url",""]},  # If image_url is null, replace with empty string
                "marketplace_name" : "$marketplace.name",
                # "marketplace_image_url" : "$marketplace.image_url"
            }
        },
        {
            "$skip" : skip
        },
        {
            "$limit" : limit+skip
        }
    ])
    if sort_by != None and sort_by != "":
        sort = {
            "$sort" : {
                sort_by : int(sort_by_value)
            }
        }
        pipeline.append(sort)
    product_list = list(Product.objects.aggregate(*(pipeline)))
    # Get total product count
    count_pipeline.extend([
        {
            "$count": "total_count"
        }
    ])
    total_count_result = list(Product.objects.aggregate(*(count_pipeline)))
    total_count = total_count_result[0]['total_count'] if total_count_result else 0

    data['total_count'] = total_count
    data['product_list'] = product_list
    return data



def getProductCategoryList(request):
    data = dict()
    marketplace_id = request.GET.get('marketplace_id')
    match = {}
    if marketplace_id != None and marketplace_id != "":
        match['marketplace_id'] = ObjectId(marketplace_id)
    match['end_level'] = True
    pipeline = [
        {
            "$match": match    
        },
        {
            "$lookup": {
                "from": "product",
                "localField": "name",
                "foreignField": "category",
                "as": "products"
            }
        },
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "name": 1,
                "product_count": {"$size": "$products"}
            }
        },
    ]
    category_list = list(Category.objects.aggregate(*(pipeline)))
    data['category_list'] = category_list
    return data



def getBrandList(request):
    data = dict()
    marketplace_id = request.GET.get('marketplace_id')
    pipeline = []
    if marketplace_id != None and marketplace_id != "":
        match = {
            "$match" : {
                "marketplace_id" : ObjectId(marketplace_id)
            }
        }
        pipeline.append(match)
    pipeline.extend([
        {
            "$lookup": {
                "from": "product",
                "localField": "_id",
                "foreignField": "brand_id",
                "as": "products"
            }
        },
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "name" : 1,
                "product_count": {"$size": "$products"}
            }
        }
    ])
    brand_list = list(Brand.objects.aggregate(*(pipeline)))
    data['brand_list'] = brand_list
    return data


def fetchProductDetails(request):
    data = dict()
    product_id = request.GET.get('product_id')
    pipeline = [
        {
            "$match" : {
                "_id" : ObjectId(product_id)
            }
        },
        {
            "$lookup": {
                "from": "marketplace",
                "localField": "marketplace_id",
                "foreignField": "_id",
                "as": "marketplace_ins"
            }
        },
        {
            "$unwind": "$marketplace_ins"
        },
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "product_title" : {"$ifNull" : ["$product_title", ""]},
                "product_description" : {"$ifNull" : ["$product_description", ""]},
                "product_id" : {"$ifNull" : ["$product_id", ""]},
                "product_id_type" : {"$ifNull" : ["$product_id_type", ""]},
                "sku" : {"$ifNull" : ["$sku", ""]},
                "price" : {"$ifNull" : ["$price", 0]},
                "currency" : {"$ifNull" : ["$currency", ""]},
                "quantity" : {"$ifNull" : ["$quantity", 0]},
                "published_status" : 1,
                "marketplace_ins" : "$marketplace_ins.name",
                "marketplace_image_url" : "$marketplace_ins.image_url",
                "item_condition" : {"$ifNull" : ["$item_condition", ""]},
                "item_note" : {"$ifNull" : ["$item_note", ""]},
                "listing_id" : {"$ifNull" : ["$listing_id", ""]},
                "upc" : {"$ifNull" : ["$upc", ""]},
                "gtin" : {"$ifNull" : ["$gtin", ""]},
                "asin" : {"$ifNull" : ["$asin", ""]},
                "model_number" : {"$ifNull" : ["$model_number", ""]},
                "category" : {"$ifNull" : ["$category", ""]},
                "brand_name" : {"$ifNull" : ["$brand_name", ""]},
                "manufacturer_name" : {"$ifNull" : ["$manufacturer_name", ""]},
                "attributes" : {"$ifNull" : ["$attributes", {}]},
                "features" : {"$ifNull" : ["$features", []]},
                "shelf_path" : {"$ifNull" : ["$shelf_path", ""]},
                "image_url" : {"$ifNull" : ["$image_url", ""]},
                "image_urls" : {"$ifNull" : ["$image_urls", []]}
            }
        }
    ]
    product_details = list(Product.objects.aggregate(*(pipeline)))
    if len(product_details):
        data = product_details[0]
    return data


def getOrdersBasedOnProduct(request):
    data = dict()
    product_id = request.GET.get('product_id')

    pipeline = [
        {
            "$match": {
                "_id": ObjectId(product_id)  # Ensure product_id is a valid ObjectId
            }
        },
        {
            "$lookup": {
                "from": "marketplace",
                "localField": "marketplace_id",
                "foreignField": "_id",
                "as": "marketplace_ins"
            }
        },
        {
            "$unwind": {
                "path": "$marketplace_ins",
            }
        },
        {
            "$lookup": {
                "from": "order",
                "let": {
                    "marketplace_id": "$marketplace_id",
                    "product_title": "$product_title",
                    "marketplace_name": "$marketplace_ins.name"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": ["$marketplace_id", "$$marketplace_id"]
                            }
                        }
                    },
                    {
                        "$addFields": {
                            "filtered_order": {
                                "$filter": {
                                    "input": "$order_details",
                                    "as": "item",
                                    "cond": {
                                        "$or": [
                                            {
                                                "$and": [
                                                    {"$eq": ["$$marketplace_name", "Amazon"]},
                                                    {"$eq": ["$$item.Title", "$$product_title"]}
                                                ]
                                            },
                                            {
                                                "$and": [
                                                    {"$ne": ["$$marketplace_name", "Amazon"]},
                                                    {"$eq": ["$$item.item.productName", "$$product_title"]}
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "filtered_order": {"$ne": []}  # Ensure at least one matching order exists
                        }
                    }
                ],
                "as": "order_ins"
            }
        },
        {
            "$unwind": {
                "path": "$order_ins",
                "preserveNullAndEmptyArrays": True  # Ensure the product is not removed if no orders exist
            }
        },
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$order_ins._id"},
                "purchase_order_id": "$order_ins.purchase_order_id",
                "order_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": "$order_ins.order_date",
                    }
                },
                "order_status": "$order_ins.order_status",
                "order_total": "$order_ins.order_total",
                "currency": "$order_ins.currency",
                "marketplace_name": "$marketplace_ins.name",
                "filtered_order": {"$arrayElemAt": ["$order_ins.filtered_order", 0]}  # Get first matching order item
            }
        },
        {
            "$project": {
                "_id": 0,
                "id": 1,
                "purchase_order_id": 1,
                "order_date": 1,
                "order_status": 1,
                "order_total": 1,
                "currency": 1,
                "marketplace_name": 1
            }
        }
    ]

    orders = list(Product.objects.aggregate(*pipeline))
    if len(orders) == 1 and orders[0]['id'] == None:
        orders =[]

    data['orders'] = orders
    return data


#---------------------------------ORDER APIS-------------------------------------------------------------------


@csrf_exempt
def fetchAllorders(request):
    data = dict()
    orders = []
    pipeline = []
    count_pipeline = []

    json_request = JSONParser().parse(request)
    print("111111111111111111111111111111111111111111111111111111")
    print("json_request",json_request)
    user_id = json_request.get('user_id')
    limit = int(json_request.get('limit', 100))  # Default limit = 100 if not provided
    skip = int(json_request.get('skip', 0))  # Default skip = 0 if not provided
    market_place_id = json_request.get('marketplace_id')
    sort_by = json_request.get('sort_by')
    sort_by_value = json_request.get('sort_by_value')
    if market_place_id != None and market_place_id != "" and market_place_id != "all" and market_place_id == "custom":
        pipeline = [
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "sku": {"$ifNull": ["$sku", ""]},
                "customer_name": {"$ifNull": ["$customer_name", ""]},
                "to_address": {"$ifNull": ["$to_address", ""]},
                "quantity": {"$ifNull": ["$quantity", 0]},
                "total_price": {"$ifNull": [{"$round": ["$total_price", 0.0]}, 0.0]},
                "taxes": {"$ifNull": ["$taxes", 0.0]},
                "purchase_order_date": {"$ifNull": ["$purchase_order_date", None]},
                "expected_delivery_date": {"$ifNull": ["$expected_delivery_date", None]},
            }
        },
        {
            "$skip": skip
        },
        {
            "$limit": limit + skip
        }
        ]
        if sort_by != None and sort_by != "":
            sort = {
                "$sort" : {
                    sort_by : int(sort_by_value)
                }
            }
            pipeline.append(sort)

        manual_orders = list(customOrder.objects.aggregate(*pipeline))
        count_pipeline = [
            {
                "$count": "total_count"
            }
        ]
        total_count_result = list(customOrder.objects.aggregate(*(count_pipeline)))
        total_count = total_count_result[0]['total_count'] if total_count_result else 0
        data['total_count'] = total_count
        data['manual_orders'] = manual_orders
        data['status'] = "custom"

    elif market_place_id != None and market_place_id != "" and market_place_id != "all" and market_place_id != "custom":
        match = {
            "$match": {
                "marketplace_id": ObjectId(market_place_id)
            }
        }
        pipeline.append(match)
        count_pipeline.append(match)
    if market_place_id != "custom":
        pipeline.extend([

            {
                "$lookup": {
                    "from": "marketplace",
                    "localField": "marketplace_id",
                    "foreignField": "_id",
                    "as": "marketplace_ins"
                }
            },
            {
                "$unwind": "$marketplace_ins"
            },
            {
                "$project": {
                    "_id": 0,
                    "id": {"$toString": "$_id"},
                    "purchase_order_id": "$purchase_order_id",
                    "order_date": {
                        "$dateToString": {
                            "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                            "date": "$order_date",
                        }
                        },
                    "order_status": "$order_status",
                    "order_total": "$order_total",
                    "currency": "$currency",
                    "marketplace_name": "$marketplace_ins.name",

                }
            },
            {
                "$skip": skip
            },
            {
                "$limit": limit + skip
            }
        ])
        if sort_by != None and sort_by != "":
            sort = {
                "$sort" : {
                    sort_by : int(sort_by_value)
                }
            }
            pipeline.append(sort)
        orders = list(Order.objects.aggregate(*(pipeline)))
        count_pipeline.extend([
            {
                "$count": "total_count"
            }
        ])
        total_count_result = list(Order.objects.aggregate(*(count_pipeline)))
        total_count = total_count_result[0]['total_count'] if total_count_result else 0
        
        data['orders'] = orders
        data['total_count'] = total_count
        data['status'] = ""

    pipeline = [
            {
                "$project" : {
                    "_id" : 0,
                    "id" : {"$toString" : "$_id"},
                    "name" : 1,
                    "image_url" : 1,
                }
            }
        ]
    data['marketplace_list'] = list(Marketplace.objects.aggregate(*(pipeline)))
    return data



def fetchOrderDetails(request):
    data = {}
    user_id = request.GET.get('user_id')
    order_id = request.GET.get('order_id')

    pipeline = [
        {
            "$match": {
                "_id": ObjectId(order_id)
            }
        },
        {
            "$lookup": {
                "from": "marketplace",
                "localField": "marketplace_id",
                "foreignField": "_id",
                "as": "marketplace_ins"
            }
        },
        {
            "$unwind": "$marketplace_ins"
        },
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "purchase_order_id": {"$ifNull": ["$purchase_order_id", ""]},
                "customer_order_id": {"$ifNull": ["$customer_order_id", ""]},
                "seller_order_id": {"$ifNull": ["$seller_order_id", ""]},
                "customer_email_id": {"$ifNull": ["$customer_email_id", ""]},
                "order_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": {"$ifNull": ["$order_date", None]},
                    }
                },
                "earliest_ship_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": {"$ifNull": ["$earliest_ship_date", None]},
                    }
                },
                "latest_ship_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": {"$ifNull": ["$latest_ship_date", None]},
                    }
                },
                "last_update_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": {"$ifNull": ["$last_update_date", None]},
                    }
                },
                "shipping_information": {"$ifNull": ["$shipping_information", {}]},
                "ship_service_level": {"$ifNull": ["$ship_service_level", ""]},
                "shipment_service_level_category": {"$ifNull": ["$shipment_service_level_category", ""]},
                "automated_shipping_settings": {"$ifNull": ["$automated_shipping_settings", {}]},
                "order_status": {"$ifNull": ["$order_status", ""]},
                "number_of_items_shipped": {"$ifNull": ["$number_of_items_shipped", 0]},
                "number_of_items_unshipped": {"$ifNull": ["$number_of_items_unshipped", 0]},
                "fulfillment_channel": {"$ifNull": ["$fulfillment_channel", ""]},
                "sales_channel": {"$ifNull": ["$sales_channel", ""]},
                "order_type": {"$ifNull": ["$order_type", ""]},
                "is_premium_order": {"$ifNull": ["$is_premium_order", False]},
                "is_prime": {"$ifNull": ["$is_prime", False]},
                "has_regulated_items": {"$ifNull": ["$has_regulated_items", False]},
                "is_replacement_order": {"$ifNull": ["$is_replacement_order", False]},
                "is_sold_by_ab": {"$ifNull": ["$is_sold_by_ab", False]},
                "is_ispu": {"$ifNull": ["$is_ispu", False]},
                "is_access_point_order": {"$ifNull": ["$is_access_point_order", False]},
                "is_business_order": {"$ifNull": ["$is_business_order", False]},
                "marketplace_name": {"$ifNull": ["$marketplace_ins.name", ""]},
                "payment_method": {"$ifNull": ["$payment_method", ""]},
                "payment_method_details": {"$ifNull": ["$payment_method_details", []]},
                "order_total": {"$ifNull": [{"$round": ["$order_total", 2]}, 0]},
                "currency": {"$ifNull": ["$currency", ""]},
                "is_global_express_enabled": {"$ifNull": ["$is_global_express_enabled", False]},
                "order_items": {"$ifNull": ["$order_items", []]}
            }
        }
    ]

    order_details = list(Order.objects.aggregate(*pipeline))

    if order_details:
        
        data = order_details[0]
        # Fetch OrderItems details using the IDs
        order_items_ids = data.get("order_items", [])
        if order_items_ids:
            order_items = DatabaseModel.list_documents(OrderItems.objects,{"id__in" : order_items_ids})
            
            serialized_items = []
            for item in order_items:
                serialized_item = {
                    "id": str(item.id),
                    "OrderId": item.OrderId,
                    "Platform": item.Platform,
                    "ProductDetails": {
                        "product_id": str(item.ProductDetails.product_id.id) if item.ProductDetails and item.ProductDetails.product_id else None,
                        "Title": item.ProductDetails.Title if item.ProductDetails else None,
                        "SKU": item.ProductDetails.SKU if item.ProductDetails else None,
                        "Condition": item.ProductDetails.Condition if item.ProductDetails else None,
                        "QuantityOrdered": item.ProductDetails.QuantityOrdered if item.ProductDetails else 0,
                        "QuantityShipped": item.ProductDetails.QuantityShipped if item.ProductDetails else 0
                    },
                    "Pricing": item.Pricing.to_mongo() if hasattr(item.Pricing, "to_mongo") else (dict(item.Pricing) if item.Pricing else {}),
                    "Fulfillment": {
                        "FulfillmentOption": item.Fulfillment.FulfillmentOption if item.Fulfillment else None,
                        "ShipMethod": item.Fulfillment.ShipMethod if item.Fulfillment else None,
                        "Carrier": item.Fulfillment.Carrier if item.Fulfillment else None,
                        "TrackingNumber": item.Fulfillment.TrackingNumber if item.Fulfillment else None,
                        "TrackingURL": item.Fulfillment.TrackingURL if item.Fulfillment else None,
                        "ShipDateTime": item.Fulfillment.ShipDateTime.isoformat() if item.Fulfillment and item.Fulfillment.ShipDateTime else None
                    },
                    "OrderStatus": {
                        "Status": item.OrderStatus.Status if item.OrderStatus else None,
                        "StatusDate": item.OrderStatus.StatusDate.isoformat() if item.OrderStatus and item.OrderStatus.StatusDate else None
                    },
                    "TaxCollection": item.TaxCollection.to_mongo() if hasattr(item.TaxCollection, "to_mongo") else (dict(item.TaxCollection) if item.TaxCollection else {}),
                    "IsGift": item.IsGift
                }
                serialized_items.append(serialized_item)
            
            data["order_items"] = serialized_items
        else:
            data["order_items"] = []

    return data


#----------------------ORDER CREATION---------------------------------------------------------------------------


@csrf_exempt
def getProductListForOrdercreation(request):
    data = dict()
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id')
    skip = int(json_request.get('skip',0))
    limit = int(json_request.get('limit',50))
    search_query = json_request.get('search_query')
    pipeline = []
    count_pipeline = []
    match = {}
    if marketplace_id != None and marketplace_id != "":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if search_query != None and search_query != "":
        match['product_title'] = {"$regex": search_query, "$options": "i"}
    if match != {}:
        match_pipeline = {
            "$match" : match}
        pipeline.append(match_pipeline)
    pipeline.extend([
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "product_title" : 1,
                "sku" : 1,
                "price" : 1,
            }
        },
        {
            "$skip" : skip
        },
        {
            "$limit" : limit
        }
    ])
    product_list = list(Product.objects.aggregate(*(pipeline)))
    count_pipeline.extend([
        {
            "$count": "total_count"
        }
    ])
    total_count_result = list(Product.objects.aggregate(*(count_pipeline)))
    total_count = total_count_result[0]['total_count'] if total_count_result else 0

    data['total_count'] = total_count
    
    return product_list


@csrf_exempt
def createManualOrder(request):
    data = dict()
    json_request = JSONParser().parse(request)
    print("json_request",json_request)
    product_id = json_request.get('product_id')
    product_title = json_request.get('product_title')
    sku = json_request.get('sku')
    customer_name = json_request.get('customer_name')
    to_address = json_request.get('to_address')
    quantity = int(json_request.get('quantity', 1))
    unit_price = DatabaseModel.get_document(Product.objects,{"id": product_id},['price']).price
    taxes = float(json_request.get('taxes')) if json_request.get('taxes') != '' else 0.0
    phone_number = json_request.get('phone_number')
    # purchase_order_date = json_request.get('purchase_order_date')
    expected_delivery_date = datetime.strptime(json_request.get('expected_delivery_date'), '%Y-%m-%d') if json_request.get('expected_delivery_date') else None
    supplier_name = json_request.get('supplier_name')
    tags = json_request.get('tags') if json_request.get('tags') != '' else []
    notes = json_request.get('notes', '')

    total_price = (unit_price * quantity) + taxes

    manual_order = customOrder(
        product_id=ObjectId(product_id),
        product_title=product_title,
        sku=sku,
        customer_name=customer_name,
        to_address=to_address,
        quantity=quantity,
        unit_price=unit_price,
        total_price=total_price,
        taxes=taxes,
        phone_number=phone_number,
        # purchase_order_date=purchase_order_date,
        expected_delivery_date=expected_delivery_date,
        supplier_name=supplier_name,
        tags=tags,
        notes=notes
    )
    manual_order.save()
    data['message'] = "Manual order created successfully."
    data['order_id'] = str(manual_order.id)
    return data

@csrf_exempt
def listManualOrders(request):
    data = dict()
    json_request = JSONParser().parse(request)
    print("json_request",json_request)
    limit = int(json_request.get('limit', 100))  # Default limit = 100 if not provided
    skip = int(json_request.get('skip', 0))  # Default skip = 0 if not provided
    sort_by = json_request.get('sort_by')
    sort_by_value = json_request.get('sort_by_value')
    pipeline = [
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "sku": {"$ifNull": ["$sku", ""]},
                "customer_name": {"$ifNull": ["$customer_name", ""]},
                "to_address": {"$ifNull": ["$to_address", ""]},
                "quantity": {"$ifNull": ["$quantity", 0]},
                "total_price": {"$ifNull": [{"$round": ["$total_price", 0.0]}, 0.0]},
                "taxes": {"$ifNull": ["$taxes", 0.0]},
                "purchase_order_date": {"$ifNull": ["$purchase_order_date", None]},
                "expected_delivery_date": {"$ifNull": ["$expected_delivery_date", None]},
            }
        },
        {
            "$skip": skip
        },
        {
            "$limit": limit + skip
        }
    ]
    if sort_by != None and sort_by != "":
        sort = {
            "$sort" : {
                sort_by : int(sort_by_value)
            }
        }
        pipeline.append(sort)

    manual_orders = list(customOrder.objects.aggregate(*pipeline))
    count_pipeline = [
        {
            "$count": "total_count"
        }
    ]
    total_count_result = list(customOrder.objects.aggregate(*(count_pipeline)))
    total_count = total_count_result[0]['total_count'] if total_count_result else 0
    data['total_count'] = total_count
    data['manual_orders'] = manual_orders
    return data


def updateManualOrder(request):
    data = dict()
    try:
        json_request = JSONParser().parse(request)
        order_id = json_request.get('order_id')
        manual_order = customOrder.objects.get(id=ObjectId(order_id))

        manual_order.product_title = json_request.get('product_title', manual_order.product_title)
        manual_order.sku = json_request.get('sku', manual_order.sku)
        manual_order.customer_name = json_request.get('customer_name', manual_order.customer_name)
        manual_order.to_address = json_request.get('to_address', manual_order.to_address)
        manual_order.quantity = int(json_request.get('quantity', manual_order.quantity))
        manual_order.unit_price = float(json_request.get('unit_price', manual_order.unit_price))
        manual_order.taxes = float(json_request.get('taxes', manual_order.taxes))
        manual_order.phone_number = json_request.get('phone_number', manual_order.phone_number)
        manual_order.purchase_order_date = json_request.get('purchase_order_date', manual_order.purchase_order_date)
        manual_order.expected_delivery_date = json_request.get('expected_delivery_date', manual_order.expected_delivery_date)
        manual_order.supplier_name = json_request.get('supplier_name', manual_order.supplier_name)
        manual_order.mark_order_as_shipped = json_request.get('mark_order_as_shipped', manual_order.mark_order_as_shipped)
        manual_order.mark_order_as_paid = json_request.get('mark_order_as_paid', manual_order.mark_order_as_paid)
        manual_order.tags = json_request.get('tags', manual_order.tags)
        manual_order.notes = json_request.get('notes', manual_order.notes)

        manual_order.total_price = (manual_order.unit_price * manual_order.quantity) + manual_order.taxes
        manual_order.save()

        data['message'] = "Manual order updated successfully."
    except customOrder.DoesNotExist:
        data['error'] = "Manual order not found."
    except Exception as e:
        data['error'] = str(e)
    return data


def fetchManualOrderDetails(request):
    data = dict()
    order_id = request.GET.get('order_id')
    pipeline = [
        {
        "$match": {
            "_id": ObjectId(order_id)
        }
        },
        {
        "$lookup": {
            "from": "product",
            "localField": "product_id",
            "foreignField": "_id",
            "as": "product_details"
        }
        },
        {
        "$unwind": {
            "path": "$product_details",
            "preserveNullAndEmptyArrays": True
        }
        },
        {
        "$project": {
            "_id": 0,
            "id": {"$toString": "$_id"},
            "product_title": 1,
            "sku": 1,
            "customer_name": 1,
            "to_address": 1,
            "quantity": 1,
            "unit_price": 1,
            "total_price": 1,
            "taxes": 1,
            "phone_number": 1,
            "purchase_order_date": 1,
            "expected_delivery_date": 1,
            "supplier_name": 1,
            "mark_order_as_shipped": 1,
            "mark_order_as_paid": 1,
            "tags": 1,
            "notes": 1,
            "product_image": {"$ifNull": ["$product_details.image_url", ""]}
        }
        }
    ]
    manual_order_details = list(customOrder.objects.aggregate(*pipeline))
    if manual_order_details:
        data['order_details'] = manual_order_details[0]
    else:
        data['error'] = "Manual order not found."
    return data


#-------------------------------------DASH BOARD APIS-------------------------------------------------------------------------------------------------

def ordersCountForDashboard(request):
    data = dict()
    marketplace_id = request.GET.get('marketplace_id')
    pipeline = []
    if marketplace_id != None and marketplace_id != "":
        match = {
            "$match" : {
                "marketplace_id" : ObjectId(marketplace_id)
            }
        }
        pipeline.append(match)
    pipeline.extend([
        {
            "$group": {
                "_id": None,
                "count": {"$sum": 1}
            }
        }
    ])
    order_status_count = list(Order.objects.aggregate(*(pipeline)))
    data['total_order_count'] = order_status_count
    return data


def totalSalesAmount(request):
    data = dict()
    marketplace_id = request.GET.get('marketplace_id')
    pipeline = []
    if marketplace_id != None and marketplace_id != "":
        match = {
            "$match" : {
                "marketplace_id" : ObjectId(marketplace_id)
            }
        }
        pipeline.append(match)
    pipeline.extend([
        {
            "$group": {
                "_id": None,
                "total_sales": {"$sum": "$order_total"}
            }
        }
    ])
    total_sales = list(Order.objects.aggregate(*(pipeline)))
    data['total_sales'] = total_sales[0]['total_sales'] if total_sales else 0
    return data


@csrf_exempt
def salesAnalytics(request):
    data = dict()
    try:
        json_request = JSONParser().parse(request)
        date_range = json_request.get('date_range', '7days')  # Default to 7 days
        start_date = json_request.get('start_date')  # Optional custom start date
        end_date = json_request.get('end_date')  # Optional custom end date

        # Determine the date range
        if date_range == '1day':
            start_date = datetime.now() - timedelta(days=1)
        elif date_range == '7days':
            start_date = datetime.now() - timedelta(days=7)
        elif date_range == '1month':
            start_date = datetime.now() - timedelta(days=30)
        elif start_date and end_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            start_date = datetime.now() - timedelta(days=7)

        end_date = end_date or datetime.now()

        # Sales count and values per day
        pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": {"$year": "$order_date"},
                        "month": {"$month": "$order_date"},
                        "day": {"$dayOfMonth": "$order_date"}
                    },
                    "sales_count": {"$sum": 1},
                    "sales_value": {"$sum": "$order_total"}
                }
            },
            {
                "$sort": {"_id": 1}
            }
        ]
        sales_data = list(Order.objects.aggregate(*pipeline))

        # Sales data by category
        category_pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            },
            {
                "$lookup": {
                    "from": "product",
                    "localField": "order_details.product_id",
                    "foreignField": "_id",
                    "as": "product_details"
                }
            },
            {
                "$unwind": "$product_details"
            },
            {
                "$group": {
                    "_id": "$product_details.category",
                    "sales_count": {"$sum": 1},
                    "sales_value": {"$sum": "$order_total"}
                }
            },
            {
                "$sort": {"sales_value": -1}
            }
        ]
        category_data = list(Order.objects.aggregate(*category_pipeline))

        # Sales data by channels
        channel_pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            },
            {
                "$lookup": {
                    "from": "marketplace",
                    "localField": "marketplace_id",
                    "foreignField": "_id",
                    "as": "marketplace_details"
                }
            },
            {
                "$unwind": "$marketplace_details"
            },
            {
                "$group": {
                    "_id": "$marketplace_details.name",
                    "sales_count": {"$sum": 1},
                    "sales_value": {"$sum": "$order_total"}
                }
            },
            {
                "$sort": {"sales_value": -1}
            }
        ]
        channel_data = list(Order.objects.aggregate(*channel_pipeline))

        data['sales_data'] = sales_data
        data['category_data'] = category_data
        data['channel_data'] = channel_data
    except Exception as e:
        data['error'] = str(e)
    return data

@csrf_exempt
def mostSellingProducts(request):
    data = dict()
    try:
        # Pipeline to get top 5 most selling products based on channels
        pipeline = [
            {
                "$lookup": {
                    "from": "order",
                    "localField": "_id",
                    "foreignField": "order_details.product_id",
                    "as": "orders"
                }
            },
            {
                "$unwind": "$orders"
            },
            {
                "$lookup": {
                    "from": "marketplace",
                    "localField": "orders.marketplace_id",
                    "foreignField": "_id",
                    "as": "marketplace_details"
                }
            },
            {
                "$unwind": "$marketplace_details"
            },
            {
                "$group": {
                    "_id": {
                        "product_id": "$_id",
                        "product_title": "$product_title",
                        "sku": "$sku",
                        "marketplace_name": "$marketplace_details.name"
                    },
                    "total_quantity_sold": {"$sum": "$orders.order_details.quantity"}
                }
            },
            {
                "$sort": {"total_quantity_sold": -1}
            },
            {
                "$limit": 5
            },
            {
                "$project": {
                    "_id": 0,
                    "product_id": {"$toString": "$_id.product_id"},
                    "product_title": "$_id.product_title",
                    "sku": "$_id.sku",
                    "marketplace_name": "$_id.marketplace_name",
                    "total_quantity_sold": 1
                }
            }
        ]

        top_products = list(Product.objects.aggregate(*pipeline))
        data['top_products'] = top_products
    except Exception as e:
        data['error'] = str(e)
    return data


@csrf_exempt
def getSalesTrendPercentage(request):
    data = dict()
    json_request = JSONParser().parse(request)
    trend_type = json_request.get('trend_type', 'channel')  # 'channel' or 'category'

    # Get the current month and previous month date ranges
    now = datetime.now()
    current_month_start = datetime(now.year, now.month, 1)
    previous_month_end = current_month_start - timedelta(days=1)
    previous_month_start = datetime(previous_month_end.year, previous_month_end.month, 1)

    # Match pipeline for current and previous month
    match_pipeline = [
        {
            "$facet": {
                "current_month": [
                    {
                        "$match": {
                            "order_date": {
                                "$gte": current_month_start,
                                "$lt": now
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": f"${'marketplace_id' if trend_type == 'channel' else 'category'}",
                            "sales_value": {"$sum": "$order_total"}
                        }
                    }
                ],
                "previous_month": [
                    {
                        "$match": {
                            "order_date": {
                                "$gte": previous_month_start,
                                "$lt": current_month_start
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": f"${'marketplace_id' if trend_type == 'channel' else 'category'}",
                            "sales_value": {"$sum": "$order_total"}
                        }
                    }
                ]
            }
        }
    ]

    trend_data = list(Order.objects.aggregate(*match_pipeline))

    if trend_data:
        current_month_data = {item["_id"]: item["sales_value"] for item in trend_data[0]["current_month"]}
        previous_month_data = {item["_id"]: item["sales_value"] for item in trend_data[0]["previous_month"]}

        trend_percentage = []
        for key in set(current_month_data.keys()).union(previous_month_data.keys()):
            current_value = current_month_data.get(key, 0)
            previous_value = previous_month_data.get(key, 0)
            percentage_change = ((current_value - previous_value) / previous_value * 100) if previous_value != 0 else (100 if current_value > 0 else 0)
            trend_percentage.append({
                "id": str(key),
                "current_month_sales": current_value,
                "previous_month_sales": previous_value,
                "trend_percentage": round(percentage_change, 2)
            })

        data['trend_percentage'] = trend_percentage
    else:
        data['trend_percentage'] = []

    return data

@csrf_exempt
def fetchSalesSummary(request):
    data = {}
    total_sales_pipeline = []
    pipeline = []
    match = {}

    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id')
    if marketplace_id:
        match = {
            "$match": {
                "marketplace_id": ObjectId(marketplace_id)
            }
        }
        total_sales_pipeline.append(match)
        pipeline.append(match)
        

    # Pipeline to calculate total units sold, total sold product count, and total sales
    total_sales_pipeline.extend([
        {
            "$group": {
                "_id": None,  # Grouping by None to get a single summary document
                "total_sales": {"$sum": "$order_total"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "total_sales": {"$round" : ["$total_sales",2]}
            }
        }
    ])

    summary = list(Order.objects.aggregate(*total_sales_pipeline))
    if summary:
        data['total_sales'] = summary[0].get('total_sales', 0)
    else:
        data['total_sales'] = 0

    pipeline.extend([
        {
            "$unwind": "$order_items"  # Unwind the order_items list to process each item individually
        },
        {
            "$group": {
                "_id": None,  # Grouping by None to get a single summary document
                "ids": {"$addToSet": "$order_items"}  # Collect unique order_items
            }
        },
        {
            "$project": {
                "_id": 0,
                "ids": 1
            }
        },
    ])

    summary1 = list(Order.objects.aggregate(*pipeline))
    if summary1:
        s_pipeline = [
            {"$match" : {
                "_id" : {"$in" : summary1[0]['ids']}
            }},
             {
            "$group": {
                "_id": None,
                "total_units_sold": {"$sum": "$ProductDetails.QuantityOrdered"},
                "unique_product_ids": {"$addToSet": "$ProductDetails.product_id"},
            }
            },
            {
                "$project": {
                    "_id": 0,
                    "total_units_sold": 1,
                    "total_sold_product_count": {"$size": "$unique_product_ids"},
                }
            }
        ]
        summary2 = list(OrderItems.objects.aggregate(*s_pipeline))
        if summary2:
            data['total_units_sold'] = summary2[0].get('total_units_sold', 0)
            data['total_sold_product_count'] = summary2[0].get('total_sold_product_count', 0)
    else:
        data['total_units_sold'] = 0
        data['total_sold_product_count'] = 0
    return data
