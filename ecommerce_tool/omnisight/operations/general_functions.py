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


@csrf_exempt
def getProductList(request):
    data = dict()
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id')
    skip = int(json_request.get('skip'))
    limit = int(json_request.get('limit'))
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
            "$limit" : 1500#limit
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



@csrf_exempt
def fetchAllorders(request):
    data = dict()
    orders = []
    pipeline = []
    count_pipeline = []

    json_request = JSONParser().parse(request)
    user_id = json_request.get('user_id')
    limit = int(json_request.get('limit', 100))  # Default limit = 100 if not provided
    skip = int(json_request.get('skip', 0))  # Default skip = 0 if not provided
    market_place_id = json_request.get('marketplace_id')
    sort_by = json_request.get('sort_by')
    sort_by_value = json_request.get('sort_by_value')

    if market_place_id != None and market_place_id != "" and market_place_id != "all":
        match = {
            "$match": {
                "marketplace_id": ObjectId(market_place_id)
            }
        }
        pipeline.append(match)
        count_pipeline.append(match)
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
            "$limit": limit
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
    data['orders'] = orders
    data['total_count'] = total_count
    return data


def fetchOrderDetails(request):
    data = dict()
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
                "purchase_order_id": "$purchase_order_id",
                "customer_order_id": "$customer_order_id",
                "seller_order_id": "$seller_order_id",
                "customer_email_id": "$customer_email_id",
                "order_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": "$order_date",
                    }
                },
                "earliest_ship_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": "$earliest_ship_date",
                    }
                },
                "latest_ship_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": "$latest_ship_date",
                    }
                },
                "last_update_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                        "date": "$last_update_date",
                    }
                },
                "shipping_information": "$shipping_information",
                "ship_service_level": "$ship_service_level",
                "shipment_service_level_category": "$shipment_service_level_category",
                "automated_shipping_settings": "$automated_shipping_settings",
                "order_details": "$order_details",
                "order_status": "$order_status",
                "number_of_items_shipped": "$number_of_items_shipped",
                "number_of_items_unshipped": "$number_of_items_unshipped",
                "fulfillment_channel": "$fulfillment_channel",
                "sales_channel": "$sales_channel",
                "order_type": "$order_type",
                "is_premium_order": "$is_premium_order",
                "is_prime": "$is_prime",
                "has_regulated_items": "$has_regulated_items",
                "is_replacement_order": "$is_replacement_order",
                "is_sold_by_ab": "$is_sold_by_ab",
                "is_ispu": "$is_ispu",
                "is_access_point_order": "$is_access_point_order",
                "is_business_order": "$is_business_order",
                "marketplace_name": "$marketplace_ins.name",
                "payment_method": "$payment_method",
                "payment_method_details": "$payment_method_details",
                "order_total": "$order_total",
                "currency": "$currency",
                "is_global_express_enabled": "$is_global_express_enabled",
            }
        }
    ]
    order_details = list(Order.objects.aggregate(*(pipeline)))
    if len(order_details):
        data = order_details[0]
    return data


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

