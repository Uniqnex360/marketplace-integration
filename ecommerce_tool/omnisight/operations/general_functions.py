import requests
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from omnisight.operations.walmart_utils import getAccesstoken
from omnisight.models import *
import pandas as pd
from ecommerce_tool.crud import DatabaseModel
from bson import ObjectId
import json



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



def getProductList(request):
    data = dict()
    marketplace_id = request.GET.get('marketplace_id')
    skip = int(request.GET.get('skip'))
    limit = int(request.GET.get('limit'))
    marketplace = request.GET.get('marketplace')
    category_name = request.GET.get('category_name')
    pipeline = []
    count_pipeline = []
    if marketplace != None and marketplace != "" and marketplace == "all":
        pass
    else:
        match = {
            "$match" : {
                "marketplace_id" : ObjectId(marketplace_id)
            }
        },
        pipeline.append(match)
        count_pipeline.append(match)
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
            "$limit" : limit
        }
    ])
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
        match,
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "name" : 1
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
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "name" : 1
            }
        }
    ])
    brand_list = list(Brand.objects.aggregate(*(pipeline)))
    data['brand_list'] = brand_list
    return data