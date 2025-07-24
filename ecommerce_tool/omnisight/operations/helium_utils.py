from omnisight.models import Product, Order,pageview_session_count,OrderItems,Marketplace
from bson import ObjectId
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from ecommerce_tool.crud import DatabaseModel
import threading
import math
import pytz
from concurrent.futures import ThreadPoolExecutor
import logging
logger = logging.getLogger(__name__)
import pandas as pd
from pytz import timezone

def convertdateTotimezone(start_date,end_date,timezone_str):
    import pytz
    local_tz = pytz.timezone(timezone_str)
        
    naive_from_date = datetime.strptime(start_date, '%Y-%m-%d')
    naive_to_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    naive_to_date = naive_to_date.replace(hour=23, minute=59, second=59)
    
    start_date = local_tz.localize(naive_from_date)
    end_date = local_tz.localize(naive_to_date)
    return start_date, end_date

def convertLocalTimeToUTC(start_date, end_date, timezone_str):
    import pytz
    local_tz = pytz.timezone(timezone_str)
        
    # If dates are naive (no timezone), localize them
    if start_date.tzinfo is None:
        start_date = local_tz.localize(start_date)
    if end_date.tzinfo is None:
        end_date = local_tz.localize(end_date)
    
    # Convert to UTC
    start_date = start_date.astimezone(pytz.UTC)
    end_date = end_date.astimezone(pytz.UTC)
    return start_date, end_date

def getOrdersListBasedonProductId(productIds,start_date=None, end_date=None):
    """
    Fetches the list of orders based on the provided product ID using a pipeline aggregation.

    Args:
        productId (str): The ID of the product for which to fetch orders.

    Returns:
        list: A list of dictionaries containing order details.
    """
    pipeline = []
    if start_date and end_date:
        pipeline.append({
            "$match": {
                "order_date": {"$gte": start_date, "$lte": end_date},
                "order_status": {"$in": ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']}
            }
        })
    pipeline.extend([
        {
            "$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",
                "as": "order_items"
            }
        },
        {"$unwind": "$order_items"},
        {
            "$match": {
                "order_items.ProductDetails.product_id": {"$in": productIds}
            }
        },
        {
            "$group" : {
                "_id" : None,
                "orderIds" : { "$addToSet": "$_id" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "orderIds": 1
            }
        }
    ])
    orders = list(Order.objects.aggregate(*pipeline))
    if orders != []:
        orders = orders[0]['orderIds']
    return orders



def getproductIdListBasedonbrand(brandIds,start_date=None, end_date=None):
    """
    Fetches the list of product IDs based on the provided brand ID using a pipeline aggregation.

    Args:
        productId (str): The ID of the brand for which to fetch product IDs.

    Returns:
        list: A list of dictionaries containing product details.
    """
    orders = []
    pipeline = [
        {
            "$match": {
                "brand_id": {"$in": [ObjectId(bid) for bid in brandIds]}
            }
        },
        {
            "$group" : {
                "_id" : None,
                "productIds" : { "$addToSet": "$_id" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "productIds": 1
            }
        }
    ]
    products = list(Product.objects.aggregate(*pipeline))
    if products != []:
        orders = getOrdersListBasedonProductId(products[0]['productIds'],start_date, end_date)
    return orders


def getproductIdListBasedonManufacture(manufactureName = [],start_date=None, end_date=None):
    """
    Fetches the list of product IDs based on the provided brand ID using a pipeline aggregation.

    Args:
        productId (str): The ID of the brand for which to fetch product IDs.

    Returns:
        list: A list of dictionaries containing product details.
    """
    orders = []
    pipeline = [
        {
            "$match": {
                "manufacturer_name": {"$in":manufactureName }
            }
        },
        {
            "$group" : {
                "_id" : None,
                "productIds" : { "$addToSet": "$_id" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "productIds": 1
            }
        }
    ]
    products = list(Product.objects.aggregate(*pipeline))
    if products != []:
        orders = getOrdersListBasedonProductId(products[0]['productIds'],start_date, end_date)
    return orders

def get_date_range(preset, time_zone_str="UTC"):

    # Get the timezone object
    tz = timezone(time_zone_str)

    # Get today's date in the specified timezone
    now = datetime.now(tz)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if preset == "Today":
        start = today
        return start, start.replace(hour=23, minute=59, second=59)
    elif preset == "Yesterday":
        start = today - timedelta(days=1)
        return start, start.replace(hour=23, minute=59, second=59)
    elif preset == "This Week":
        start = today - timedelta(days=today.weekday())
        return start, today.replace(hour=23, minute=59, second=59)
    elif preset == "This Month":
        start = today.replace(day=1)
        return start, today.replace(hour=23, minute=59, second=59)
    elif preset == "This Year":
        return today.replace(month=1, day=1), today.replace(hour=23, minute=59, second=59)
    
    elif preset == "Last Week":
        start = today - timedelta(days=today.weekday() + 7)
        end = start + timedelta(days=6)
        return start.replace(hour=0, minute=0, second=0, microsecond=0), end.replace(hour=23, minute=59, second=59)
    elif preset == "Last 7 days":
        return today - timedelta(days=7), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 14 days":
        return today - timedelta(days=14), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 30 days":
        return today - timedelta(days=30), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 60 days":
        return today - timedelta(days=60), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last 90 days":
        return today - timedelta(days=90), (today - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    
    elif preset == "Last Month":
        start = (today.replace(day=1) - relativedelta(months=1))
        return start, (today.replace(day=1) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "This Quarter":
        quarter = (today.month - 1) // 3
        start = today.replace(month=quarter * 3 + 1, day=1)
        return start, (start + relativedelta(months=3) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    elif preset == "Last Quarter":
        quarter = ((today.month - 1) // 3) - 1
        start = today.replace(month=quarter * 3 + 1, day=1)
        return start, (start + relativedelta(months=3) - timedelta(days=1)).replace(hour=23, minute=59, second=59)
    
    elif preset == "Last Year":
        return today.replace(year=today.year - 1, month=1, day=1), today.replace(year=today.year - 1, month=12, day=31, hour=23, minute=59, second=59)
    return today, (today + timedelta(days=1)).replace(hour=23, minute=59, second=59)



def grossRevenue(start_date, end_date, marketplace_id=None, brand_id=None, 
                product_id=None, manufacuture_name=[], fulfillment_channel=None, 
                timezone='UTC'):    
    # Convert local timezone dates to UTC
    if timezone != 'UTC':
        start_date,end_date = convertLocalTimeToUTC(start_date, end_date, timezone)
    
    # Remove timezone info for MongoDB query (assuming your MongoDB driver expects naive UTC)
    start_date = start_date.replace(tzinfo=None)
    end_date = end_date.replace(tzinfo=None)
    
    pipeline = [
        {
            "$project": {
                "_id": 1,
                "name": 1,
                "image_url": 1,
            }
        }
    ]
    marketplace_list = list(Marketplace.objects.aggregate(*(pipeline)))
    
    match = dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = {"$ne": "Cancelled"}
    match['order_total'] = {"$gt": 0}
    
    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel
    if marketplace_id not in [None, "", "all", "custom"]:
        match['marketplace_id'] = ObjectId(marketplace_id)
    
    if manufacuture_name not in [None, "", []]:
        ids = getproductIdListBasedonManufacture(manufacuture_name, start_date, end_date)
        match["_id"] = {"$in": ids}
    elif product_id not in [None, "", []]:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id, start_date, end_date)
        match["_id"] = {"$in": ids}
    elif brand_id not in [None, "", []]:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id, start_date, end_date)
        match["_id"] = {"$in": ids}
    
    pipeline = [
        {
            "$match": match
        },
        {
            "$project": {
                "_id": 1,
                "order_date": 1,
                "order_items": 1,
                "order_total": 1,
                "marketplace_id": 1,
                "currency": 1,
                "shipping_address": 1,
                "shipping_information": 1,
                "shipping_price": {"$ifNull": ["$ShippingPrice", 0.0]},
                "items_order_quantity": {"$ifNull": ["$items_order_quantity", 0]},
            }
        }
    ]
    
    order_list = list(Order.objects.aggregate(*pipeline))
    
    for order_ins in order_list:
        for marketplace in marketplace_list:
            order_ins['marketplace_name'] = marketplace['name']
    
    return order_list


def get_previous_periods(current_start, current_end):
    # Calculate the duration of the current period
    period_duration = current_end - current_start
    if period_duration.days > 1:
        period_duration += timedelta(days=1)
    
    # Calculate previous periods
    previous_period = {
        'start': (current_start - period_duration).strftime('%b %d, %Y'),
        'end': (current_start - timedelta(days=1)).strftime('%b %d, %Y')
    }
    
    previous_week = {
        'start': (current_start - timedelta(weeks=1)).strftime('%b %d, %Y'),
        'end': (current_end - timedelta(weeks=1)).strftime('%b %d, %Y')
    }
    
    previous_month = {
        'start': (current_start - relativedelta(months=1)).strftime('%b %d, %Y'),
        'end': (current_end - relativedelta(months=1)).strftime('%b %d, %Y')
    }
    
    previous_year = {
        'start': (current_start - relativedelta(years=1)).strftime('%b %d, %Y'),
        'end': (current_end - relativedelta(years=1)).strftime('%b %d, %Y')
    }
    
    response_data = {
        'previous_period': previous_period,
        'previous_week': previous_week,
        'previous_month': previous_month,
        'previous_year': previous_year,
        'current_period': {
            'start': current_start.strftime('%b %d, %Y'),
            'end': current_end.strftime('%b %d, %Y')
        }
    }

    return response_data


def refundOrder(start_date, end_date, marketplace_id=None,brand_id=None,product_id=None,manufacuture_name=[],fulfillment_channel=None,timezone='UTC'):    
    # Convert local timezone dates to UTC
    if timezone != 'UTC':
        start_date,end_date = convertLocalTimeToUTC(start_date, end_date, timezone)
    
    # Remove timezone info for MongoDB query (assuming your MongoDB driver expects naive UTC)
    start_date = start_date.replace(tzinfo=None)
    end_date = end_date.replace(tzinfo=None)
    match=dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = "Refunded"
    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    if manufacuture_name != None and manufacuture_name != "" and manufacuture_name != []:
        ids = getproductIdListBasedonManufacture(manufacuture_name,start_date, end_date)
        match["_id"] = {"$in": ids}

    elif product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id,start_date, end_date)
        match["_id"] = {"$in": ids}

    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id,start_date, end_date)
        match["_id"] = {"$in": ids}
        
    pipeline = [
            {
            "$match": match
            },
            {
            "$project": {
                "_id": 0,
                "order_items": 1,
                "order_total" :1,
                "order_date" : 1
            }
            },
            {
                "$sort" : {
                    "_id" : -1
                }
            }
        ]

    result = list(Order.objects.aggregate(*pipeline))
    return result


def AnnualizedRevenueAPIView(target_date):
    # Calculate date range (last 12 months from today)
    start_date = target_date - timedelta(days=365)
    # Initialize variables
    monthly_revenues = []
    total_gross_revenue = 0
    
    # Calculate revenue for each of the last 12 months
    for i in range(12):
        month_start = start_date + timedelta(days=30*i)
        month_end = month_start + timedelta(days=30)
        
        # Get gross revenue for this month (using your existing grossRevenue function)
        monthly_result = grossRevenue(month_start, month_end)
        monthly_gross = sum(ins['order_total'] for ins in monthly_result) if monthly_result else 0
        monthly_revenues.append(monthly_gross)
        total_gross_revenue += monthly_gross
    
    # Calculate average monthly revenue
    average_monthly = total_gross_revenue / 12 if 12 > 0 else 0
    
    # Calculate annualized revenue (average * 12)
    annualized_revenue = average_monthly * 12
    
    # Return just the final value rounded to 2 decimal places
    annualized_revenue = round(annualized_revenue, 2)
    return annualized_revenue


def getdaywiseproductssold(start_date, end_date, product_id, is_hourly=False):
   
    date_format = "%Y-%m-%d %H:00" if is_hourly else "%Y-%m-%d"

    pipeline = [
        {
            "$match": {
                "order_date": {"$gte": start_date, "$lte": end_date},
                "order_status": {"$in": ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']}
            }
        },
        {
            "$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",
                "as": "order_items_ins"
            }
        },
        {"$unwind": "$order_items_ins"},
        {
            "$match": {
                "order_items_ins.ProductDetails.product_id": ObjectId(product_id)
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": date_format,
                        "date": "$order_date"
                    }
                },
                "total_quantity": {"$sum": "$order_items_ins.ProductDetails.QuantityOrdered"},
                "total_price": {"$sum": "$order_items_ins.Pricing.ItemPrice.Amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": "$_id",
                "total_quantity": 1,
                "total_price": {"$round": ["$total_price", 2]}
            }
        },
        {"$sort": {"date": 1}}
    ]

    orders = list(Order.objects.aggregate(*pipeline))
    return orders


def pageViewsandSessionCount(start_date,end_date,product_id):
    """
    Fetches the list of orders based on the provided product ID using a pipeline aggregation.

    Args:
        productId (str): The ID of the product for which to fetch orders.

    Returns:
        list: A list of dictionaries containing order details.
    """
    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "product_id": ObjectId(product_id)
            }
        },
        {
            "$project": {
                "_id": 0,
                "date" : 1,
                "page_views": 1,
                "session_count": "$sessions"
            }
        }
    ]
    views_list = list(pageview_session_count.objects.aggregate(*pipeline))
    return views_list


def create_empty_bucket_data(time_key):
    """Return a valid empty data structure for a time bucket"""
    return {
        "gross_revenue": 0.0,
        "net_profit": 0.0,
        "profit_margin": 0.0,
        "orders": 0,
        "units_sold": 0,
        "refund_amount": 0.0,
        "refund_quantity": 0,
        "current_date": time_key
    }
    
def get_graph_data(start_date, end_date, preset, marketplace_id, brand_id=None, product_id=None, 
                  manufacturer_name=None, fulfillment_channel=None, timezone="UTC"):
    
    # --- 1. SETUP: TIMEZONES AND BUCKETING ---
    user_timezone = pytz.timezone(timezone)
    
    # Determine the bucketing unit for the aggregation pipeline
    if preset in ["Today", "Yesterday"]:
        bucket_unit = "hour"
        time_format = "%Y-%m-%d %H:00:00"
    else:
        bucket_unit = "day"
        time_format = "%Y-%m-%d 00:00:00"
        
    # --- 2. BUILD THE MAIN AGGREGATION PIPELINE ---
    # This single pipeline will replace thousands of individual queries.
    
    # Stage 1: Initial filtering of orders
    match_stage = {
        "$match": {
            "order_date": {"$gte": start_date, "$lt": end_date},
            "order_status__in": ['Shipped', 'Delivered', 'Acknowledged', 'Pending', 'Unshipped', 'PartiallyShipped']
        }
    }
    # Add optional filters
    if fulfillment_channel:
        match_stage["$match"]['fulfillment_channel'] = fulfillment_channel
    if marketplace_id and marketplace_id not in ["all", "custom"]:
        match_stage["$match"]['marketplace_id'] = ObjectId(marketplace_id)
    
    # Note: These helper functions might be slow. Consider optimizing them if needed.
    if manufacturer_name:
        ids = getproductIdListBasedonManufacture(manufacturer_name, start_date, end_date)
        match_stage["$match"]["_id__in"] = ids
    elif product_id:
        product_ids_obj = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_ids_obj, start_date, end_date)
        match_stage["$match"]["_id__in"] = ids
    elif brand_id:
        brand_ids_obj = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_ids_obj, start_date, end_date)
        match_stage["$match"]["_id__in"] = ids

    pipeline = [
        match_stage,
        # Stage 2: Deconstruct the order_items array to process each item individually
        {"$unwind": "$order_items"},
        
        # Stage 3: Look up details for each order item from the OrderItems collection.
        # This replaces the query-per-item loop.
        {"$lookup": {
            "from": "order_items",  # The actual name of your OrderItems collection
            "localField": "order_items",
            "foreignField": "_id",
            "as": "item_details"
        }},
        {"$unwind": "$item_details"}, # Deconstruct the looked-up array

        # Stage 4: Look up product details (for COGS)
        {"$lookup": {
            "from": "product", # The actual name of your Product collection
            "localField": "item_details.ProductDetails.product_id",
            "foreignField": "_id",
            "as": "product_details"
        }},
        {"$unwind": {"path": "$product_details", "preserveNullAndEmptyArrays": True}},

        # Stage 5: Group documents into time buckets (hour or day)
        # $dateTrunc is powerful and handles timezones correctly. Requires MongoDB 5.0+.
        {"$group": {
            "_id": {
                "time_bucket": {
                    "$dateTrunc": {
                        "date": "$order_date",
                        "unit": bucket_unit,
                        "timezone": timezone
                    }
                }
            },
            "gross_revenue": {"$sum": {"$ifNull": ["$item_details.Pricing.ItemPrice.Amount", 0]}},
            "units_sold": {"$sum": {"$ifNull": ["$item_details.QuantityOrdered", 1]}},
            "total_orders": {"$addToSet": "$_id"}, # Collect unique order IDs
            # Calculate net profit per item before summing
            "net_profit": {"$sum": {
                "$add": [
                    {"$subtract": [
                        {"$ifNull": ["$item_details.Pricing.ItemPrice.Amount", 0]},
                        # Choose COGS based on marketplace
                        {"$ifNull": [
                            {"$cond": {
                                "if": {"$eq": ["$marketplace_id.name", "Amazon"]}, 
                                "then": "$product_details.total_cogs",
                                "else": "$product_details.w_total_cogs"
                            }},
                            0
                        ]}
                    ]},
                    {"$ifNull": ["$product_details.vendor_funding", 0]}
                ]
            }},
        }},
        
        # Stage 6: Final formatting
        {"$project": {
            "_id": 0,
            "time_key": {"$dateToString": {"format": time_format, "date": "$_id.time_bucket", "timezone": timezone}},
            "gross_revenue": {"$round": ["$gross_revenue", 2]},
            "net_profit": {"$round": ["$net_profit", 2]},
            "profit_margin": {
                "$round": [
                    {"$cond": {
                        "if": {"$gt": ["$gross_revenue", 0]},
                        "then": {"$multiply": [{"$divide": ["$net_profit", "$gross_revenue"]}, 100]},
                        "else": 0
                    }},
                    2
                ]
            },
            "orders": {"$size": "$total_orders"},
            "units_sold": 1,
        }},
        {"$sort": {"time_key": 1}}
    ]

    # --- 3. EXECUTE QUERIES CONCURRENTLY ---
    # We now only have TWO main database tasks: one for sales, one for refunds.
    
    def fetch_sales_data():
        # The `aggregate` method returns a CommandCursor, so we convert it to a list
        return list(Order.objects.aggregate(*pipeline))

    def fetch_refund_data():
        # This query is now run only ONCE for the entire date range
        refunds = refundOrder(start_date, end_date, marketplace_id, brand_id, product_id)
        # Process refunds into a dictionary for easy lookup
        refunds_by_bucket = {}
        if refunds:
            for refund in refunds:
                # Convert refund date to user's timezone and truncate to the bucket
                refund_dt_utc = refund['order_date'].replace(tzinfo=pytz.UTC)
                refund_dt_local = refund_dt_utc.astimezone(user_timezone)
                
                if bucket_unit == 'hour':
                    bucket_start = refund_dt_local.replace(minute=0, second=0, microsecond=0)
                else: # day
                    bucket_start = refund_dt_local.replace(hour=0, minute=0, second=0, microsecond=0)
                
                key = bucket_start.strftime(time_format)
                
                if key not in refunds_by_bucket:
                    refunds_by_bucket[key] = {"refund_amount": 0, "refund_quantity": 0}
                
                refunds_by_bucket[key]["refund_amount"] += refund.get('order_total', 0)
                refunds_by_bucket[key]["refund_quantity"] += len(refund.get('order_items', []))
        return refunds_by_bucket

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_sales = executor.submit(fetch_sales_data)
        future_refunds = executor.submit(fetch_refund_data)
        
        sales_results = future_sales.result()
        refunds_by_bucket = future_refunds.result()
        
    # --- 4. MERGE RESULTS AND FILL GAPS ---
    # Create a zero-filled template for all time buckets in the range
    graph_data = {}
    current_time = start_date
    while current_time < end_date:
        key = current_time.astimezone(user_timezone).strftime(time_format)
        graph_data[key] = {
            "current_date": key, "gross_revenue": 0, "net_profit": 0, "profit_margin": 0,
            "orders": 0, "units_sold": 0, "refund_amount": 0, "refund_quantity": 0
        }
        if bucket_unit == 'hour':
            current_time += timedelta(hours=1)
        else:
            current_time += timedelta(days=1)
            
    # Populate the template with data from the sales aggregation
    for result in sales_results:
        time_key = result['time_key']
        if time_key in graph_data:
            graph_data[time_key].update(result)

    # Populate the template with data from the refunds
    for time_key, refund_data in refunds_by_bucket.items():
        if time_key in graph_data:
            graph_data[time_key].update(refund_data)

    return graph_data

def totalRevenueCalculation(start_date, end_date, marketplace_id=None, brand_id=None, product_id=None, manufacturer_name=None, fulfillment_channel=None,timezone_str="UTC"):
    total = dict()
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    total_units = 0
    total_orders = 0
    temp_other_price = 0
    vendor_funding = 0
    refund_quantity_ins = 0

    result = grossRevenue(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone_str)
    refund_ins = refundOrder(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone_str)

    if refund_ins:
        for ins in refund_ins:
            refund += ins['order_total']
            refund_quantity_ins += len(ins['order_items'])

    total_orders = len(result)

    def process_order(order):
        nonlocal gross_revenue, total_cogs, total_units, temp_other_price, vendor_funding

        tax_price = 0
        gross_revenue += order['order_total']
        total_units += order['items_order_quantity']

        for j in order['order_items']:
            pipeline = [
                {
                    "$match": {
                        "_id": j
                    }
                },
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }
                },
                {
                    "$unwind": {
                        "path": "$product_ins",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
                        "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
                        "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
                        "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                    }
                }
            ]
            result = list(OrderItems.objects.aggregate(*pipeline))
            if result:
                tax_price += result[0]['tax_price']
                temp_other_price += result[0]['price']

                if order['marketplace_name'] == "Amazon":
                    total_cogs += result[0]['total_cogs']
                else:
                    total_cogs += result[0]['w_total_cogs']
                
                vendor_funding += result[0]['vendor_funding']

    # Create threads for processing orders
    threads = []
    for order in result:
        thread = threading.Thread(target=process_order, args=(order,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Calculate net profit
    net_profit = (temp_other_price - total_cogs) + vendor_funding

    # Total values
    total = {
        "gross_revenue": round(gross_revenue, 2),
        "net_profit": round(net_profit, 2),
        "profit_margin": round((net_profit / gross_revenue) * 100, 2) if gross_revenue else 0,
        "orders": round(total_orders, 2),
        "units_sold": round(total_units, 2),
        "refund_amount": round(refund, 2),
        "refund_quantity": refund_quantity_ins
    }
    return total


def calculate_metricss(
    from_date,
    to_date,
    marketplace_id,
    brand_id,
    product_id,
    manufacturer_name,
    fulfillment_channel,
    timezone='UTC',
    include_extra_fields=False,
    use_threads=False
):
    gross_revenue = 0
    total_cogs = 0
    net_profit = 0
    total_units = 0
    vendor_funding = 0
    temp_price = 0
    refund = 0
    tax_price = 0
    sessions = 0
    page_views = 0
    shipping_cost = 0
    unitSessionPercentage = 0
    sku_set = set()
    p_id = set()

    result = grossRevenue(from_date, to_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone)
    all_item_ids = [ObjectId(item_id) for order in result for item_id in order['order_items']]
    refund_ins = refundOrder(from_date, to_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone)
    refund = len(refund_ins)
    
    
    if timezone != 'UTC':
        from_date, to_date = convertLocalTimeToUTC(from_date, to_date, timezone)
    
    # Remove timezone info for MongoDB query (assuming your MongoDB driver expects naive UTC)
    from_date = from_date.replace(tzinfo=None)
    to_date = to_date.replace(tzinfo=None)

    item_pipeline = [
        { "$match": { "_id": { "$in": all_item_ids } } },
        {
            "$lookup": {
                "from": "product",
                "localField": "ProductDetails.product_id",
                "foreignField": "_id",
                "as": "product_ins"
            }
        },
        { "$unwind": { "path": "$product_ins", "preserveNullAndEmptyArrays": True } },
        {
            "$project": {
                "p_id" : "$product_ins._id",
                "price": "$Pricing.ItemPrice.Amount",
                "tax_price": "$Pricing.ItemTax.Amount",
                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                "sku": "$product_ins.sku",
                "total_cogs": { "$ifNull": ["$product_ins.total_cogs", 0] },
                "w_total_cogs": { "$ifNull": ["$product_ins.w_total_cogs", 0] },
                "vendor_funding": { "$ifNull": ["$product_ins.vendor_funding", 0] },
                "a_shipping_cost" : {"$ifNull":["$product_ins.a_shipping_cost",0]},
                "w_shiping_cost" : {"$ifNull":["$product_ins.w_shiping_cost",0]},
            }
        }
    ]

    item_details_map = {str(item['_id']): item for item in OrderItems.objects.aggregate(*item_pipeline)}

    def process_order(order):
        nonlocal gross_revenue, temp_price, tax_price, total_cogs, vendor_funding, total_units, sku_set, page_views, sessions, shipping_cost,p_id

        gross_revenue += order['order_total']
        total_units += order['items_order_quantity']
        for item_id in order['order_items']:
            item_data = item_details_map.get(str(item_id))
            if item_data:
                temp_price += item_data['price']
                tax_price += item_data.get('tax_price', 0)

                if order.get('marketplace_name') == "Amazon":
                    total_cogs += item_data.get('total_cogs', 0)
                    shipping_cost += item_data.get('a_shipping_cost', 0)
                else:
                    total_cogs += item_data.get('w_total_cogs', 0)
                    shipping_cost += item_data.get('w_shiping_cost', 0)

                vendor_funding += item_data.get('vendor_funding', 0)
                

                if item_data.get('sku'):
                    sku_set.add(item_data['sku'])

                try:
                    p_id.add(item_data['p_id'])
                except:
                    pass

    # Modified threading approach
    if use_threads:
        # Use a ThreadPool with limited workers
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(process_order, order) for order in result]
            for future in as_completed(futures):
                future.result()  # This will raise exceptions if any occurred
    else:
        # Process sequentially
        for order in result:
            process_order(order)
    pipeline = [
        {
            "$match": {
                "product_id": {"$in": list(p_id)},
                "date": {"$gte": from_date, "$lte": to_date}
            }
        },
        {
            "$group": {
                "_id": None,
                "page_views": {"$sum": "$page_views"},
                "sessions": {"$sum": "$sessions"}
            }
        }
    ]
    p_result = list(pageview_session_count.objects.aggregate(*pipeline))
    for P_ins in p_result:
        page_views += P_ins.get('page_views', 0)
        sessions += P_ins.get('sessions', 0)

    net_profit = (temp_price - total_cogs) + vendor_funding
    margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
    unitSessionPercentage = (total_units / sessions) * 100 if sessions else 0

    base_result = {
        "grossRevenue": round(gross_revenue, 2),
        "expenses": round(total_cogs, 2),
        "netProfit": round(net_profit, 2),
        "roi": round((net_profit / total_cogs) * 100, 2) if total_cogs > 0 else 0,
        "unitsSold": total_units,
        "refunds": refund,
        "skuCount": len(sku_set),
        "sessions": sessions,
        "pageViews": page_views,
        "unitSessionPercentage": round(unitSessionPercentage, 2),
        "margin": round(margin, 2),
        "orders": len(result)
    }

    if include_extra_fields:
        base_result.update({
            "seller": "",
            "tax_price": round(tax_price, 2),
            "total_cogs": round(total_cogs, 2),
            "product_cost": round(temp_price, 2),
            "shipping_cost": round(shipping_cost,2),
        })

    return base_result



def totalRevenueCalculationForProduct(start_date, end_date, marketplace_id=None, brand_id=None, product_id=None, manufacturer_name=None, fulfillment_channel=None,timezone_str="UTC"):
    total = dict()
    gross_revenue = 0
    total_cogs = 0
    net_profit = 0
    total_units = 0
    temp_other_price = 0
    vendor_funding = 0

    result = grossRevenue(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone_str)


    def process_order(order):
        nonlocal gross_revenue, total_cogs, total_units, temp_other_price, vendor_funding

        tax_price = 0
        gross_revenue += order['order_total']
        total_units += order['items_order_quantity']

        for j in order['order_items']:
            pipeline = [
                {
                    "$match": {
                        "_id": j
                    }
                },
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }
                },
                {
                    "$unwind": {
                        "path": "$product_ins",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
                        "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
                        "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
                        "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                    }
                }
            ]
            result = list(OrderItems.objects.aggregate(*pipeline))
            if result:
                tax_price += result[0]['tax_price']
                temp_other_price += result[0]['price']

                if order['marketplace_name'] == "Amazon":
                    total_cogs += result[0]['total_cogs']
                else:
                    total_cogs += result[0]['w_total_cogs']
                
                vendor_funding += result[0]['vendor_funding']

    # Create threads for processing orders
    threads = []
    for order in result:
        thread = threading.Thread(target=process_order, args=(order,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Calculate net profit
    net_profit = (temp_other_price - total_cogs) + vendor_funding

    # Total values
    total = {
        "gross_revenue": round(gross_revenue, 2),
        "net_profit": round(net_profit, 2),
        "units_sold": round(total_units, 2)
    }
    return total



def get_top_movers(yesterday_data, previous_day_data):
    # Create lookup for previous day's data by SKU
    prev_data_map = {item['sku']: item for item in previous_day_data}
    
    changes = []
    
    for item in yesterday_data:
        sku = item['sku']
        yesterday_units = item['unitsSold']
        prev_units = prev_data_map.get(sku, {}).get('unitsSold', 0)
        
        change = yesterday_units - prev_units  # can be positive or negative
        
        changes.append({
            'sku': sku,
            "id" : item['id'],
            "asin" : item['asin'],
            "fulfillmentChannel" : item['fulfillmentChannel'],
            'product_name': item['product_name'],
            'images': item['images'],
            "unitsSold" : yesterday_units,
            "grossRevenue" : item['grossRevenue'],
            "netProfit" : item['netProfit'],
            "totalCogs" : round(item['totalCogs'],2),
            "netProfit" : item['netProfit'],
            "m_name": item.get("m_name", ""),
            'yesterday_units': yesterday_units,
            'previous_units': prev_units,
            'change_in_units': change,
        })
    
    top_increasing = sorted(
        [c for c in changes if c['change_in_units'] > 0],
        key=lambda x: x['change_in_units'],
        reverse=True
    )[:3]
    
    # Top 3 decreasing
    top_decreasing = sorted(
        [c for c in changes if c['change_in_units'] < 0],
        key=lambda x: x['change_in_units']
    )[:3]
    
    return {
        'top_3_products': top_increasing,
        'least_3_products': top_decreasing
    }