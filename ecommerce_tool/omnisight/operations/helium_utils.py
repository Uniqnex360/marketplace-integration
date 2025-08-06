from __future__ import annotations
from omnisight.models import Product, Order,pageview_session_count,OrderItems,Marketplace
from bson import ObjectId
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from ecommerce_tool.crud import DatabaseModel
import threading
import math
import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

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
    match['order_status'] = {"$nin": ["Canceled", "Cancelled"]}
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
                "shipping_price": {"$ifNull": ["$shipping_price", 0.0]},
                "items_order_quantity": {"$ifNull": ["$items_order_quantity", 0]},
            }
        }
    ]
    
    order_list = list(Order.objects.aggregate(*pipeline))
    
    for order_ins in order_list:
        for marketplace in marketplace_list:
            order_ins['marketplace_name'] = marketplace['name']
        tax=order_ins.get('TaxAmount',0.0)
        if isinstance(tax,dict):
            tax=tax.get('Amount',0.0)
        order_ins['order_total']=round(order_ins.get('order_total',0.0)-tax,2)
    
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
    import pytz
    
    # Store the original timezone for later conversion
    user_timezone = pytz.timezone(timezone) if timezone != 'UTC' else pytz.UTC
    
    # Store original dates in user timezone
    original_start_date = start_date
    original_end_date = end_date
    
    # Convert to UTC for database queries
    if timezone != 'UTC':
        start_date_utc, end_date_utc = convertLocalTimeToUTC(start_date, end_date, timezone)
    else:
        start_date_utc = start_date
        end_date_utc = end_date
    
    # Remove timezone info for MongoDB query
    start_date_utc = start_date_utc.replace(tzinfo=None)
    end_date_utc = end_date_utc.replace(tzinfo=None)
    
    # Create time buckets and maintain a mapping of UTC keys to local dates
    bucket_to_local_date_map = {}
    
    if preset in ["Today", "Yesterday"]:
        # For hourly data, work with UTC buckets
        time_buckets = [(start_date_utc + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) 
                      for i in range(24)]
        time_format = "%Y-%m-%d %H:00:00"
    else:
        # For daily data, create buckets based on the requested date range
        time_buckets = []
        time_format = "%Y-%m-%d 00:00:00"
        
        # Start from beginning of first day
        current_date = original_start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # End at beginning of day after last day
        end_date_midnight = original_end_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        
        while current_date < end_date_midnight:
            # Convert local midnight to UTC
            utc_bucket = current_date.astimezone(pytz.UTC).replace(tzinfo=None)
            time_buckets.append(utc_bucket)
            
            # Store the mapping of UTC bucket to local date
            utc_key = utc_bucket.strftime(time_format)
            local_date_key = current_date.strftime(time_format)
            bucket_to_local_date_map[utc_key] = local_date_key
            
            current_date += timedelta(days=1)

    # Initialize graph data with all time periods
    graph_data = {}
    
    for dt in time_buckets:
        time_key = dt.strftime(time_format)
        graph_data[time_key] = {
            "gross_revenue": 0,
            "net_profit": 0,
            "profit_margin": 0,
            "orders": 0,
            "units_sold": 0,
            "refund_amount": 0,
            "refund_quantity": 0
        }

    # For the overall query, we need to extend the end date to capture all orders
    # that fall within the last day when converted to user timezone
    if preset not in ["Today", "Yesterday"] and timezone != 'UTC':
        # Add one day to ensure we capture all orders in the last day
        query_end_date = end_date_utc + timedelta(days=1)
    else:
        query_end_date = end_date_utc

    # Get all orders for the entire range
    match = {
        'order_status__in': ['Shipped', 'Delivered', 'Acknowledged', 'Pending', 'Unshipped', 'PartiallyShipped'],
        'order_date__gte': start_date_utc,
        'order_date__lt': query_end_date
    }

    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel
    if marketplace_id not in [None, "", "all", "custom"]:
        match['marketplace_id'] = ObjectId(marketplace_id)
    
    if manufacturer_name not in [None, "", []]:
        ids = getproductIdListBasedonManufacture(manufacturer_name, start_date_utc, end_date_utc)
        match["id__in"] = ids
    elif product_id not in [None, "", []]:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id, start_date_utc, end_date_utc)
        match["id__in"] = ids
    elif brand_id not in [None, "", []]:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id, start_date_utc, end_date_utc)
        match["id__in"] = ids

    # Get orders by bucket
    orders_by_bucket = {}
    for dt in time_buckets:
        bucket_start = dt
        if preset in ["Today", "Yesterday"]:
            bucket_end = dt + timedelta(hours=1)
        else:
            bucket_end = dt + timedelta(days=1)
        
        bucket_match = match.copy()
        bucket_match['order_date__gte'] = bucket_start
        bucket_match['order_date__lt'] = bucket_end
        
        orders = DatabaseModel.list_documents(Order.objects, bucket_match)
        orders_by_bucket[dt.strftime(time_format)] = list(orders)

    def process_time_bucket(time_key):
        nonlocal graph_data, orders_by_bucket

        bucket_orders = orders_by_bucket.get(time_key, [])
        gross_revenue = 0
        total_cogs = 0
        refund_amount = 0
        refund_quantity = 0
        total_units = 0
        temp_other_price = 0
        vendor_funding = 0

        bucket_start = datetime.strptime(time_key, time_format).replace(tzinfo=pytz.UTC)
        if preset in ["Today", "Yesterday"]:
            bucket_end = bucket_start + timedelta(hours=1)
        else:
            bucket_end = bucket_start + timedelta(days=1)

        # Calculate refunds
        refund_ins = refundOrder(bucket_start, bucket_end, marketplace_id, brand_id, product_id)
        if refund_ins:
            for ins in refund_ins:
                if bucket_start <= ins['order_date'] < bucket_end:
                    refund_amount += ins['order_total']
                    refund_quantity += len(ins['order_items'])

        # Process each order in the bucket
        for order in bucket_orders:
            gross_revenue += order.order_total
            total_units += order.items_order_quantity if order.items_order_quantity else 0
            for item in order.order_items:
                pipeline = [
                    {"$match": {"_id": item.id}},
                    {"$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }},
                    {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                    {"$project": {
                        "_id": 0,
                        "price": {"$ifNull": ["$Pricing.ItemPrice.Amount", 0]},
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "tax_price": {"$ifNull": ["$Pricing.ItemTax.Amount", 0]},
                        "total_cogs": {"$ifNull": ["$product_ins.total_cogs", 0]},
                        "w_total_cogs": {"$ifNull": ["$product_ins.w_total_cogs", 0]},
                        "vendor_funding": {"$ifNull": ["$product_ins.vendor_funding", 0]},
                    }}
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                if result:
                    temp_other_price += result[0]['price']
                    total_cogs += result[0]['total_cogs'] if order.marketplace_id.name == "Amazon" else result[0]['w_total_cogs']
                    vendor_funding += result[0]['vendor_funding']

        # Calculate metrics
        net_profit = (temp_other_price - total_cogs) + vendor_funding
        profit_margin = round((net_profit / gross_revenue) * 100, 2) if gross_revenue else 0

        graph_data[time_key] = {
            "gross_revenue": round(gross_revenue, 2),
            "net_profit": round(net_profit, 2),
            "profit_margin": profit_margin,
            "orders": len(bucket_orders),
            "units_sold": total_units,
            "refund_amount": round(refund_amount, 2),
            "refund_quantity": refund_quantity
        }

    # Process time buckets with limited threading
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_time_bucket, time_key): time_key for time_key in graph_data}
        for future in futures:
            future.result()

    # Convert to final output with correct dates
    converted_graph_data = {}
    
    # Get the requested date range (just the dates)
    start_date_only = original_start_date.date()
    end_date_only = original_end_date.date()
    
    if preset in ["Today", "Yesterday"]:
        # For hourly data, convert normally
        for utc_time_key, data in graph_data.items():
            utc_dt = datetime.strptime(utc_time_key, time_format).replace(tzinfo=pytz.UTC)
            local_dt = utc_dt.astimezone(user_timezone)
            local_time_key = local_dt.strftime(time_format)
            
            converted_graph_data[local_time_key] = data
            converted_graph_data[local_time_key]["current_date"] = local_time_key
    else:
        # For daily data, use the pre-mapped local dates
        for utc_time_key, data in graph_data.items():
            # Get the correct local date from our mapping
            local_time_key = bucket_to_local_date_map.get(utc_time_key)
            
            if local_time_key:
                # Parse the local date to check if it's in range
                local_date = datetime.strptime(local_time_key, time_format).date()
                
                # Only include if the date is within the original requested range
                if start_date_only <= local_date <= end_date_only:
                    converted_graph_data[local_time_key] = data
                    converted_graph_data[local_time_key]["current_date"] = local_time_key

    return converted_graph_data


def totalRevenueCalculation(
    start_date, end_date, marketplace_id=None, brand_id=None, product_id=None,
    manufacturer_name=None, fulfillment_channel=None, timezone_str="UTC"
):
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

    # Fetch all orders and refunds
    result = grossRevenue(
        start_date, end_date, marketplace_id, brand_id, product_id,
        manufacturer_name, fulfillment_channel, timezone_str
    )
    refund_ins = refundOrder(
        start_date, end_date, marketplace_id, brand_id, product_id,
        manufacturer_name, fulfillment_channel, timezone_str
    )

    # Calculate refund totals
    if refund_ins:
        for ins in refund_ins:
            refund += ins['order_total']
            refund_quantity_ins += len(ins['order_items'])

    total_orders = len(result)

    # --- Batch fetch all order item IDs ---
    all_item_ids = []
    for order in result:
        all_item_ids.extend(order['order_items'])

    # Remove duplicates, just in case
    all_item_ids = list(set(all_item_ids))

    # --- Batch fetch all OrderItems ---
    item_docs = list(OrderItems.objects.filter(_id__in=all_item_ids))
    item_dict = {str(item['_id']): item for item in item_docs}

    # --- Batch fetch all Product IDs ---
    product_ids = set()
    for item in item_docs:
        pid = item.get('ProductDetails', {}).get('product_id')
        if pid:
            product_ids.add(pid)

    # --- Batch fetch all Products ---
    product_docs = list(Product.objects.filter(_id__in=list(product_ids)))
    product_dict = {str(prod['_id']): prod for prod in product_docs}

    # --- Process orders ---
    for order in result:
        gross_revenue += order['order_total']
        total_units += order['items_order_quantity']

        for item_id in order['order_items']:
            item = item_dict.get(str(item_id))
            if not item:
                continue
            product_id = item.get('ProductDetails', {}).get('product_id')
            product = product_dict.get(str(product_id))
            price = item.get('Pricing', {}).get('ItemPrice', {}).get('Amount', 0)
            tax_price = item.get('Pricing', {}).get('ItemTax', {}).get('Amount', 0)
            cogs = product.get('cogs', 0.0) if product else 0.0
            total_cogs_val = product.get('total_cogs', 0) if product else 0
            w_total_cogs = product.get('w_total_cogs', 0) if product else 0
            vendor_funding_val = product.get('vendor_funding', 0) if product else 0

            temp_other_price += price
            if order['marketplace_name'] == "Amazon":
                total_cogs += total_cogs_val
            else:
                total_cogs += w_total_cogs
            vendor_funding += vendor_funding_val

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
    """Optimized version with better database queries and caching"""
    
    # Initialize variables
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

    # Batch process orders and refunds in parallel
    with ThreadPoolExecutor(max_workers=3) as executor:
        orders_future = executor.submit(
            grossRevenue, from_date, to_date, marketplace_id, brand_id, 
            product_id, manufacturer_name, fulfillment_channel, timezone
        )
        refunds_future = executor.submit(
            refundOrder, from_date, to_date, marketplace_id, brand_id, 
            product_id, manufacturer_name, fulfillment_channel, timezone
        )
        
        result = orders_future.result()
        refund_ins = refunds_future.result()
        refund = len(refund_ins)

    if not result:
        return {
            "grossRevenue": 0, "expenses": 0, "netProfit": 0, "roi": 0,
            "unitsSold": 0, "refunds": refund, "skuCount": 0, "sessions": 0,
            "pageViews": 0, "unitSessionPercentage": 0, "margin": 0, "orders": 0
        }

    # Get all item IDs efficiently
    all_item_ids = [ObjectId(item_id) for order in result for item_id in order['order_items']]
    
    if timezone != 'UTC':
        from_date, to_date = convertLocalTimeToUTC(from_date, to_date, timezone)
    
    from_date = from_date.replace(tzinfo=None)
    to_date = to_date.replace(tzinfo=None)

    # Optimized item pipeline with better indexing hints
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
                "p_id": "$product_ins._id",
                "price": "$Pricing.ItemPrice.Amount",
                "tax_price": "$Pricing.ItemTax.Amount",
                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                "sku": "$product_ins.sku",
                "total_cogs": { "$ifNull": ["$product_ins.total_cogs", 0] },
                "w_total_cogs": { "$ifNull": ["$product_ins.w_total_cogs", 0] },
                "vendor_funding": { "$ifNull": ["$product_ins.vendor_funding", 0] },
                "a_shipping_cost": {"$ifNull": ["$product_ins.a_shipping_cost", 0]},
                "w_shiping_cost": {"$ifNull": ["$product_ins.w_shiping_cost", 0]},
            }
        }
    ]

    # Execute item details query
    item_details_map = {str(item['_id']): item for item in OrderItems.objects.aggregate(*item_pipeline)}

    def process_order(order):
        nonlocal gross_revenue, temp_price, tax_price, total_cogs, vendor_funding, total_units, sku_set, page_views, sessions, shipping_cost, p_id

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

                if item_data.get('p_id'):
                    p_id.add(item_data['p_id'])

    # Process orders with better threading control
    if use_threads and len(result) > 100:  # Only use threads for large datasets
        with ThreadPoolExecutor(max_workers=min(4, len(result) // 25)) as executor:
            futures = [executor.submit(process_order, order) for order in result]
            for future in as_completed(futures):
                future.result()
    else:
        for order in result:
            process_order(order)

    # Get page views and sessions in parallel if we have product IDs
    if p_id:
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
        if p_result:
            page_views = p_result[0].get('page_views', 0)
            sessions = p_result[0].get('sessions', 0)

    # Calculate final metrics
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
            "shipping_cost": round(shipping_cost, 2),
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