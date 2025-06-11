from omnisight.models import Product, Order,pageview_session_count,OrderItems,Marketplace
from bson import ObjectId
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from ecommerce_tool.crud import DatabaseModel
import threading
import pandas as pd
from pytz import timezone



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
        return start, (start + timedelta(days=6)).replace(hour=23, minute=59, second=59)
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
    import pytz
    
    # Convert local timezone dates to UTC
    if timezone != 'UTC':
        local_tz = pytz.timezone(timezone)
        
        # If dates are naive (no timezone), localize them
        if start_date.tzinfo is None:
            start_date = local_tz.localize(start_date)
        if end_date.tzinfo is None:
            end_date = local_tz.localize(end_date)
        
        # Convert to UTC
        start_date = start_date.astimezone(pytz.UTC)
        end_date = end_date.astimezone(pytz.UTC)
    
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
    match['order_status'] = {"$in": ['Shipped', 'Delivered', 'Acknowledged', 'Pending', 'Unshipped', 'PartiallyShipped']}
    
    # Rest of your existing code...
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
                "marketplace_id": 1,
                "currency": 1,
                "shipping_address": 1,
                "shipping_information": 1,
                "shipping_price": {"$ifNull": ["$ShippingPrice", 0.0]},
                "items_order_quantity": {"$ifNull": ["$ItemsOrderQuantity", 0]},
            }
        }
    ]
    
    order_list = list(Order.objects.aggregate(*pipeline))
    
    for order_ins in order_list:
        for marketplace in marketplace_list:
            order_ins['marketplace_name'] = marketplace['name']
        del order_ins['marketplace_id']
    
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
    import pytz
    
    # Convert local timezone dates to UTC
    if timezone != 'UTC':
        local_tz = pytz.timezone(timezone)
        
        # If dates are naive (no timezone), localize them
        if start_date.tzinfo is None:
            start_date = local_tz.localize(start_date)
        if end_date.tzinfo is None:
            end_date = local_tz.localize(end_date)
        
        # Convert to UTC
        start_date = start_date.astimezone(pytz.UTC)
        end_date = end_date.astimezone(pytz.UTC)
    
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
    """
    Fetch total quantity and price of a product sold between start_date and end_date,
    grouped by day or hour based on is_hourly flag.
 
    Args:
        start_date (datetime): Start date/time for filtering orders.
        end_date (datetime): End date/time for filtering orders.
        product_id (str): The product ID to filter by.
        is_hourly (bool): If True, group by hour; else group by day.
 
    Returns:
        list: List of dicts with keys 'date', 'total_quantity', and 'total_price'.
    """
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



def get_graph_data(start_date, end_date, preset,marketplace_id,brand_id=None,product_id=None,manufacturer_name=None,fulfillment_channel=None):
    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Determine time buckets based on preset
    if preset in ["Today", "Yesterday"]:
        # Hourly data for 24 hours
        time_buckets = [(start_date + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) 
                      for i in range(24)]
        time_format = "%Y-%m-%d %H:00:00"
    else:
        # # Daily data - include up to today
        # if preset in ["This Week", "This Month", "This Quarter", "This Year"]:
        #     # For current periods, include days up to today
        #     days = (end_date - start_date).days + 1
        # else:
        #     # For past periods, include all days
        #     days = (end_date - start_date).days +1 
        days = (end_date - start_date).days +1
     
          
        time_buckets = [(start_date + timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0) 
                      for i in range(days)]
        time_format = "%Y-%m-%d 00:00:00"

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

    # Get all orders grouped by time bucket
    orders_by_bucket = {}
    match = dict()
    match['order_status__in'] = ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    if product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id)
        match["id__in"] = ids

    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id)
        match["id__in"] = ids
    for dt in time_buckets:
        bucket_start = dt
        if preset in ["Today", "Yesterday"]:
            bucket_end = dt + timedelta(hours=1)
        else:
            bucket_end = dt + timedelta(days=1)
        # 2️⃣ Fetch all Shipped/Delivered orders for the 24-hour period
        
        match['order_date__gte'] = bucket_start
        match['order_date__lte'] = bucket_end

        orders = DatabaseModel.list_documents(Order.objects,match)
        orders_by_bucket[dt.strftime(time_format)] = list(orders)

    def process_time_bucket(time_key):
        nonlocal graph_data, orders_by_bucket, preset, marketplace_id, brand_id, product_id

        bucket_orders = orders_by_bucket.get(time_key, [])
        gross_revenue = 0
        total_cogs = 0
        refund_amount = 0
        refund_quantity = 0
        total_units = 0
        other_price = 0
        temp_other_price = 0
        vendor_funding = 0

        bucket_start = datetime.strptime(time_key, "%Y-%m-%d %H:00:00")
        if preset in ["Today", "Yesterday"]:
            bucket_end = bucket_start.replace(minute=59, second=59)
        else:
            bucket_end = bucket_start.replace(hour=23, minute=59, second=59)

        # Calculate refunds first
        refund_ins = refundOrder(bucket_start, bucket_end, marketplace_id, brand_id, product_id)
        if refund_ins:
            for ins in refund_ins:
                if ins['order_date'] >= bucket_start and ins['order_date'] < bucket_end:
                    refund_amount += ins['order_total']
                    refund_quantity += len(ins['order_items'])

        # Process each order in the bucket
        for order in bucket_orders:
            gross_revenue += order.order_total
            tax_price = 0

            for item in order.order_items:
                # Get product and COGS
                pipeline = [
                    {
                        "$match": {
                            "_id": item.id
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
                    temp_other_price += result[0]['price']
                    
                    if order.marketplace_id.name == "Amazon":
                        total_cogs += result[0]['total_cogs'] 
                    else:
                        total_cogs += result[0]['w_total_cogs']
                    total_units += 1
                    tax_price += result[0]['tax_price']
                    vendor_funding += result[0]['vendor_funding']

        # Calculate net profit and margin
        net_profit = (temp_other_price - total_cogs) + vendor_funding
        profit_margin = round((net_profit / gross_revenue) * 100, 2) if gross_revenue else 0

        # Update graph data for this time bucket
        graph_data[time_key] = {
            "gross_revenue": round(gross_revenue, 2),
            "net_profit": round(net_profit, 2),
            "profit_margin": profit_margin,
            "orders": len(bucket_orders),
            "units_sold": total_units,
            "refund_amount": round(refund_amount, 2),
            "refund_quantity": refund_quantity,
            "date": time_key
        }

    # Create threads for each time bucket
    threads = []
    for time_key in graph_data:
        thread = threading.Thread(target=process_time_bucket, args=(time_key,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    return graph_data


def totalRevenueCalculation(start_date, end_date, marketplace_id=None, brand_id=None, product_id=None, manufacturer_name=None, fulfillment_channel=None):
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

    result = grossRevenue(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)
    refund_ins = refundOrder(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)

    if refund_ins:
        for ins in refund_ins:
            refund += ins['order_total']
            refund_quantity_ins += len(ins['order_items'])

    total_orders = len(result)

    def process_order(order):
        nonlocal gross_revenue, total_cogs, total_units, temp_other_price, vendor_funding

        tax_price = 0
        gross_revenue += order['order_total']

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
                total_units += 1
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
    import pytz
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

    result = grossRevenue(from_date, to_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone)
    all_item_ids = [ObjectId(item_id) for order in result for item_id in order['order_items']]
    refund_ins = refundOrder(from_date, to_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel,timezone)
    refund = len(refund_ins)
    
    
    # Convert local timezone dates to UTC
    if timezone != 'UTC':
        local_tz = pytz.timezone(timezone)
        
        # If dates are naive (no timezone), localize them
        if from_date.tzinfo is None:
            from_date = local_tz.localize(from_date)
        if to_date.tzinfo is None:
            to_date = local_tz.localize(to_date)
        
        # Convert to UTC
        from_date = from_date.astimezone(pytz.UTC)
        to_date = to_date.astimezone(pytz.UTC)
    
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
        nonlocal gross_revenue, temp_price, tax_price, total_cogs, vendor_funding, total_units, sku_set, page_views, sessions, shipping_cost

        gross_revenue += order['order_total']
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
                total_units += 1

                if item_data.get('sku'):
                    sku_set.add(item_data['sku'])

                try:
                    pipeline = [
                        {
                            "$match": {
                                "product_id": {"$in": [item_data['p_id']]},
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
                    result = list(pageview_session_count.objects.aggregate(*pipeline))
                    for P_ins in result:
                        page_views += P_ins.get('page_views', 0)
                        sessions += P_ins.get('sessions', 0)
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



def totalRevenueCalculationForProduct(start_date, end_date, marketplace_id=None, brand_id=None, product_id=None, manufacturer_name=None, fulfillment_channel=None):
    total = dict()
    gross_revenue = 0
    total_cogs = 0
    net_profit = 0
    total_units = 0
    temp_other_price = 0
    vendor_funding = 0

    result = grossRevenue(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)


    def process_order(order):
        nonlocal gross_revenue, total_cogs, total_units, temp_other_price, vendor_funding

        tax_price = 0
        gross_revenue += order['order_total']

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
                total_units += 1
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