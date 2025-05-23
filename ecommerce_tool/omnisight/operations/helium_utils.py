from omnisight.models import Product, Order,pageview_session_count,OrderItems
from bson import ObjectId
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from ecommerce_tool.crud import DatabaseModel
import threading



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
                "order_status": {"$in": ['Shipped', 'Delivered']}
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

def get_date_range(preset):
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    if preset == "Today":
        return today, today + timedelta(days=1)
    elif preset == "Yesterday":
        return today - timedelta(days=1), today
    elif preset == "This Week":
        start = today - timedelta(days=today.weekday())
        return start, today
    elif preset == "This Month":
        start = today.replace(day=1)
        return start, today
    elif preset == "This Year":
        return today.replace(month=1, day=1), today
    
    elif preset == "Last Week":
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=7)
    elif preset == "Last 7 days":
        return today - timedelta(days=7), today - timedelta(days=1)
    elif preset == "Last 14 days":
        return today - timedelta(days=14), today - timedelta(days=1)
    elif preset == "Last 30 days":
        return today - timedelta(days=30), today - timedelta(days=1)
    elif preset == "Last 60 days":
        return today - timedelta(days=60), today - timedelta(days=1)
    elif preset == "Last 90 days":
        return today - timedelta(days=90), today - timedelta(days=1)
    
    elif preset == "Last Month":
        start = (today.replace(day=1) - relativedelta(months=1))
        return start, today.replace(day=1)
    elif preset == "This Quarter":
        quarter = (today.month - 1) // 3
        start = today.replace(month=quarter * 3 + 1, day=1)
        return start, start + relativedelta(months=3)
    elif preset == "Last Quarter":
        quarter = ((today.month - 1) // 3) - 1
        start = today.replace(month=quarter * 3 + 1, day=1)
        return start, start + relativedelta(months=3)
    
    elif preset == "Last Year":
        return today.replace(year=today.year - 1, month=1, day=1), today.replace(month=1, day=1)
    return today, today + timedelta(days=1)



def grossRevenue(start_date, end_date, marketplace_id=None,brand_id=None,product_id=None,manufacuture_name=[],fulfillment_channel=None):
    match=dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
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
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id" : 1,
                    "order_date": 1,
                    "order_items": 1,
                    "order_total": 1,
                    "marketplace_name": "$marketplace_ins.name",
                    "marketplace_id": 1,
                    "currency": 1,
                    "shipping_address": 1,
                    "shipping_information": 1,
                    "shipping_price" : {"$ifNull": ["$ShippingPrice", 0.0]},
                    "items_order_quantity" : {"$ifNull": ["$ItemsOrderQuantity", 0.0]},
                }
            }
        ]
    return list(Order.objects.aggregate(*pipeline))


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


def refundOrder(start_date, end_date, marketplace_id=None,brand_id=None,product_id=None,manufacuture_name=[],fulfillment_channel=None):
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
 
    # Choose the date format for grouping based on is_hourly
    date_format = "%Y-%m-%d %H:00" if is_hourly else "%Y-%m-%d"
 
    pipeline = [
        {
            "$match": {
                "order_date": {"$gte": start_date, "$lte": end_date},
                "order_status": {"$in": ['Shipped', 'Delivered']}
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
        {
            "$unwind": "$order_items_ins"
        },
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
        {
            "$sort": {"date": 1}  # Sort ascending by date/hour
        }
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
                "session_count": 1
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
    match['order_status__in'] = ['Shipped', 'Delivered']
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
                    total_cogs += result[0]['total_cogs']
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


def totalRevenueCalculation(start_date, end_date, marketplace_id=None,brand_id=None,product_id=None,manufacturer_name=None,fulfillment_channel=None):
    total = dict()
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    total_units = 0
    total_orders = 0
    temp_other_price = 0
    vendor_funding = 0

    result = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
    refund_ins = refundOrder(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
    refund_quantity_ins = 0
    if refund_ins != []:
        for ins in refund_ins:
            refund += ins['order_total']
            refund_quantity_ins += len(ins['order_items'])
    total_orders = len(result)
    if result != []:
        for ins in result:
            tax_price = 0
            gross_revenue += ins['order_total']
            # other_price = 0
            # temp_other_price = 0 
            for j in ins['order_items']:                  
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
                            "_id" : 0,
                            "price": {"$ifNull":["$Pricing.ItemPrice.Amount",0]},
                            "cogs": {"$ifNull":["$product_ins.cogs",0.0]},
                            "tax_price": {"$ifNull":["$Pricing.ItemTax.Amount",0]},
                            "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                            "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                            "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                if result != []:
                    tax_price += result[0]['tax_price']
                    temp_other_price += result[0]['price']
                    total_cogs += result[0]['total_cogs']
                    total_units += 1
                    vendor_funding += result[0]['vendor_funding'] 
        # other_price += ins['order_total'] - temp_other_price - tax_price

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