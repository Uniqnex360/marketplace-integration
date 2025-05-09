from omnisight.models import Product, Order, OrderItems, Marketplace, Brand, Category
from bson import ObjectId
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta


def getOrdersListBasedonProductId(productIds):
    """
    Fetches the list of orders based on the provided product ID using a pipeline aggregation.

    Args:
        productId (str): The ID of the product for which to fetch orders.

    Returns:
        list: A list of dictionaries containing order details.
    """
    pipeline = [
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
    ]
    orders = list(Order.objects.aggregate(*pipeline))
    if orders != []:
        orders = orders[0]['orderIds']
    return orders



def getproductIdListBasedonbrand(brandIds):
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
        orders = getOrdersListBasedonProductId(products[0]['productIds'])
    return orders


def getproductIdListBasedonManufacture(manufactureName = []):
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
        orders = getOrdersListBasedonProductId(products[0]['productIds'])
    return orders

def get_date_range(preset):
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    if preset == "Today":
        return today, today + timedelta(days=1)
    elif preset == "Yesterday":
        return today - timedelta(days=1), today
    elif preset == "This Week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=7)
    elif preset == "Last Week":
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=7)
    elif preset == "Last 7 days":
        return today - timedelta(days=6), today - timedelta(days=1)
    elif preset == "Last 14 days":
        return today - timedelta(days=13), today - timedelta(days=1)
    elif preset == "Last 30 days":
        return today - timedelta(days=29), today - timedelta(days=1)
    elif preset == "Last 60 days":
        return today - timedelta(days=59), today - timedelta(days=1)
    elif preset == "Last 90 days":
        return today - timedelta(days=89), today - timedelta(days=1)
    elif preset == "This Month":
        start = today.replace(day=1)
        return start, (start + relativedelta(months=1))
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
    elif preset == "This Year":
        return today.replace(month=1, day=1), today.replace(year=today.year + 1, month=1, day=1)
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
        ids = getproductIdListBasedonManufacture(manufacuture_name)
        match["_id"] = {"$in": ids}
    
    elif product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id)
        match["_id"] = {"$in": ids}

    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id)
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
                    "shipping_information": 1
                }
            },
        ]
    return list(Order.objects.aggregate(*pipeline))


def get_previous_periods(current_start, current_end):
    # Calculate the duration of the current period
    period_duration = current_end - current_start
    
    # Calculate previous periods
    previous_period = {
        'start': (current_start - period_duration).strftime('%b %d, %Y'),
        'end': (current_end - period_duration - timedelta(days=1)).strftime('%b %d, %Y')
    }
    
    previous_week = {
        'start': (current_start - timedelta(weeks=1)).strftime('%b %d, %Y'),
        'end': (current_end - timedelta(weeks=1) - timedelta(days=1)).strftime('%b %d, %Y')
    }
    
    previous_month = {
        'start': (current_start - relativedelta(months=1)).strftime('%b %d, %Y'),
        'end': (current_end - relativedelta(months=1) - timedelta(days=1)).strftime('%b %d, %Y')
    }
    
    previous_year = {
        'start': (current_start - relativedelta(years=1)).strftime('%b %d, %Y'),
        'end': (current_end - relativedelta(years=1) - timedelta(days=1)).strftime('%b %d, %Y')
    }
    
    response_data = {
        'previous_period': previous_period,
        'previous_week': previous_week,
        'previous_month': previous_month,
        'previous_year': previous_year,
        'current_period': {
            'start': current_start.strftime('%b %d, %Y'),
            'end': (current_end - timedelta(days=1)).strftime('%b %d, %Y')
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
        ids = getproductIdListBasedonManufacture(manufacuture_name)
        match["_id"] = {"$in": ids}

    elif product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id)
        match["_id"] = {"$in": ids}

    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id)
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



