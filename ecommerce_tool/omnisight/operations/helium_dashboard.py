from mongoengine import Q
from omnisight.models import OrderItems,Order,Marketplace,Product,CityDetails,user,notes_data,chooseMatrix,Fee,Refund
from mongoengine.queryset.visitor import Q
from dateutil.relativedelta import relativedelta
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime,timedelta
from bson.son import SON
from django.http import JsonResponse
from django.http import HttpResponse
import openpyxl
import csv
from collections import OrderedDict, defaultdict
from io import StringIO
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font
import io
from pytz import timezone
from bson import ObjectId
from calendar import monthrange
from ecommerce_tool.settings import MARKETPLACE_ID,SELLER_ID


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
        return today - timedelta(days=6), today + timedelta(days=1)
    elif preset == "Last 14 days":
        return today - timedelta(days=13), today + timedelta(days=1)
    elif preset == "Last 30 days":
        return today - timedelta(days=29), today + timedelta(days=1)
    elif preset == "Last 60 days":
        return today - timedelta(days=59), today + timedelta(days=1)
    elif preset == "Last 90 days":
        return today - timedelta(days=89), today + timedelta(days=1)
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



def grossRevenue(start_date, end_date, marketplace_id=None):
    match=dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
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
                    "currency": 1
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


def refundOrder(start_date, end_date, marketplace_id=None):
    match=dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = "Refunded"
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
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

def get_metrics_by_date_range(request):
    marketplace_id = request.GET.get('marketplace_id',None)
    target_date_str = request.GET.get('target_date')
    # Parse target date and previous day
    target_date = datetime.strptime(target_date_str, "%d/%m/%Y")
    previous_date = target_date - timedelta(days=1)
    eight_days_ago = target_date - timedelta(days=8)

    # Define the date filters
    date_filters = {
        "targeted": {
            "start": datetime(target_date.year, target_date.month, target_date.day),
            "end": datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
        },
        "previous": {
            "start": datetime(previous_date.year, previous_date.month, previous_date.day),
            "end": datetime(previous_date.year, previous_date.month, previous_date.day, 23, 59, 59)
        }
    }

    # Define the last 8 days filter as a dictionary with each day's range
    last_8_days_filter = {}
    for i in range(1, 9):
        day = eight_days_ago + timedelta(days=i)
        day_key = day.strftime("%B %d, %Y").lower()
        last_8_days_filter[day_key] = {
            "start": datetime(day.year, day.month, day.day),
            "end": datetime(day.year, day.month, day.day, 23, 59, 59)
        }

    metrics = {}
    graph_data = {}

    for key, date_range in last_8_days_filter.items():
        gross_revenue = 0
        result = grossRevenue(date_range["start"], date_range["end"])
        if result != []:            
            for ins in result:
                gross_revenue += ins['order_total']
                
        graph_data[key] = {
            "gross_revenue": round(gross_revenue, 2),
        }
    metrics["graph_data"] = graph_data
    for key, date_range in date_filters.items():
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        margin = 0
        net_profit = 0
        total_units = 0
        total_orders = 0
        tax_price = 0

        result = grossRevenue(date_range["start"], date_range["end"],marketplace_id)
        refund_ins = refundOrder(date_range["start"], date_range["end"],marketplace_id)
        if refund_ins != []:
            for ins in refund_ins:
                refund += len(ins['order_items'])
        total_orders = len(result)
        if result != []:
            for ins in result:
                tax_price = 0
                gross_revenue += ins['order_total']
                other_price = 0
                temp_other_price = 0 
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
                                "price": "$Pricing.ItemPrice.Amount",
                                "cogs": {"$ifNull":["$product_ins.cogs",0.0]},
                                "tax_price": "$Pricing.ItemTax.Amount",
                            }
                        }
                    ]
                    result = list(OrderItems.objects.aggregate(*pipeline))
                    tax_price += result[0]['tax_price']
                    temp_other_price += result[0]['price']
                    total_cogs += result[0]['cogs']
                    total_units += 1
            other_price += ins['order_total'] - temp_other_price - tax_price

            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100
        metrics[key] = {
            "gross_revenue": round(gross_revenue, 2),
            "total_cogs": round(total_cogs, 2),
            "refund": round(refund, 2),
            "margin": round(margin, 2),
            "net_profit": round(net_profit, 2),
            "total_orders": round(total_orders, 2),
            "total_units": round(total_units, 2)
        }

    difference = {
        "gross_revenue": round(metrics["targeted"]["gross_revenue"] - metrics["previous"]["gross_revenue"],2),
        "total_cogs": round(metrics["targeted"]["total_cogs"] - metrics["previous"]["total_cogs"],2),
        "refund": round(metrics["targeted"]["refund"] - metrics["previous"]["refund"],2),
        "margin": round(metrics["targeted"]["margin"] - metrics["previous"]["margin"],2),
        "net_profit": round(metrics["targeted"]["net_profit"] - metrics["previous"]["net_profit"],2),
        "total_orders": round(metrics["targeted"]["total_orders"] - metrics["previous"]["total_orders"],2),
        "total_units": round(metrics["targeted"]["total_units"] - metrics["previous"]["total_units"],2),
    }
    metrics['targeted']["business_value"] = AnnualizedRevenueAPIView(target_date)
    name = "Today Snapshot"
    item_pipeline = [
                        { "$match": { "name": name } }
                    ]
    item_result = list(chooseMatrix.objects.aggregate(*item_pipeline))
    if item_result:
        item_result = item_result[0]
        if item_result['select_all']:
            pass
        if item_result['gross_revenue'] == False:
            del metrics['targeted']["gross_revenue"]
            del metrics['previous']["gross_revenue"]
        if item_result['units_sold'] == False:
            del metrics['targeted']["total_units"]
            del metrics['previous']["total_units"]
        if item_result['total_cogs'] == False:
            del metrics['targeted']["total_cogs"]
            del metrics['previous']["total_cogs"]
        if item_result['business_value'] == False:
            del metrics['targeted']["business_value"]
            # del metrics['previous']["business_value"]
        if item_result['orders'] == False:
            del metrics['targeted']["total_orders"]
            del metrics['previous']["total_orders"]
        if item_result['refund_quantity'] == False:
            del metrics['targeted']["refund"]
            del metrics['previous']["refund"]
        if item_result['profit_margin'] == False:
            del metrics['targeted']["margin"]
            del metrics['previous']["margin"]
    metrics["difference"] = difference
    return metrics


def LatestOrdersTodayAPIView(request):
    marketplace_id = request.GET.get('marketplace_id', None)
    today = datetime.utcnow().date()
    start_of_day = datetime.combine(today, datetime.min.time())
    end_of_day = datetime.combine(today, datetime.max.time())

    match = dict()
    match['order_date'] = {"$gte": start_of_day, "$lte": end_of_day}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    # 1️⃣ Hourly aggregation: Use order-level date for bucket, sum quantities from items
    hourly_pipeline = [
        {
            "$match": match
        },
        {
            "$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",
                "as": "items"
            }
        },
        {
            "$addFields": {
                "bucket": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:00:00",
                        "date": "$order_date",
                        "timezone": "UTC"
                    }
                },
                "unitsCount": {
                    "$sum": {
                        "$map": {
                            "input": "$items",
                            "as": "it",
                            "in": "$$it.ProductDetails.QuantityOrdered"
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$bucket",
                "unitsCount": { "$sum": "$unitsCount" },
                "ordersCount": { "$sum": 1 }
            }
        },
        {
            "$project": {
                "_id": 0,
                "k": "$_id",
                "v": {
                    "unitsCount": "$unitsCount",
                    "ordersCount": "$ordersCount"
                }
            }
        },
        {
            "$sort": { "k": 1 }
        }
    ]

    raw = list(Order.objects.aggregate(*hourly_pipeline))
    chart_dict = {r["k"]: r["v"] for r in raw}

    # Fill missing hours
    filled = {}
    bucket_time = start_of_day.replace(minute=0, second=0, microsecond=0)
    for i in range(25):
        key = bucket_time.strftime("%Y-%m-%d %H:00:00")
        filled[key] = chart_dict.get(key, {"unitsCount": 0, "ordersCount": 0})
        bucket_time += timedelta(hours=1)

    # 2️⃣ Detailed order info
    detail_pipeline = [
        {
            "$match": match
        },
        {
            "$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",
                "as": "items"
            }
        },
        { "$unwind": "$items" },
        {
            "$lookup": {
                "from": "product",
                "localField": "items.ProductDetails.product_id",
                "foreignField": "_id",
                "as": "product"
            }
        },
        { "$unwind": { "path": "$product", "preserveNullAndEmptyArrays": True } },
        {
            "$addFields": {
                "sellerSku": "$items.ProductDetails.SKU",
                "unitPrice": "$items.Pricing.ItemPrice.Amount",
                "quantityOrdered": "$items.ProductDetails.QuantityOrdered",
                "title": "$items.ProductDetails.Title",
                "imageUrl": "$product.image_url",
                "purchaseDate": "$order_date",
                "orderId": "$purchase_order_id"
            }
        },
        {
            "$project": {
                "_id": 0,
                "sellerSku": 1,
                "title": 1,
                "quantityOrdered": 1,
                "imageUrl": 1,
                "price": { "$multiply": ["$unitPrice", "$quantityOrdered"] },
                "purchaseDate": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:%M:%S",
                        "date": "$purchaseDate",
                        "timezone": "UTC"
                    }
                }
            }
        },
        {
            "$sort": { "purchaseDate": -1 }
        }
    ]

    orders = list(Order.objects.aggregate(*detail_pipeline))
    data = dict()
    data = {
        "orders": orders,
        "hourly_order_count": filled
    }
    return data


def LatestOrdersTodayAPIView(request):
    marketplace_id = request.GET.get('marketplace_id', None)
    # 1️⃣ Compute bounds for "today" based on the user's local timezone
    user_timezone = request.GET.get('timezone', 'US/Pacific')  # Default to US/Pacific if no timezone is provided
    local_tz = timezone(user_timezone)

    now = datetime.now(local_tz)
    # For a 24-hour period ending now
    start_of_day = now - timedelta(hours=24)
    end_of_day = now

    # 2️⃣ Fetch all Shipped/Delivered orders for the 24-hour period
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        qs = Order.objects.filter(
            order_date__gte=start_of_day,
            order_date__lte=end_of_day,
            order_status__in=["Shipped", "Delivered"],
            marketplace_id=ObjectId(marketplace_id)
        )
    else:
        qs = Order.objects.filter(
            order_date__gte=start_of_day,
            order_date__lte=end_of_day,
            order_status__in=["Shipped", "Delivered"]
        )

    # 3️⃣ Pre-fill a 24-slot OrderedDict for every hour in the time range
    chart = OrderedDict()
    bucket = start_of_day.replace(minute=0, second=0, microsecond=0)
    for _ in range(25):  # 25 to include the current hour
        key = bucket.strftime("%Y-%m-%d %H:00:00")
        chart[key] = {"ordersCount": 0, "unitsCount": 0}
        bucket += timedelta(hours=1)

    # 4️⃣ Build the detail array + populate chart
    orders_out = []
    for order in qs:
        # Convert order_date to user's timezone for consistent bucketing
        order_local_time = order.order_date.astimezone(local_tz)
        
        # hour bucket for this order
        bk = order_local_time.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00:00")
        
        # Only process if the bucket exists in our chart
        if bk in chart:
            chart[bk]["ordersCount"] += 1
            
            # iterate each OrderItems instance referenced on this order
            for item in order.order_items:
                sku = item.ProductDetails.SKU
                asin = item.ProductDetails.ASIN if hasattr(item.ProductDetails, 'ASIN') and item.ProductDetails.ASIN is not None else ""
                qty = item.ProductDetails.QuantityOrdered
                unit_price = item.Pricing.ItemPrice.Amount
                title = item.ProductDetails.Title
                # lazy-load the Product doc for image_url
                prod_ref = item.ProductDetails.product_id
                img_url = prod_ref.image_url if prod_ref else None

                total_price = round(unit_price * qty, 2)
                purchase_dt = order_local_time.strftime("%Y-%m-%d %H:%M:%S")

                orders_out.append({
                    "sellerSku": sku,
                    "asin": asin,
                    "title": title,
                    "quantityOrdered": qty,
                    "imageUrl": img_url,
                    "price": total_price,
                    "purchaseDate": purchase_dt
                })

                # add to units count
                chart[bk]["unitsCount"] += qty

    # 5️⃣ sort orders by most recent purchaseDate
    orders_out.sort(key=lambda o: o["purchaseDate"], reverse=True)
    
    # Convert chart to list format for easier frontend consumption
    chart_list = [{"hour": hour, **data} for hour, data in chart.items()]
    
    data = {
        "orders": orders_out,
        "hourly_order_count": chart_list
    }
    return data



def get_graph_data(start_date, end_date, preset,marketplace_id):
    marketplace_boolean = False
    # 2️⃣ Fetch all Shipped/Delivered orders for the 24-hour period
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        marketplace_boolean = True
    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Determine time buckets based on preset
    if preset in ["Today", "Yesterday"]:
        # Hourly data for 24 hours
        time_buckets = [(start_date + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) 
                      for i in range(24)]
        time_format = "%Y-%m-%d %H:00:00"
    else:
        # Daily data - only up to current day
        if preset in ["This Week", "This Month", "This Quarter", "This Year"]:
            # For current periods, only include days up to today
            last_day = min(end_date, today + timedelta(days=1))  # Include today
            days = (last_day - start_date).days
        else:
            # For past periods, include all days
            days = (end_date - start_date).days
            
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
    for dt in time_buckets:
        bucket_start = dt
        if preset in ["Today", "Yesterday"]:
            bucket_end = dt + timedelta(hours=1)
        else:
            bucket_end = dt + timedelta(days=1)

        if marketplace_boolean:
            orders = Order.objects.filter(
                order_date__gte=bucket_start,
                order_date__lt=bucket_end,
                marketplace_id=ObjectId(marketplace_id)
            )
        else:
            orders = Order.objects.filter(
                order_date__gte=bucket_start,
                order_date__lt=bucket_end
            )
        orders_by_bucket[dt.strftime(time_format)] = list(orders)

    # Process each time bucket
    for time_key in graph_data:
        bucket_orders = orders_by_bucket.get(time_key, [])
        gross_revenue = 0
        total_cogs = 0
        refund_amount = 0
        refund_quantity = 0
        total_units = 0
        other_price = 0

        bucket_start = datetime.strptime(time_key, "%Y-%m-%d %H:00:00")
        if preset in ["Today", "Yesterday"]:
            bucket_end = bucket_start.replace(minute=59, second=59)
        else:
            bucket_end = bucket_start.replace(hour=23, minute=59, second=59)
        # Calculate refunds first (same as your total calculation)
        refund_ins = refundOrder(bucket_start, bucket_end,marketplace_id)
        if refund_ins:
            for ins in refund_ins:
                if ins['order_date'] >= bucket_start and ins['order_date'] < bucket_end:
                    refund_amount += ins['order_total']
                    refund_quantity += len(ins['order_items'])

        # Process each order in the bucket
        for order in bucket_orders:
            gross_revenue += order.order_total
            temp_other_price = 0
            tax_price = 0
            
            for item in order.order_items:
                # Get product and COGS (same as your total calculation)
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
                            "price": "$Pricing.ItemPrice.Amount",
                            "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                            "tax_price": "$Pricing.ItemTax.Amount",
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                if result:
                    temp_other_price += result[0]['price']
                    total_cogs += result[0]['cogs']
                    total_units += 1
                    tax_price += result[0]['tax_price']
            
            other_price += order.order_total - temp_other_price - tax_price

        # Calculate net profit and margin
        net_profit = gross_revenue - (other_price + total_cogs)
        profit_margin = round((net_profit / gross_revenue) * 100, 2) if gross_revenue else 0

        # Update graph data for this time bucket
        graph_data[time_key] = {
            "gross_revenue": round(gross_revenue, 2),
            "net_profit": round(net_profit, 2),
            "profit_margin": profit_margin,
            "orders": len(bucket_orders),
            "units_sold": total_units,
            "refund_amount": round(refund_amount, 2),
            "refund_quantity": refund_quantity
        }

    return graph_data


def totalRevenueCalculation(start_date, end_date, marketplace_id=None):
    total = dict()
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    total_units = 0
    total_orders = 0

    result = grossRevenue(start_date, end_date,marketplace_id)
    refund_ins = refundOrder(start_date, end_date,marketplace_id)
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
            other_price = 0
            temp_other_price = 0 
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
                            "price": "$Pricing.ItemPrice.Amount",
                            "cogs": {"$ifNull":["$product_ins.cogs",0.0]},
                            "tax_price": "$Pricing.ItemTax.Amount",
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                tax_price += result[0]['tax_price']
                temp_other_price += result[0]['price']
                total_cogs += result[0]['cogs']
                total_units += 1
        other_price += ins['order_total'] - temp_other_price - tax_price

        net_profit = gross_revenue - (other_price + total_cogs)

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


def RevenueWidgetAPIView(request):
    preset = request.GET.get("preset", "Today")
    compare_startdate = request.GET.get("compare_startdate")
    compare_enddate = request.GET.get("compare_enddate")
    marketplace_id = request.GET.get("marketplace_id", None)

    start_date, end_date = get_date_range(preset)
    comapre_past = get_previous_periods(start_date, end_date)
    total = totalRevenueCalculation(start_date, end_date,marketplace_id)
    graph_data = get_graph_data(start_date, end_date, preset,marketplace_id)
    
    data = dict()
    data = {
        "total": total,
        "graph": graph_data,
        "comapre_past" : comapre_past
    }
    if compare_startdate != None and compare_startdate != "":

        compare_startdate = datetime.strptime(compare_startdate, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
        compare_enddate = datetime.strptime(compare_enddate, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=0)

        compare_total = totalRevenueCalculation(compare_startdate, compare_enddate,marketplace_id)
        initial = "Today" if compare_startdate.date() == compare_enddate.date() else None
        

        
        difference = {
        "gross_revenue": round(((total["gross_revenue"] - compare_total["gross_revenue"]) / compare_total["gross_revenue"] * 100) if compare_total["gross_revenue"] else 0, 2),
        "net_profit": round(((total["net_profit"] - compare_total["net_profit"]) / compare_total["net_profit"] * 100) if compare_total["net_profit"] else 0, 2),
        "profit_margin": round(((total["profit_margin"] - compare_total["profit_margin"]) / compare_total["profit_margin"] * 100) if compare_total["profit_margin"] else 0, 2),
        "orders": round(((total["orders"] - compare_total["orders"]) / compare_total["orders"] * 100) if compare_total["orders"] else 0, 2),
        "units_sold": round(((total["units_sold"] - compare_total["units_sold"]) / compare_total["units_sold"] * 100) if compare_total["units_sold"] else 0, 2),
        "refund_amount": round(((total["refund_amount"] - compare_total["refund_amount"]) / compare_total["refund_amount"] * 100) if compare_total["refund_amount"] else 0, 2),
        "refund_quantity": round(((total["refund_quantity"] - compare_total["refund_quantity"]) / compare_total["refund_quantity"] * 100) if compare_total["refund_quantity"] else 0, 2),
        }
        data['compare_total'] = difference
        data['previous_total'] = compare_total
        data['compare_graph'] = get_graph_data(compare_startdate, compare_enddate, initial,marketplace_id)
    name = "Revenue"
    item_pipeline = [
                        { "$match": { "name": name } }
                    ]
    item_result = list(chooseMatrix.objects.aggregate(*item_pipeline))
    if item_result:
        item_result = item_result[0]
    
        if item_result['select_all']:
            pass
        if item_result['gross_revenue'] == False:
            del data['total']["gross_revenue"]
        if item_result['units_sold'] == False:
            del data['total']["units_sold"]
        # if item_result['acos'] == False:
        #     del data['total']["acos"]
        # if item_result['tacos'] == False:
        #     del data['total']["tacos"]
        if item_result['refund_quantity'] == False:
            del data['total']["refund_quantity"]
        if item_result['refund_amount'] == False:
            del data['total']["refund_amount"]
        if item_result['net_profit'] == False:
            del data['total']["net_profit"]
        if item_result['profit_margin'] == False:
            del data['total']["profit_margin"]
        # if item_result['roas'] == False:
        #     del data['total']["roas"]
        if item_result['orders'] == False:
            del data['total']["orders"]
        # if item_result['ppc_spend'] == False:
        #     del data['total']["ppc_spend"]
    return data



def get_top_products(request):
    marketplace_id = request.GET.get('marketplace_id', None)
    metric = request.GET.get("sortBy", "units_sold")  # 'price', 'refund', etc.
    preset = request.GET.get("preset", "Today")  # today, yesterday, last_7_days
    start_date, end_date = get_date_range(preset)

    # Decide which field to sort by
    sort_field = {
        "units_sold": "total_units",
        "price": "total_price",
        "refund": "refund_qty"
    }.get(metric, "total_units")

    # Decide which field to use for chart values
    chart_value_field = {
        "units_sold": "$order_items_ins.ProductDetails.QuantityOrdered",
        "price": {
            "$multiply": [
                "$order_items_ins.Pricing.ItemPrice.Amount",
                "$order_items_ins.ProductDetails.QuantityOrdered"
            ]
        },
        "refund": "$order_items_ins.ProductDetails.QuantityShipped"
    }.get(metric, "$order_items_ins.ProductDetails.QuantityOrdered")
    match = dict()
    match['order_date'] = {"$gte": start_date, "$lte": end_date}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if metric == "refund":
        match['order_status'] = "Refunded"
    

    pipeline = [
        {
            "$match": match
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
            "$unwind": {
                "path": "$order_items_ins",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$lookup": {
                "from": "product",
                "localField": "order_items_ins.ProductDetails.product_id",
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
            "$addFields": {
                "chart_key": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:00:00",
                        "date": "$order_date"
                    }
                },
                "chart_value": chart_value_field
            }
        },
        {
            "$group": {
                "_id": "$order_items_ins.ProductDetails.product_id",
                "product": {"$first": "$order_items_ins.ProductDetails.Title"},
                "asin": {"$first": "$order_items_ins.ProductDetails.ASIN"},
                "sku": {"$first": "$order_items_ins.ProductDetails.SKU"},
                "product_image": {"$first": "$product_ins.image_url"},
                "total_units": {"$sum": "$order_items_ins.ProductDetails.QuantityOrdered"},
                "total_price": {
                    "$sum": {
                        "$multiply": [
                            "$order_items_ins.Pricing.ItemPrice.Amount",
                            "$order_items_ins.ProductDetails.QuantityOrdered"
                        ]
                    }
                },
                "refund_qty": {"$sum": "$order_items_ins.ProductDetails.QuantityShipped"},
                "chart": {
                    "$push": {
                        "k": "$chart_key",
                        "v": "$chart_value"
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "product": 1,
                "sku": 1,
                "asin": 1,
                "product_image": 1,
                "total_units": 1,
                "total_price": 1,
                "refund_qty": 1,
                "chart": {
                    "$arrayToObject": {
                        "$filter": {
                            "input": "$chart",
                            "as": "item",
                            "cond": {
                                "$and": [
                                    {"$ne": ["$$item.k", None]},
                                    {"$ne": ["$$item.v", None]},
                                    {"$eq": [{"$type": "$$item.k"}, "string"]}
                                ]
                            }
                        }
                    }
                }
            }
        },
        {
            "$sort": SON([(sort_field, -1)])
        },
        {
            "$limit": 10
        }
    ]

    result = list(Order.objects.aggregate(pipeline))
    data = {"results": {"items": result}}
    return data



#########################SELVA WORKING APIS##########

def calculate_metricss(start_date, end_date,marketplace_id):
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    margin = 0
    total_units = 0
    sku_set = set()
    result = grossRevenue(start_date, end_date,marketplace_id)
    order_total = 0
    other_price = 0
    tax_price = 0
    if result:
        for order in result:
            gross_revenue += order['order_total']
            order_total = order['order_total']
            temp_price = 0
            tax_price = 0
            for item_id in order['order_items']:
                item_pipeline = [
                    { "$match": { "_id": item_id } },
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
                            "_id": 0,
                            "price": "$Pricing.ItemPrice.Amount",
                            "tax_price": "$Pricing.ItemTax.Amount",
                            "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                            "sku": "$product_ins.sku"
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item_data = item_result[0]
                    temp_price += item_data['price']
                    tax_price += item_data['tax_price']

                    total_cogs += item_data['cogs']
                    total_units += 1
                    if item_data.get('sku'):
                        sku_set.add(item_data['sku'])
            # other_price += (order_total - temp_price) + total_cogs
        other_price += order_total - temp_price - tax_price
        net_profit = gross_revenue - (other_price + total_cogs)


        
        margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
    return {
        "grossRevenue": round(gross_revenue, 2),
        "expenses": round((other_price + total_cogs) , 2),
        "netProfit": round(net_profit, 2),
        "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price+ total_cogs > 0 else 0,
        "unitsSold": total_units,
        "refunds": refund,  
        "skuCount": len(sku_set),
        "sessions": 0,
        "pageViews": 0,
        "unitSessionPercentage": 0,
        "margin": round(margin, 2)
    }


def getPeriodWiseData(request):
    target_date = datetime.today() - timedelta(days=1)
    marketplace_id = request.GET.get('marketplace_id', None)
    def to_utc_format(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def format_period_metrics(label, current_start, current_end, prev_start, prev_end,marketplace_id):
        current_metrics = calculate_metricss(current_start, current_end,marketplace_id)
        previous_metrics = calculate_metricss(prev_start, prev_end,marketplace_id)

        output = {
            "label": label,
            "period": {
                "current": {
                    "from": to_utc_format(current_start),
                    "to": to_utc_format(current_end)
                },
                "previous": {
                    "from": to_utc_format(prev_start),
                    "to": to_utc_format(prev_end)
                }
            }
        }

        for key in current_metrics:
            output[key] = {
                "current": current_metrics[key],
                "previous": previous_metrics[key],
                "delta": round(current_metrics[key] - previous_metrics[key], 2)
            }

        return output

    # Yesterday
    y_current_start, y_current_end = get_date_range("Yesterday")
    y_previous_start = y_current_start - timedelta(days=1)
    y_previous_end = y_previous_start + timedelta(hours=23, minutes=59, seconds=59)

    # Last 7 Days
    l7_current_start = y_current_start - timedelta(days=6)
    l7_current_end = y_current_end
    l7_previous_start = l7_current_start - timedelta(days=7)
    l7_previous_end = l7_current_start - timedelta(seconds=1)

    # Last 30 Days
    l30_current_start = y_current_start - timedelta(days=29)
    l30_current_end = y_current_end
    l30_previous_start = l30_current_start - timedelta(days=30)
    l30_previous_end = l30_current_start - timedelta(seconds=1)

    ytd_current_start = datetime(y_current_start.year, 1, 1)
    ytd_current_end = y_current_end
    last_year = y_current_start.year - 1
    ytd_previous_start = datetime(last_year, 1, 1)
    ytd_previous_end = datetime(last_year, y_current_start.month, y_current_start.day, 23, 59, 59)
    
    response_data = {
        "yesterday": format_period_metrics("Yesterday", y_current_start, y_current_end, y_previous_start, y_previous_end,marketplace_id),
        "last7Days": format_period_metrics("Last 7 Days", l7_current_start, l7_current_end, l7_previous_start, l7_previous_end,marketplace_id),
        "last30Days": format_period_metrics("Last 30 Days", l30_current_start, l30_current_end, l30_previous_start, l30_previous_end,marketplace_id),
        "yearToDate": format_period_metrics("Year to Date", ytd_current_start, ytd_current_end, ytd_previous_start, ytd_previous_end,marketplace_id),
    }
    return JsonResponse(response_data, safe=False)


def getPeriodWiseDataXl(request):
    marketplace_id = request.GET.get('marketplace_id', None)
    current_date = datetime.today() - timedelta(days=1)
    def calculate_metrics(start_date, end_date,marketplace_id):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        tax_price = 0
        result = grossRevenue(start_date, end_date,marketplace_id)
        order_total = 0
        other_price = 0
        marketplace_name = ""
        if result:
            for order in result:
                marketplace_name = order['marketplace_name']
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                tax_price = 0
                for item_id in order['order_items']:
                    item_pipeline = [
                        { "$match": { "_id": item_id } },
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
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])
                # other_price += (order_total - temp_price) + total_cogs
            other_price += order_total - temp_price - tax_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((other_price + total_cogs), 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price+ total_cogs > 0 else 0,
            "unitsSold": total_units,
            "refunds": refund,  
            "skuCount": len(sku_set),
            "sessions": 0,
            "pageViews": 0,
            "unitSessionPercentage": 0,
            "margin": round(margin, 2),
            "seller":SELLER_ID,
            "marketplace":marketplace_name
        }

    def create_period_row(label, start, end,marketplace_id):
        data = calculate_metrics(start, end,marketplace_id)
        return [
            label,
            data["seller"],
            data["marketplace"],
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            data["grossRevenue"],
            data["expenses"],
            data["netProfit"],
            data["roi"],
            data["unitsSold"],
            data["refunds"],
            data["skuCount"],
            data["sessions"],
            data["pageViews"],
            data["unitSessionPercentage"],
            data["margin"]
        ]

    today, y_current_end = get_date_range("Today")
    yesterday = today - timedelta(days=1)
    last_7_start = today - timedelta(days=7)
    last_30_start = today - timedelta(days=30)
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    period_rows = [
        create_period_row("Yesterday", yesterday, today,marketplace_id),
        create_period_row("Last 7 Days", last_7_start, today - timedelta(seconds=1),marketplace_id),
        create_period_row("Last 30 Days", last_30_start, today - timedelta(seconds=1),marketplace_id),
        create_period_row("Month to Date", month_start, today,marketplace_id),
        create_period_row("Year to Date", year_start, today,marketplace_id),
    ]

    headers = [
        "Period", "Seller", "Marketplace", "Start Date", "End Date",
        "Gross Revenue", "Expenses", "Net Profit", "ROI %",
        "Units Sold", "Refunds", "SKU Count", "Sessions",
        "Page Views", "Unit Session %", "Margin %"
    ]

    # Create Excel workbook
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Period Metrics"
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.value = header
        cell.font = Font(bold=True)
    # ws.append(headers)
    for row in period_rows:
        ws.append(row)

    # Format columns for better readability
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        ws.column_dimensions[column].width = max_length + 2

    # Return Excel as response
    response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=PeriodWiseMetrics.xlsx'
    wb.save(response)
    return response


def exportPeriodWiseCSV(request):
    marketplace_id = request.GET.get('marketplace_id', None)
    current_date = datetime.today() - timedelta(days=1)
    
    def calculate_metrics(start_date, end_date,marketplace_id):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        tax_price = 0
        result = grossRevenue(start_date, end_date,marketplace_id)
        order_total = 0
        other_price = 0
        marketplace_name = ""
        if result:
            for order in result:
                marketplace_name = order['marketplace_name']
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                tax_price = 0
                
                for item_id in order['order_items']:
                    item_pipeline = [
                        { "$match": { "_id": item_id } },
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
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",

                                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])
                # other_price += (order_total - temp_price) + total_cogs
            other_price += order_total - temp_price - tax_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((other_price + total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price+ total_cogs > 0 else 0,
            "unitsSold": total_units,
            "refunds": refund,  
            "skuCount": len(sku_set),
            "sessions": 0,
            "pageViews": 0,
            "unitSessionPercentage": 0,
            "margin": round(margin, 2),
            "seller":SELLER_ID,
            "marketplace":marketplace_name
        }

    def create_period_row(label, start, end,marketplace_id):
        data = calculate_metrics(start, end,marketplace_id)
        return [
            label,
            data["seller"],
            data["marketplace"],
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            str(data["grossRevenue"]),  # Ensuring the value is converted to string
            str(data["expenses"]),     # Ensuring the value is converted to string
            str(data["netProfit"]),    # Ensuring the value is converted to string
            str(data["roi"]),         # Ensuring the value is converted to string
            str(data["unitsSold"]),   # Ensuring the value is converted to string
            str(data["refunds"]),     # Ensuring the value is converted to string
            str(data["skuCount"]),    # Ensuring the value is converted to string
            str(data["sessions"]),    # Ensuring the value is converted to string
            str(data["pageViews"]),   # Ensuring the value is converted to string
            str(data["unitSessionPercentage"]),  # Ensuring the value is converted to string
            str(data["margin"])      # Ensuring the value is converted to string
        ]

    today, y_current_end = get_date_range("Today")

    yesterday = today - timedelta(days=1)
    last_7_start = today - timedelta(days=7)
    last_30_start = today - timedelta(days=30)
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    period_rows = [
        create_period_row("Yesterday", yesterday, yesterday,marketplace_id),
        create_period_row("Last 7 Days", last_7_start, today - timedelta(seconds=1),marketplace_id),
        create_period_row("Last 30 Days", last_30_start, today - timedelta(seconds=1),marketplace_id),
        create_period_row("Month to Date", month_start, today,marketplace_id),
        create_period_row("Year to Date", year_start, today,marketplace_id),
    ]

    headers = [
        "Period", "Seller", "Marketplace", "Start Date", "End Date",
        "Gross Revenue", "Expenses", "Net Profit", "ROI %",
        "Units Sold", "Refunds", "SKU Count", "Sessions",
        "Page Views", "Unit Session %", "Margin %"
    ]

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="PeriodWiseMetrics.csv"'
    writer = csv.writer(response)
    writer.writerow(headers)
    for row in period_rows:
        writer.writerow(row)

    return response


def getPeriodWiseDataCustom(request):
    current_date = datetime.utcnow()
    marketplace_id = request.GET.get('marketplace_id', None)
    
    def calculate_metrics(start_date, end_date,marketplace_id):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
 
        result = grossRevenue(start_date, end_date,marketplace_id)
        order_total = 0
        other_price = 0
        tax_price = 0
        temp_price = 0
        if result:
            for order in result:
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                tax_price = 0
                
                for item_id in order['order_items']:
                    item_pipeline = [
                        { "$match": { "_id": item_id } },
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
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])
                # other_price += (order_total - temp_price) + total_cogs
            other_price += order_total - temp_price - tax_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((other_price + total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price+ total_cogs > 0 else 0,
            "unitsSold": total_units,
            "refunds": refund,  
            "skuCount": len(sku_set),
            "sessions": 0,
            "pageViews": 0,
            "unitSessionPercentage": 0,
            "margin": round(margin, 2),
            "seller":"",
            "tax_price":tax_price,
            "total_cogs":total_cogs,
            "product_cost":order_total,
            "shipping_cost":0,
            'orders':len(result)
        }
 
    def create_period_response(label, cur_from, cur_to, prev_from, prev_to,marketplace_id):
        current = calculate_metrics(cur_from, cur_to,marketplace_id)
        previous = calculate_metrics(prev_from, prev_to,marketplace_id)
        def with_delta(metric):
            return {
                "current": current[metric],
                "previous": previous[metric],
                "delta": round(current[metric] - previous[metric], 2)
            }
        return {
            "dateRanges": {
                "current": {"from": cur_from.isoformat() + "Z","to": (cur_to - timedelta(days=1)).isoformat() + "Z"},
                "previous": {"from": prev_from.isoformat() + "Z", "to": prev_to.isoformat() + "Z"}
            },
            "summary": {
                "grossRevenue": with_delta("grossRevenue"),
                "netProfit": with_delta("netProfit"),
                "expenses": with_delta("expenses"),
                "unitsSold": with_delta("unitsSold"),
                "refunds": with_delta("refunds"),
                "skuCount": with_delta("skuCount"),
                "sessions": with_delta("sessions"),
                "pageViews": with_delta("pageViews"),
                "unitSessionPercentage": with_delta("unitSessionPercentage"),
                "margin": with_delta("margin"),
                "roi": with_delta("roi"),
                "orders": with_delta("orders"),
            },
            "netProfitCalculation": {
                "current": {
                    "gross": current["grossRevenue"],
                    "totalCosts": current["expenses"],
                    "productRefunds": current["refunds"],
                    "totalTax": current["tax_price"] if 'tax_price' in current else 0,
                    "totalTaxWithheld": 0,
                    "ppcProductCost": 0,
                    "ppcBrandsCost": 0,
                    "ppcDisplayCost": 0,
                    "ppcStCost": 0,
                    "cogs": current["total_cogs"] if 'total_cogs' in current else 0,
                    "product_cost": current["product_cost"] ,
                    "shipping_cost": current["shipping_cost"] ,
                },
                "previous": {
                    "gross": previous["grossRevenue"],
                    "totalCosts": previous["expenses"],
                    "productRefunds": previous["refunds"],
                    "totalTax": previous["total_cogs"] if 'total_cogs' in previous else 0,
                    "totalTaxWithheld": 0,
                    "ppcProductCost": 0,
                    "ppcBrandsCost": 0,
                    "ppcDisplayCost": 0,
                    "ppcStCost": 0,
                    "cogs": previous["total_cogs"] if 'total_cogs' in previous else 0,
                    "product_cost": previous["product_cost"] ,
                    "shipping_cost": previous["shipping_cost"] ,
                }
            }
        }
 
    today_start, today_end = get_date_range("Today")

    yesterday_start = today_start - timedelta(days=1)
    yesterday_end = today_start - timedelta(seconds=1)
    last_7_start = today_start - timedelta(days=7)
    
    preset = request.GET.get('preset')
    from_date, to_date = get_date_range(preset)
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration
    today_start, today_end = get_date_range('Today')
    yesterday_start, yesterday_end = get_date_range('Yesterday')
    last_7_start, last_7_end = get_date_range('Last 7 days')
    last_7_prev_start = today_start - timedelta(days=14)
    last_7_prev_end = last_7_start - timedelta(seconds=1)
    print(today_start,today_end)
    print(from_date,to_date)
    response_data = {
        "today": create_period_response("Today", today_start, today_end, yesterday_start, yesterday_end,marketplace_id),
        "yesterday": create_period_response("Yesterday", yesterday_start, yesterday_end, yesterday_start - timedelta(days=1), yesterday_end - timedelta(days=1),marketplace_id),
        "last7Days": create_period_response("Last 7 Days", last_7_start, last_7_end , last_7_prev_start, last_7_prev_end,marketplace_id),
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date,marketplace_id),
    }
 
    return JsonResponse(response_data, safe=False)
 

def allMarketplaceData(request):
    preset = request.GET.get("preset")
    from_date,to_date = get_date_range(preset)
    def grouped_marketplace_metrics(start_date, end_date):
        orders = grossRevenue(start_date, end_date)
        grouped_orders = defaultdict(list)
        
        for order in orders:
            key = (order.get("marketplace_id"), order.get("currency"))
            grouped_orders[key].append(order)

        marketplace_metrics = defaultdict(lambda: {"currency_list": []})

        for (mp_id, currency), orders in grouped_orders.items():
            gross_revenue = 0
            total_cogs = 0
            total_units = 0
            refund = 0
            tax_price = 0
            other_price = 0
            total_product_cost = 0
            sku_set = set()

            m_obj = Marketplace.objects(id=mp_id)
            marketplace = m_obj[0].name if m_obj else ""

            for order in orders:
                gross_revenue += order["order_total"]
                order_total = order["order_total"]
                temp_price = 0
                tax_price = 0

                for item_id in order['order_items']:
                    item_pipeline = [
                        {"$match": {"_id": item_id}},
                        {
                            "$lookup": {
                                "from": "product",
                                "localField": "ProductDetails.product_id",
                                "foreignField": "_id",
                                "as": "product_ins"
                            }
                        },
                        {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                        {
                            "$project": {
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_product_cost += item_data['price']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            other_price += order_total - temp_price - tax_price

            expenses = ((total_cogs + other_price ))
            net_profit = gross_revenue - expenses
            roi = (net_profit / expenses) * 100 if expenses > 0 else 0
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

            currency_data = {
                "currency": currency,
                "grossRevenue": round(gross_revenue, 2),
                "expenses": round(expenses, 2),
                "netProfit": round(net_profit, 2),
                "roi": round(roi, 2),
                "unitsSold": total_units,
                "refunds": refund,
                "skuCount": len(sku_set),
                "margin": round(margin, 2),
                "sessions": 0,
                "pageViews": 0,
                "unitSessionPercentage": 0,
                "seller": "",
                "tax_price": round(tax_price, 2),
                "total_cogs": round(total_cogs, 2),
                "product_cost": round(total_product_cost, 2),
                "shipping_cost": 0
            }

            # Append currency data to the appropriate marketplace
            marketplace_metrics[marketplace]["currency_list"].append(currency_data)

        # Convert the grouped dictionary to a list of marketplace data
        return [{"marketplace": marketplace, "currency_list": data["currency_list"]} 
                for marketplace, data in marketplace_metrics.items()]

    def calculate_metrics(start_date, end_date):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()

        result = grossRevenue(start_date, end_date)
        order_total = 0
        other_price = 0
        tax_price = 0

        if result:
            for order in result:
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                tax_price = 0

                for item_id in order['order_items']:
                    item_pipeline = [
                        {"$match": {"_id": item_id}},
                        {
                            "$lookup": {
                                "from": "product",
                                "localField": "ProductDetails.product_id",
                                "foreignField": "_id",
                                "as": "product_ins"
                            }
                        },
                        {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                        {
                            "$project": {
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            other_price += order_total - temp_price - tax_price

            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((other_price + total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price + total_cogs > 0 else 0,
            "unitsSold": total_units,
            "refunds": refund,
            "skuCount": len(sku_set),
            "sessions": 0,
            "pageViews": 0,
            "unitSessionPercentage": 0,
            "margin": round(margin, 2),
            "seller": "",
            "tax_price": tax_price,
            "total_cogs": total_cogs,
            "product_cost": order_total,
            "shipping_cost": 0
        }

    def create_period_response(label, cur_from, cur_to, prev_from, prev_to):
        current = calculate_metrics(cur_from, cur_to)
        previous = calculate_metrics(prev_from, prev_to)

        def with_delta(metric):
            return {
                "current": current[metric],
                "previous": previous[metric],
                "delta": round(current[metric] - previous[metric], 2)
            }

        return {
            "all_marketplace": {
                "grossRevenue": with_delta("grossRevenue"),
                "netProfit": with_delta("netProfit"),
                "expenses": with_delta("expenses"),
                "unitsSold": with_delta("unitsSold"),
                "refunds": with_delta("refunds"),
                "skuCount": with_delta("skuCount"),
                "sessions": with_delta("sessions"),
                "pageViews": with_delta("pageViews"),
                "unitSessionPercentage": with_delta("unitSessionPercentage"),
                "margin": with_delta("margin"),
                "roi": with_delta("roi")
            },
            "marketplace_list": grouped_marketplace_metrics(cur_from, cur_to)
        }

    current_date = datetime.now()
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration

    response_data = {
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date),
        "from_date":from_date,
        "to_date":to_date
    }

    return JsonResponse(response_data, safe=False)



def getProductPerformanceSummary(request):
    # yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    # today = yesterday + timedelta(days=1)
    marketplace_id = request.GET.get('marketplace_id', None)
    from_date, to_date = get_date_range('Yesterday')
    match=dict()
    match['order_date'] = {"$gte": from_date, "$lte": to_date}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    order_pipeline = [
        {
            "$match": match
        },
        {
            "$project": {
                "_id": 0,
                "order_items": 1,
                "order_total": 1,
                "marketplace_name": 1
            }
        }
    ]
    orders = list(Order.objects.aggregate(*order_pipeline))

    sku_summary = defaultdict(lambda: {
        "sku": "",
        "product_name": "",
        "images": "",
        "unitsSold": 0,
        "grossRevenue": 0.0,
        "totalCogs": 0.0,
        "netProfit": 0.0,
        "margin": 0.0
    })

    for order in orders:
        order_total = order.get("order_total", 0.0)
        item_ids = order.get("order_items", [])
        temp_price = 0.0
        total_cogs = 0.0
        sku_set = set()
        tax_price = 0
        for item_id in item_ids:
            item_pipeline = [
                {"$match": {"_id": item_id}},
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }
                },
                {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "price": "$Pricing.ItemPrice.Amount",
                        "tax_price": "$Pricing.ItemTax.Amount",

                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "sku": "$product_ins.sku",
                        "product_name": "$product_ins.product_title",
                        "images": "$product_ins.image_url"
                    }
                }
            ]
            item_result = list(OrderItems.objects.aggregate(*item_pipeline))
            if item_result:
                item_data = item_result[0]
                sku = item_data.get("sku")
                product_name = item_data.get("product_name", "")
                tax_price += item_data['tax_price']

                images = item_data.get("images", [])
                price = item_data.get("price", 0.0)
                cogs = item_data.get("cogs", 0.0)
                temp_price += price
                total_cogs += cogs
                if sku:
                    sku_set.add(sku)

                    sku_summary[sku]["sku"] = sku
                    sku_summary[sku]["product_name"] = product_name
                    sku_summary[sku]["images"] = images
                    sku_summary[sku]["unitsSold"] += 1
                    sku_summary[sku]["grossRevenue"] += price
                    sku_summary[sku]["totalCogs"] += cogs

        other_price = order_total - temp_price - tax_price

        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            net_profit = gross - (other_price + cogs)
            margin = (net_profit / gross) * 100 if gross > 0 else 0
            sku_summary[sku]["netProfit"] = round(net_profit, 2)
            sku_summary[sku]["margin"] = round(margin, 2)

    sorted_skus = sorted(sku_summary.values(), key=lambda x: x["unitsSold"], reverse=True)
    print(len(sorted_skus))
    top_3 = sorted_skus[:3]
    least_3 = sorted_skus[-3:] if len(sorted_skus) >= 3 else sorted_skus

    return JsonResponse({
        "top_3_products": top_3,
        "least_3_products": least_3
    })


def downloadProductPerformanceSummary(request):
    action = request.GET.get("action", "").lower()
    marketplace_id = request.GET.get('marketplace_id', None)
    from_date, to_date = get_date_range('Yesterday')
    match=dict()
    match['order_date'] = {"$gte": from_date, "$lte": to_date}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
 
    order_pipeline = [
        {
            "$match": match
        },
        {
            "$project": {
                "_id": 0,
                "order_items": 1,
                "order_total": 1,
                "marketplace_id": 1,
                "fulfillment_channel":1
            }
        }
    ]
    orders = list(Order.objects.aggregate(*order_pipeline))
 
    sku_summary = defaultdict(lambda: {
        "sku": "",
        "product_name": "",
        "images": "",
        "unitsSold": 0,
        "grossRevenue": 0.0,
        "totalCogs": 0.0,
        "netProfit": 0.0,
        "margin": 0.0,
        "Trend" :""      
    })
 
    for order in orders:
        order_total = order.get("order_total", 0.0)
        item_ids = order.get("order_items", [])
        marketplace_id = order.get("marketplace_id", "")
        fulfillment_channel = order.get("fulfillment_channel", "")
        temp_price = 0.0
        total_cogs = 0.0
        tax_price = 0
        sku_set = set()
        Marketplace_obj = Marketplace.objects.filter(id = marketplace_id).first()
        m_name = ""
        if Marketplace_obj:
            m_name = Marketplace_obj.name
        for item_id in item_ids:
            item_pipeline = [
                {"$match": {"_id": item_id}},
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }
                },
                {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "price": "$Pricing.ItemPrice.Amount",
                        "tax_price": "$Pricing.ItemPrice.tax_price",
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "sku": "$product_ins.sku",
                        "product_name": "$product_ins.product_title",
                        "images": "$product_ins.image_urls",
                        "asin": "$product_ins.asin"
                    }
                }
            ]
            item_result = list(OrderItems.objects.aggregate(*item_pipeline))
            if item_result:
                item_data = item_result[0]
                sku = item_data.get("sku")
                product_name = item_data.get("product_name", "")
                images = item_data.get("images", [])
                asin = item_data.get("asin", "")
                
                tax_price = item_data.get("tax_price", 0.0)
                price = item_data.get("price", 0.0)
                cogs = item_data.get("cogs", 0.0)
                temp_price += price
                total_cogs += cogs
                if sku:
                    sku_set.add(sku)
 
                    sku_summary[sku]["sku"] = sku
                    sku_summary[sku]["product_name"] = product_name
                    sku_summary[sku]["images"] = images
                    sku_summary[sku]["asin"] = asin
                    sku_summary[sku]["fulfillment_channel"] = fulfillment_channel
                    sku_summary[sku]["m_name"] = m_name
                    sku_summary[sku]["unitsSold"] += 1
                    sku_summary[sku]["grossRevenue"] += price
                    sku_summary[sku]["totalCogs"] += cogs
 
        other_price = order_total - temp_price - tax_price
 
        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            net_profit = gross - (other_price + cogs)
            margin = (net_profit / gross) * 100 if gross > 0 else 0
            sku_summary[sku]["netProfit"] = round(net_profit, 2)
            sku_summary[sku]["margin"] = round(margin, 2)
            if action == "top":
                sku_summary[sku]["Trend"] = "Increasing"
            elif action == "least":
                sku_summary[sku]["Trend"] = "Decreasing"
    # Sort and limit based on action
    sorted_summary = sorted(sku_summary.values(), key=lambda x: x["unitsSold"], reverse=True)
 
    if action == "top":
        final_summary = sorted_summary[:3]
    elif action == "least":
        final_summary = sorted_summary[-3:]
    else:
        final_summary = sorted_summary  # all products
 
    # Create Excel workbook
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Product Performance"
 
    # Headers
    headers = [
         "Product Title","ASIN","SKU","Fulfillment Type","Marketplace" ,"Start Date","End Date","Gross Revenue","Net Profit","Units Sold","Trend"
    ]
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.value = header
        cell.font = Font(bold=True)
 
    # Rows
    for data in final_summary:
        ws.append([
            data["product_name"],
            data["asin"],
            data["sku"],
            data["fulfillment_channel"],
            data["m_name"],
            from_date.date(),
            to_date.date(),
            round(data["grossRevenue"], 2),
            round(data["netProfit"], 2),
            data["unitsSold"],
            data["Trend"],

        ])
 
    # Auto width
    for col_num, col in enumerate(ws.columns, start=1):
        max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        ws.column_dimensions[get_column_letter(col_num)].width = max_length + 2
 
    # Response
    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
    filename = f"Product_Performance_{from_date.strftime('%Y-%m-%d')}_{action or 'all'}.xlsx"
    response['Content-Disposition'] = f'attachment; filename={filename}'
    wb.save(response)
 
    return response
 
 
 
def downloadProductPerformanceCSV(request):
    action = request.GET.get('action', '').lower()
    marketplace_id = request.GET.get('marketplace_id', None)
    from_date, to_date = get_date_range('Yesterday')
    match=dict()
    match['order_date'] = {"$gte": from_date, "$lte": to_date}
    match['order_status'] = {"$in": ['Shipped', 'Delivered']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
    
    order_pipeline = [
        {
            "$match": match
        },
        {
            "$project": {
                "_id": 0,
                "order_items": 1,
                "order_total": 1,
                "marketplace_id": 1,
                "fulfillment_channel":1
            }
        }
    ]
    orders = list(Order.objects.aggregate(*order_pipeline))
 
    sku_summary = defaultdict(lambda: {
        "sku": "",
        "product_name": "",
        "images": "",
        "unitsSold": 0,
        "grossRevenue": 0.0,
        "totalCogs": 0.0,
        "netProfit": 0.0,
        "margin": 0.0,
        "Trend":""
    })
 
    for order in orders:
        order_total = order.get("order_total", 0.0)
        item_ids = order.get("order_items", [])
        fulfillment_channel = order.get("fulfillment_channel", "")
        temp_price = 0.0
        total_cogs = 0.0
        tax_price = 0
        sku_set = set()
        marketplace_id = order.get("marketplace_id", "")
        Marketplace_obj = Marketplace.objects.filter(id = marketplace_id).first()
        m_name = ""
        if Marketplace_obj:
            m_name = Marketplace_obj.name
        for item_id in item_ids:
            item_pipeline = [
                {"$match": {"_id": item_id}},
                {
                    "$lookup": {
                        "from": "product",
                        "localField": "ProductDetails.product_id",
                        "foreignField": "_id",
                        "as": "product_ins"
                    }
                },
                {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                {
                    "$project": {
                        "_id": 0,
                        "price": "$Pricing.ItemPrice.Amount",
                        "tax_price": "$Pricing.ItemPrice.tax_price",
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "sku": "$product_ins.sku",
                        "product_name": "$product_ins.product_title",
                        "images": "$product_ins.image_urls",
                        "asin": "$product_ins.asin"
                    }
                }
            ]
            item_result = list(OrderItems.objects.aggregate(*item_pipeline))
            if item_result:
                item_data = item_result[0]
                sku = item_data.get("sku")
                product_name = item_data.get("product_name", "")
                images = item_data.get("images", [])
                asin =  item_data.get("asin", "")
                
                price = item_data.get("price", 0.0)
                tax_price = item_data.get("tax_price", 0.0)
                cogs = item_data.get("cogs", 0.0)
                temp_price += price
                total_cogs += cogs
                if sku:
                    sku_set.add(sku)
 
                    sku_summary[sku]["sku"] = sku
                    sku_summary[sku]["product_name"] = product_name
                    sku_summary[sku]["images"] = images
                    sku_summary[sku]["m_name"] = m_name
                    sku_summary[sku]["fulfillment_channel"] = fulfillment_channel
 
                    sku_summary[sku]["asin"] = asin
                    sku_summary[sku]["unitsSold"] += 1
                    sku_summary[sku]["grossRevenue"] += price
                    sku_summary[sku]["totalCogs"] += cogs
 
        other_price = order_total - temp_price - tax_price
 
        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            net_profit = gross - (other_price + cogs)
            margin = (net_profit / gross) * 100 if gross > 0 else 0
            sku_summary[sku]["netProfit"] = round(net_profit, 2)
            sku_summary[sku]["margin"] = round(margin, 2)
            if action == "top":
                sku_summary[sku]["Trend"] = "Increasing"
            elif action == "least":
                sku_summary[sku]["Trend"] = "Decreasing"
    # Get action parameter to determine top or least
 
    # Sort and pick top 3 or least 3 based on netProfit
    sorted_summary = sorted(
        sku_summary.values(),
        key=lambda x: x["unitsSold"],
        reverse=(action == "top")
    )
    limited_summary = sorted_summary[:3]
 
    # Create CSV response
    response = HttpResponse(content_type='text/csv')
    filename = f"Product_Performance_{from_date.strftime('%Y-%m-%d')}.csv"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
 
    writer = csv.writer(response)
    # CSV headers
    writer.writerow([
         "Product Title","ASIN","SKU","Fulfillment Type","Marketplace" ,"Start Date","End Date","Gross Revenue","Net Profit","Units Sold","Trend"
    ])
 
    # CSV rows
    for data in limited_summary:
        writer.writerow([
            data["product_name"],
            data["asin"],
            data["sku"],
            data["fulfillment_channel"],
            data["m_name"],
            from_date.date(),
            to_date.date(),
            round(data["grossRevenue"], 2),
            round(data["netProfit"], 2),
            data["unitsSold"],
            data["Trend"],

        ])
 
    return response
 



def allMarketplaceDataxl(request):
    # from_str = request.GET.get("from_date")
    # to_str = request.GET.get("to_date")

    preset = request.GET.get("preset")
    from_date,to_date = get_date_range(preset)

    # try:
    #     from_date = datetime.strptime(from_str, "%Y-%m-%d")
    #     to_date = datetime.strptime(to_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    # except:
    #     to_date = datetime.now()
    #     from_date = to_date - timedelta(days=30)

    def grouped_marketplace_metrics(start_date, end_date):
        orders = grossRevenue(start_date, end_date)
        grouped_orders = defaultdict(list)
        
        for order in orders:
            key = (order.get("marketplace_id"), order.get("currency"))
            grouped_orders[key].append(order)

        marketplace_metrics = defaultdict(lambda: {"currency_list": []})

        for (mp_id, currency), orders in grouped_orders.items():
            gross_revenue = 0
            total_cogs = 0
            total_units = 0
            refund = 0
            tax_price = 0
            other_price = 0
            total_product_cost = 0
            sku_set = set()

            m_obj = Marketplace.objects(id=mp_id)
            marketplace = m_obj[0].name if m_obj else ""

            for order in orders:
                gross_revenue += order["order_total"]
                order_total = order["order_total"]
                temp_price = 0
                tax_price = 0

                for item_id in order['order_items']:
                    item_pipeline = [
                        {"$match": {"_id": item_id}},
                        {
                            "$lookup": {
                                "from": "product",
                                "localField": "ProductDetails.product_id",
                                "foreignField": "_id",
                                "as": "product_ins"
                            }
                        },
                        {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                        {
                            "$project": {
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_product_cost += item_data['price']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            other_price += order_total - temp_price - tax_price

            expenses = total_cogs + other_price
            net_profit = gross_revenue - expenses
            roi = (net_profit / expenses) * 100 if expenses > 0 else 0
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

            currency_data = {
                "Marketplace": marketplace,
                "Currency": currency,
                "Start Date": from_date.date(),
                "End Date": to_date.date(),
                "Gross Revenue": round(gross_revenue, 2),
                "Expenses": round(expenses, 2),
                # "SKU Count": len(sku_set),
                # "Tax Price": round(tax_price, 2),
                "COGS": round(total_cogs, 2),
                "Net Profit": round(net_profit, 2),
                "Margin (%)": round(margin, 2),
                "ROI (%)": round(roi, 2),
                "Refunds": refund,
                "Units Sold": total_units,
                # "Product Cost": round(total_product_cost, 2),
                # "Other Price": round(other_price, 2),
            }

            marketplace_metrics[marketplace]["currency_list"].append(currency_data)

        rows = []
        for _, data in marketplace_metrics.items():
            for row in data["currency_list"]:
                rows.append(row)
        return rows

    # Build the Excel workbook
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Marketplace Metrics"

    # Get data
    data_rows = grouped_marketplace_metrics(from_date, to_date)

    # Write headers
    if data_rows:
        headers = list(data_rows[0].keys())
        sheet.append(headers)
        for col in range(1, len(headers) + 1):
            sheet.cell(row=1, column=col).font = Font(bold=True)

        # Write rows
        for row in data_rows:
            sheet.append(list(row.values()))

        # Auto-adjust column widths
        for col in sheet.columns:
            max_length = max(len(str(cell.value)) if cell.value else 0 for cell in col)
            sheet.column_dimensions[get_column_letter(col[0].column)].width = max_length + 2

    # Prepare response
    buffer = io.BytesIO()
    workbook.save(buffer)
    buffer.seek(0)

    response = HttpResponse(buffer, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    response["Content-Disposition"] = f'attachment; filename="marketplace_metrics_{datetime.now().date()}.xlsx"'
    return response


def downloadMarketplaceDataCSV(request):

    preset = request.GET.get("preset")
    from_date,to_date = get_date_range(preset)
    

    def grouped_marketplace_metrics(start_date, end_date):
        orders = grossRevenue(start_date, end_date)
        grouped_orders = defaultdict(list)

        for order in orders:
            key = (order.get("marketplace_id"), order.get("currency"))
            grouped_orders[key].append(order)

        marketplace_metrics = []

        for (mp_id, currency), orders in grouped_orders.items():
            gross_revenue = 0
            total_cogs = 0
            total_units = 0
            refund = 0
            tax_price = 0
            other_price = 0
            total_product_cost = 0
            sku_set = set()

            m_obj = Marketplace.objects(id=mp_id)
            marketplace = m_obj[0].name if m_obj else ""

            for order in orders:
                gross_revenue += order["order_total"]
                order_total = order["order_total"]
                temp_price = 0
                tax_price = 0

                for item_id in order['order_items']:
                    item_pipeline = [
                        {"$match": {"_id": item_id}},
                        {
                            "$lookup": {
                                "from": "product",
                                "localField": "ProductDetails.product_id",
                                "foreignField": "_id",
                                "as": "product_ins"
                            }
                        },
                        {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                        {
                            "$project": {
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_product_cost += item_data['price']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            other_price += order_total - temp_price - tax_price

            expenses = total_cogs + other_price 
            net_profit = gross_revenue - expenses
            roi = (net_profit / expenses) * 100 if expenses > 0 else 0
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

            marketplace_metrics.append({
                "Marketplace": marketplace,
                "Currency": currency,
                "Start Date": from_date.date(),
                "End Date": to_date.date(),
                "Gross Revenue": round(gross_revenue, 2),
                "Expenses": round(expenses, 2),
                # "SKU Count": len(sku_set),
                # "Tax Price": round(tax_price, 2),
                "COGS": round(total_cogs, 2),
                "Net Profit": round(net_profit, 2),
                "Margin (%)": round(margin, 2),
                "ROI (%)": round(roi, 2),
                "Refunds": refund,
                "Units Sold": total_units,
                # "Product Cost": round(total_product_cost, 2),
                # "Other Price": round(other_price, 2),
            })

        return marketplace_metrics

    # Get data
    metrics = grouped_marketplace_metrics(from_date, to_date)

    # Prepare CSV response
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="marketplace_metrics.csv"'

    writer = csv.DictWriter(response, fieldnames=metrics[0].keys())
    writer.writeheader()
    for row in metrics:
        writer.writerow(row)

    return response



@csrf_exempt
def CityCSVUploadView(request):
    if request.method != 'POST':
        return JsonResponse({"error": "Only POST method allowed"}, status=405)

    file = request.FILES.get('file')
    if not file:
        return JsonResponse({"error": "No file uploaded"}, status=400)

    try:
        try:
            decoded_file = file.read().decode('utf-8')
        except UnicodeDecodeError:
            decoded_file = file.read().decode('latin1')
    except Exception as e:
        return JsonResponse({"error": f"File decoding failed: {str(e)}"}, status=400)

    io_string = io.StringIO(decoded_file)
    reader = csv.DictReader(io_string)
    city_objects = []

    for row in reader:
        try:
            city_obj = CityDetails(
                city=row['city'],
                city_ascii=row['city_ascii'],
                state_id=row['state_id'],
                state_name=row['state_name'],
                county_fips=row['county_fips'],
                county_name=row['county_name'],
                lat=float(row['lat']),
                lng=float(row['lng']),
                population=int(row['population']),
                density=float(row['density']),
                source=row['source'],
                military=row['military'].strip().upper() == 'TRUE',
                incorporated=row['incorporated'].strip().upper() == 'TRUE',
                timezone=row['timezone'],
                ranking=int(row['ranking']),
                zips=row['zips'],
                uid=int(row['id'])
            )
            city_objects.append(city_obj)
        except Exception as e:
            return JsonResponse({"error": f"Error parsing row: {str(e)}"}, status=400)

    try:
        CityDetails.objects.insert(city_objects, load_bulk=False)
        return JsonResponse({"message": "CSV data uploaded successfully."})
    except Exception as e:
        return JsonResponse({"error": f"Bulk insert failed: {str(e)}"}, status=500)


def getCitywiseSales(request):
    level = request.GET.get("type", "city").lower()  
    action = request.GET.get("action", "all").lower()  

    today = datetime.utcnow()
    one_year_ago = today - timedelta(days=30)

    pipeline = [
        {
            "$match": {
                "order_date": {
                    "$gte": one_year_ago,
                    "$lt": today
                },
                "order_status": {"$in": ['Shipped', 'Delivered']}
            }
        },
        {
            "$project": {
                "order_total": 1,
                "marketplace_id": 1,
                "shipping_address": 1,
                "shipping_information": 1
            }
        }
    ]

    results = list(Order.objects.aggregate(*pipeline))
    grouped_data = defaultdict(lambda: {"units": 0, "gross": 0.0, "city": "", "state": "", "country": ""})

    for entry in results:
        address = entry.get("shipping_address") or entry.get("shipping_information", {}).get("postalAddress", {})

        city = address.get("city") or address.get("City")
        state = address.get("state") or address.get("StateOrRegion")
        country =  "USA"

        if level == "city" and city and state and country:
            key = f"{city}|{state}|{country}"
        elif level == "state" and state and country:
            key = f"{state}|{country}"
        elif level == "country" and country:
            key = "USA"
        else:
            continue

        grouped_data[key]["units"] += 1
        grouped_data[key]["gross"] += entry.get("order_total", 0.0)
        grouped_data[key]["city"] = city or ""
        grouped_data[key]["state"] = state or ""
        grouped_data[key]["country"] = "USA"

    geo_lookup = {}
    state_population = defaultdict(int)
    country_population = defaultdict(int)
    if level in ["city", "state", "country"]:
        # all_cities = {data["city"] for data in grouped_data.values() if data["city"]}
        # print(all_cities)
        geo_data = CityDetails.objects.filter()

        for geo in geo_data:

            geo_lookup[geo.city] = geo
            if geo.population:

                if level in ["state"]:
                    key = f"{geo.state_id}|USA"
                    state_population[key] += geo.population
                if level == "country":
                    country_population['USA'] += geo.population
    items = []
    for key, data_ in grouped_data.items():
        city = data_["city"]
        geo = geo_lookup.get(city) if level == "city" else None

        item = {
            "units": data_["units"],
            "gross": round(data_["gross"], 2),
            "country": data_["country"],
            "state_name": ""

        }

        if level in ["city", "state"]:
            item["state"] = data_["state"]

        if level == "city":
            item["city"] = city
            if geo:
                item["lat"] = geo.lat
                item["lon"] = geo.lng
                item["code"] = ""
                item["fips"] = ""
                item["population"] = geo.population
                item["state_name"] = geo.state_name
            else:
                item["lat"] = None
                item["lon"] = None
                item["code"] = ""
                item["fips"] = ""
                item["population"] = 1
                item["state_name"] = ""


        elif level == "state":
            geo_data = CityDetails.objects.filter(state_id=data_["state"]).first()
            

            item["lat"] = None
            item["lon"] = None
            item["code"] = ""
            item["fips"] = ""
            if geo_data:
                item["state_name"] = geo_data.state_name

            state_key = f"{data_['state']}|USA"
            item["population"] = state_population.get(state_key, 1)

        elif level == "country":
            item["lat"] = None
            item["lon"] = None
            item["code"] = ""
            item["fips"] = ""
            item["population"] = country_population.get('USA', 1)
        if action != "all":
            print(item["population"])
            item['units'] = item['units'] / item["population"] if item["population"] >0 else 1
            item['gross'] = item['gross'] / item["population"] if item["population"] >0 else 1
        items.append(item)

    return JsonResponse({"items": items}, safe=False)

def exportCitywiseSalesExcel(request):
    today = datetime.utcnow()
    one_year_ago = today - timedelta(days=365)
    action = request.GET.get("action", "all").lower()  

    level = request.GET.get("type", "city").lower()  

    pipeline = [
        {
            "$match": {
                "order_date": {
                    "$gte": one_year_ago,
                    "$lt": today
                }
            }
        },
        {
            "$project": {
                "order_total": 1,
                "marketplace_id": 1,
                "shipping_address": 1,
                "shipping_information": 1
            }
        }
    ]
    results = list(Order.objects.aggregate(pipeline))

    grouped_data = defaultdict(lambda: {"units": 0, "gross": 0.0, "city": "", "state": "", "country": ""})

    for entry in results:
        shipping = entry.get("shipping_address") or entry.get("shipping_information", {}).get("postalAddress", {})
        city = shipping.get("city")
        state = shipping.get("state") or shipping.get("StateOrRegion")
        country = shipping.get("country") or shipping.get("CountryCode")

        # Determine grouping key
        if level == "city" and city and state and country:
            key = f"{city}|{state}|{country}"
        elif level == "state" and state and country:
            key = f"{state}|{country}"
        elif level == "country" and country:
            key = f"{country}"
        else:
            continue  

        grouped_data[key]["units"] += 1
        grouped_data[key]["gross"] += entry.get("order_total", 0.0)
        grouped_data[key]["city"] = city or ""
        grouped_data[key]["state"] = state or ""
        grouped_data[key]["country"] = country or ""
    geo_lookup = {}
    city_population = defaultdict(int)
    state_population = defaultdict(int)
    country_population = defaultdict(int)
    if level in ["city", "state", "country"]:
        # all_cities = {data["city"] for data in grouped_data.values() if data["city"]}
        # print(all_cities)
        geo_data = CityDetails.objects.filter()

        for geo in geo_data:

            geo_lookup[geo.city] = geo
            if geo.population:
                if level in ["city"]:
                    key = f"{city}|{state}|{country}"
                    city_population[key] += geo.population
                if level in ["state"]:
                    key = f"{geo.state_id}|USA"
                    state_population[key] += geo.population
                if level == "country":
                    country_population['USA'] += geo.population
    data_rows = []
    for key, values in grouped_data.items():
        row = [one_year_ago.strftime("%b %d, %Y"), today.strftime("%b %d, %Y")]

        if level == "city":
            row.extend([values["country"], values["state"], values["city"]])
            headers = ["Date From", "Date To", "Country", "State", "City", "Gross Revenue", "Units Sold"]
        elif level == "state":
            row.extend([values["country"], values["state"]])
            headers = ["Date From", "Date To", "Country", "State", "Gross Revenue", "Units Sold"]
        else:  # country
            row.append(values["country"])
            headers = ["Date From", "Date To", "Country", "Gross Revenue", "Units Sold"]
        if action == 'all':
            row.extend([round(values["gross"], 2), values["units"]])
        else:
            if level == "city":
                row.extend([(values["gross"]/city_population.get(values["city"], 1)), values["units"]])
            elif level == "state":
                row.extend([(values["gross"]/state_population.get(values["state"], 1)), values["units"]])
            else:
                row.extend([(values["gross"]/country_population.get("USA", 1)), values["units"]])
        data_rows.append(row)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sales Data"
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.value = header
        cell.font = Font(bold=True)
    # ws.append(headers)
    for row in data_rows:
        ws.append(row)

    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass
        ws.column_dimensions[column].width = max_length + 2

    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{level.capitalize()}wiseSales.xlsx"'
    wb.save(response)
    return response


def downloadCitywiseSalesCSV(request):
    today = datetime.utcnow()
    one_year_ago = today - timedelta(days=365)
    action = request.GET.get("action", "all").lower()  # 'all' or 'percapita'
    level = request.GET.get("type", "city").lower()  # 'city', 'state', 'country'

    pipeline = [
        {
            "$match": {
                "order_date": {
                    "$gte": one_year_ago,
                    "$lt": today
                }
            }
        },
        {
            "$project": {
                "order_total": 1,
                "marketplace_id": 1,
                "shipping_address": 1,
                "shipping_information": 1
            }
        }
    ]
    results = list(Order.objects.aggregate(pipeline))
    grouped_data = defaultdict(lambda: {"units": 0, "gross": 0.0, "city": "", "state": "", "country": ""})

    for entry in results:
        shipping = entry.get("shipping_address") or entry.get("shipping_information", {}).get("postalAddress", {})
        city = shipping.get("city")
        state = shipping.get("state") or shipping.get("StateOrRegion")
        country = shipping.get("country") or shipping.get("CountryCode")

        if level == "city" and city and state and country:
            key = f"{city}|{state}|{country}"
        elif level == "state" and state and country:
            key = f"{state}|{country}"
        elif level == "country" and country:
            key = f"{country}"
        else:
            continue

        grouped_data[key]["units"] += 1
        grouped_data[key]["gross"] += entry.get("order_total", 0.0)
        grouped_data[key]["city"] = city or ""
        grouped_data[key]["state"] = state or ""
        grouped_data[key]["country"] = country or ""

    # Geo population data
    geo_lookup = {}
    city_population = {}
    state_population = defaultdict(int)  # ✅ use defaultdict here
    country_population = defaultdict(int)

    geo_data = CityDetails.objects.all()
    for geo in geo_data:
        geo_lookup[geo.city] = geo
        if geo.population:
            city_key = f"{geo.city}|{geo.state_id}|USA"
            state_key = f"{geo.state_id}|USA"
            city_population[city_key] = geo.population
            state_population[state_key] += geo.population
            country_population["USA"] += geo.population

    data_rows = []
    for key, values in grouped_data.items():
        row = [one_year_ago.strftime("%b %d, %Y"), today.strftime("%b %d, %Y")]

        if level == "city":
            row.extend([values["country"], values["state"], values["city"]])
            headers = ["Date From", "Date To", "Country", "State", "City", "Gross Revenue", "Units Sold"]
            pop_key = f"{values['city']}|{values['state']}|{values['country']}"
            population = city_population.get(pop_key, 1)
        elif level == "state":
            row.extend([values["country"], values["state"]])
            headers = ["Date From", "Date To", "Country", "State", "Gross Revenue", "Units Sold"]
            pop_key = f"{values['state']}|{values['country']}"
            population = state_population.get(pop_key, 1)
        else:  # country
            row.append(values["country"])
            headers = ["Date From", "Date To", "Country", "Gross Revenue", "Units Sold"]
            population = country_population.get(values['country'], 1)

        if action == 'all':
            row.extend([round(values["gross"], 2), values["units"]])
        else:
            per_capita = round(values["gross"] / population, 4)
            u_p = round(values["units"]/ population,4)
            row.extend([per_capita, u_p])
            headers = headers[:len(headers) - 2] + ["Per Capita Revenue", "Units Sold"]

        data_rows.append(row)

    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="{level.capitalize()}wiseSales.csv"'

    writer = csv.writer(response)
    writer.writerow(headers)
    for row in data_rows:
        writer.writerow(row)

    return response



def generate_monthly_intervals(from_date, to_date):
    intervals = []
    current_date = from_date.replace(day=1)

    while current_date <= to_date:
        intervals.append(current_date.strftime('%Y-%m-%d 00:00:00'))
        
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    print(intervals)
    return intervals

# Function to calculate the profit/loss metrics
def calculate_metrics(start_date, end_date):
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    margin = 0
    total_units = 0
    sku_set = set()
    product_categories = {}
    product_completeness = {"complete": 0, "incomplete": 0}

    result = grossRevenue(start_date, end_date)
    order_total = 0
    other_price = 0
    tax_price = 0
    temp_price = 0
    if result:
        for order in result:
            gross_revenue += order['order_total']
            order_total = order['order_total']
            temp_price = 0
            tax_price = 0

            for item_id in order['order_items']:
                item_pipeline = [
                    { "$match": { "_id": item_id } },
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
                            "_id": 0,
                            "price": "$Pricing.ItemPrice.Amount",
                            "tax_price": "$Pricing.ItemTax.Amount",
                            "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                            "sku": "$product_ins.sku",
                            "category": "$product_ins.category"
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item_data = item_result[0]
                    temp_price += item_data['price']
                    tax_price += item_data['tax_price']
                    total_cogs += item_data['cogs']
                    total_units += 1
                    if item_data.get('sku'):
                        sku_set.add(item_data['sku'])

                    # Track product category distribution
                    category = item_data.get('category', 'Unknown')
                    if category in product_categories:
                        product_categories[category] += 1
                    else:
                        product_categories[category] = 1

                    # Track product completeness
                    if item_data['price'] and item_data['cogs'] and item_data['sku']:
                        product_completeness["complete"] += 1
                    else:
                        product_completeness["incomplete"] += 1

        other_price += order_total - temp_price - tax_price
        net_profit = gross_revenue - (other_price + total_cogs)
        margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

    return {
        "grossRevenue": round(gross_revenue, 2),
        "expenses": round((other_price + total_cogs), 2),
        "netProfit": round(net_profit, 2),
        "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price + total_cogs > 0 else 0,
        "unitsSold": total_units,
        "refunds": refund,  
        "skuCount": len(sku_set),
        "sessions": 0,
        "pageViews": 0,
        "unitSessionPercentage": 0,
        "margin": round(margin, 2),
        "seller": "",
        "tax_price": tax_price,
        "total_cogs": total_cogs,
        "product_cost": order_total,
        "shipping_cost": 0,
        "productCategories": product_categories,  # Product distribution
        "productCompleteness": product_completeness  # Product completeness
    }


def get_products_with_pagination(request):
    page = int(request.GET.get("page", 1))
    page_size = 10

    # Define the pipeline for pagination and data fetching
    pipeline = [
        {
            "$facet": {
                "total_count": [{"$count": "count"}],
                "products": [
                    {"$skip": (page - 1) * page_size},
                    {"$limit": page_size},
                    {
                        "$project": {
                            "_id": 0,
                            "id": {"$toString":"$_id"},
                            "AmazonToken_id": {"$ifNull": ["$AmazonToken_id", "N/A"]},
                            "asin": {"$ifNull": ["$product_id", "N/A"]},
                            "sellerSku": {"$ifNull": ["$sku", "N/A"]},
                            "marketplace": {"$ifNull": ["$marketplace", "N/A"]},
                            "inventoryStatus": {"$ifNull": ["$inventoryStatus", "N/A"]},
                            "fulfillmentChannel": {"$ifNull": ["$fulfillmentChannel", "N/A"]},
                            "price": {"$ifNull": ["$price", "N/A"]},
                            "priceDraft": {"$ifNull": ["$priceDraft", "N/A"]},
                            "title": {"$ifNull": ["$product_title", "N/A"]},
                            "totalRatingsCount": {"$ifNull": ["$totalRatingsCount", "N/A"]},
                            "reviewRating": {"$ifNull": ["$reviewRating", "N/A"]},
                            "listingScore": {"$ifNull": ["$listingScore", "N/A"]},
                            "imageUrl": {"$ifNull": ["$image_url", "N/A"]},
                            "parentAsin": {"$ifNull": ["$parentAsin", "N/A"]},
                            "buyBoxWinnerId": {"$ifNull": ["$buyBoxWinnerId", "N/A"]},
                            "newInsightsCount": {"$ifNull": ["$newInsightsCount", "N/A"]},
                            "newInsightsGrouped": {"$ifNull": ["$newInsightsGrouped", "N/A"]},
                            "category": {"$ifNull": ["$category", "N/A"]},
                            "categoryTitle": {"$ifNull": ["$categoryTitle", "N/A"]},
                            "amazonLink": {"$ifNull": ["$amazonLink", "N/A"]},
                            "bsr": {"$ifNull": ["$bsr", "N/A"]},
                            "subcategoriesBsr": {"$ifNull": ["$subcategoriesBsr", "N/A"]},
                            "salesForToday": {"$ifNull": ["$salesForToday", 0]},
                            "unitsSoldForToday": {"$ifNull": ["$unitsSoldForToday", 0]},
                            "unitsSoldForPeriod": {"$ifNull": ["$unitsSoldForPeriod", 0]},
                            "refunds": {"$ifNull": ["$refunds", 0]},
                            "refundsAmount": {"$ifNull": ["$refundsAmount", 0]},
                            "refundRate": {"$ifNull": ["$refundRate", "0%"]},
                            "pageViews": {"$ifNull": ["$pageViews", 0]},
                            "pageViewsPercentage": {"$ifNull": ["$pageViewsPercentage", "0%"]},
                            "conversionRate": {"$ifNull": ["$conversionRate", "N/A"]},
                            "grossProfit": {"$ifNull": ["$grossProfit", 0]},
                            "netProfit": {"$ifNull": ["$netProfit", 0]},
                            "margin": {"$ifNull": ["$margin", "0%"]},
                            "totalAmazonFees": {"$ifNull": ["$totalAmazonFees", "N/A"]},
                            "roi": {"$ifNull": ["$roi", "0%"]},
                            "cogs": {"$ifNull": ["$cogs", 0]},
                            "fbaPerOrderFulfillmentFee": {"$ifNull": ["$fbaPerOrderFulfillmentFee", "N/A"]},
                            "fbaPerUnitFulfillmentFee": {"$ifNull": ["$fbaPerUnitFulfillmentFee", "N/A"]},
                            "fbaWeightBasedFee": {"$ifNull": ["$fbaWeightBasedFee", "N/A"]},
                            "variableClosingFee": {"$ifNull": ["$variableClosingFee", "N/A"]},
                            "commission": {"$ifNull": ["$commission", "N/A"]},
                            "fixedClosingFee": {"$ifNull": ["$fixedClosingFee", "N/A"]},
                            "salesTaxCollectionFee": {"$ifNull": ["$salesTaxCollectionFee", "N/A"]},
                            "shippingHbFee": {"$ifNull": ["$shippingHbFee", "N/A"]},
                            "isFavorite": {"$ifNull": ["$isFavorite", "N/A"]},
                            "trafficSessions": {"$ifNull": ["$trafficSessions", "N/A"]},
                            "trafficSessionPercentage": {"$ifNull": ["$trafficSessionPercentage", "0%"]},
                            # "deltas": {"$ifNull": ["$deltas", None]},
                            "competitorsProducts": {"$ifNull": ["$competitorsProducts", 0]},
                            "tags": {"$ifNull": ["$tags", []]},
                        }
                    }
                ]
            }
        }
    ]

    # Execute the pipeline
    result = list(Product.objects.aggregate(*pipeline))

    # Extract total count and products
    total_products = result[0]["total_count"][0]["count"] if result[0]["total_count"] else 0
    products = result[0]["products"]

    # Prepare response data
    response_data = {
        "total_products": total_products,
        "page": page,
        "page_size": page_size,
        "products": products,
    }

    return JsonResponse(response_data, safe=False)



def getProfitAndLossDetails(request):
    current_date = datetime.utcnow()
    
    def grossRevenue(start_date, end_date):
        pipeline = [
            {
                "$match": {
                    "order_date": {"$gte": start_date, "$lte": end_date},
                    "order_status": {"$in": ['Shipped', 'Delivered']}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "order_items": 1,
                    "order_total": 1
                }
            }
        ]
        return list(Order.objects.aggregate(*pipeline))
    
    def calculate_metrics(start_date, end_date):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        product_categories = {}
        product_completeness = {"complete": 0, "incomplete": 0}

        result = grossRevenue(start_date, end_date)
        order_total = 0
        other_price = 0
        tax_price = 0
        temp_price = 0
        if result:
            for order in result:
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                tax_price = 0
                
                for item_id in order['order_items']:
                    item_pipeline = [
                        { "$match": { "_id": item_id } },
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
                                "_id": 0,
                                "price": "$Pricing.ItemPrice.Amount",
                                "tax_price": "$Pricing.ItemTax.Amount",
                                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                                "sku": "$product_ins.sku",
                                "category": "$product_ins.category"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])
                        
                        # Track product category distribution
                        category = item_data.get('category', 'Unknown')
                        if category in product_categories:
                            product_categories[category] += 1
                        else:
                            product_categories[category] = 1

                        # Track product completeness
                        if item_data['price'] and item_data['cogs'] and item_data['sku']:
                            product_completeness["complete"] += 1
                        else:
                            product_completeness["incomplete"] += 1

            other_price += order_total - temp_price - tax_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((other_price + total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price+ total_cogs > 0 else 0,
            "unitsSold": total_units,
            "refunds": refund,  
            "skuCount": len(sku_set),
            "sessions": 0,
            "pageViews": 0,
            "unitSessionPercentage": 0,
            "margin": round(margin, 2),
            "seller":"",
            "tax_price":tax_price,
            "total_cogs":total_cogs,
            "product_cost":order_total,
            "shipping_cost":0,
            "productCategories": product_categories,  # Added product distribution data
            "productCompleteness": product_completeness  # Added product completeness data
        }

    def create_period_response(label, cur_from, cur_to, prev_from, prev_to):
        current = calculate_metrics(cur_from, cur_to)
        previous = calculate_metrics(prev_from, prev_to)

        def with_delta(metric):
            return {
                "current": current[metric],
                "previous": previous[metric],
                "delta": round(current[metric] - previous[metric], 2)
            }

        return {
            "dateRanges": {
                "current": {"from": cur_from.isoformat() + "Z", "to": cur_to.isoformat() + "Z"},
                "previous": {"from": prev_from.isoformat() + "Z", "to": prev_to.isoformat() + "Z"}
            },
            "summary": {
                "grossRevenue": with_delta("grossRevenue"),
                "netProfit": with_delta("netProfit"),
                "expenses": with_delta("expenses"),
                "unitsSold": with_delta("unitsSold"),
                "refunds": with_delta("refunds"),
                "skuCount": with_delta("skuCount"),
                "sessions": with_delta("sessions"),
                "pageViews": with_delta("pageViews"),
                "unitSessionPercentage": with_delta("unitSessionPercentage"),
                "margin": with_delta("margin"),
                "roi": with_delta("roi")
            },
            "netProfitCalculation": {
                "current": {
                    "gross": current["grossRevenue"],
                    "totalCosts": current["expenses"],
                    "productRefunds": current["refunds"],
                    "totalTax": current["tax_price"] if 'tax_price' in current else 0,
                    "totalTaxWithheld": 0,
                    "ppcProductCost": 0,
                    "ppcBrandsCost": 0,
                    "ppcDisplayCost": 0,
                    "ppcStCost": 0,
                    "cogs": current["total_cogs"] if 'total_cogs' in current else 0,
                    "product_cost": current["product_cost"],
                    "shipping_cost": current["shipping_cost"],
                },
                "previous": {
                    "gross": previous["grossRevenue"],
                    "totalCosts": previous["expenses"],
                    "productRefunds": previous["refunds"],
                    "totalTax": previous["total_cogs"] if 'total_cogs' in previous else 0,
                    "totalTaxWithheld": 0,
                    "ppcProductCost": 0,
                    "ppcBrandsCost": 0,
                    "ppcDisplayCost": 0,
                    "ppcStCost": 0,
                    "cogs": previous["total_cogs"] if 'total_cogs' in previous else 0,
                    "product_cost": previous["product_cost"],
                    "shipping_cost": previous["shipping_cost"],
                }
            },
            "charts": {
                "productDistribution": current["productCategories"],  # Bar chart data
                "productCompleteness": current["productCompleteness"]  # Pie chart data
            }
        }

    # current_date = datetime.now()
    # today_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    # now = current_date

    preset = request.GET.get('preset')
    from_date, to_date = get_date_range(preset)
    
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration

    response_data = {
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date),
    }

    return JsonResponse(response_data, safe=False)



def profit_loss_chart(request):
    def get_month_range(year, month):
        start_date = datetime(year, month, 1)
        last_day = monthrange(year, month)[1]
        end_date = datetime(year, month, last_day, 23, 59, 59)
        return start_date, end_date

    def gross_revenue(start_date, end_date):
        pipeline = [
            {
                "$match": {
                    "order_date": {"$gte": start_date, "$lte": end_date},
                    "order_status": {"$in": ['Shipped', 'Delivered']}
                }
            },
            {
                "$project": {
                    "order_items": 1,
                    "order_total": 1
                }
            }
        ]
        return list(Order.objects.aggregate(*pipeline))

    def calculate_metrics(start_date, end_date):
        gross_revenue_amt = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        product_categories = {}
        product_completeness = {"complete": 0, "incomplete": 0}
        order_total = 0
        other_price = 0
        tax_price = 0
        temp_price = 0

        result = gross_revenue(start_date, end_date)
        for order in result:
            gross_revenue_amt += order.get("order_total", 0)
            order_total = order.get("order_total", 0)
            temp_price = 0
            tax_price = 0

            for item_id in order.get("order_items", []):
                item_pipeline = [
                    {"$match": {"_id": item_id}},
                    {
                        "$lookup": {
                            "from": "product",
                            "localField": "ProductDetails.product_id",
                            "foreignField": "_id",
                            "as": "product_ins"
                        }
                    },
                    {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                    {
                        "$project": {
                            "price": "$Pricing.ItemPrice.Amount",
                            "tax_price": "$Pricing.ItemTax.Amount",
                            "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                            "sku": "$product_ins.sku",
                            "category": "$product_ins.category"
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item = item_result[0]
                    temp_price += item.get("price", 0)
                    tax_price += item.get("tax_price", 0)
                    total_cogs += item.get("cogs", 0)
                    total_units += 1
                    sku = item.get("sku")
                    if sku:
                        sku_set.add(sku)
                    category = item.get("category", "Unknown")
                    product_categories[category] = product_categories.get(category, 0) + 1
                    if item.get("price") and item.get("cogs") and sku:
                        product_completeness["complete"] += 1
                    else:
                        product_completeness["incomplete"] += 1

            other_price += order_total - temp_price - tax_price

        net_profit = gross_revenue_amt - (other_price + total_cogs)
        margin = (net_profit / gross_revenue_amt * 100) if gross_revenue_amt else 0

        return {
            "grossRevenue": round(gross_revenue_amt, 2),
            "expenses": round((other_price + total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if (other_price + total_cogs) else 0,
            "unitsSold": total_units,
            "refunds": refund,
            "skuCount": len(sku_set),
            "margin": round(margin, 2),
            "tax_price": tax_price,
            "total_cogs": total_cogs,
            "product_cost": order_total,
            "productCategories": product_categories,
            "productCompleteness": product_completeness
        }

    def generate_month_keys(start_year, start_month, end_year, end_month):
        months = []
        current = datetime(start_year, start_month, 1)
        end = datetime(end_year, end_month, 1)
        while current <= end:
            months.append(current.strftime("%Y-%m-%d 00:00:00"))
            current += timedelta(days=32)
            current = current.replace(day=1)
        return months

    # Init
    metrics = ["grossRevenue", "estimatedPayout", "expenses", "netProfit", "units", "ppcSales"]
    values = {metric: {} for metric in metrics}
    preset = request.GET.get('preset')
    from_date, to_date = get_date_range(preset)

    # Preset types
    hourly_presets = ["Today", "Yesterday"]
    daily_presets = ["This Week", "Last Week", "Last 7 days", "Last 14 days", "Last 30 days", "Last 60 days", "Last 90 days"]

    # Key generation
    if preset in hourly_presets:
        interval_keys = [(from_date + timedelta(hours=i)).strftime("%Y-%m-%d %H:00:00") 
                         for i in range(0, int((to_date - from_date).total_seconds() // 3600) + 1)]
        interval_type = "hour"
    elif preset in daily_presets:
        interval_keys = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d 00:00:00") 
                         for i in range((to_date - from_date).days + 1)]
        interval_type = "day"
    else:
        interval_keys = generate_month_keys(
            from_date.year, from_date.month,
            to_date.year, to_date.month
        )
        interval_type = "month"

    # Main loop
    for key in interval_keys:
        if interval_type == "hour":
            start = datetime.strptime(key, "%Y-%m-%d %H:00:00")
            end = start + timedelta(hours=1) - timedelta(seconds=1)
        elif interval_type == "day":
            start = datetime.strptime(key, "%Y-%m-%d 00:00:00")
            end = start + timedelta(days=1) - timedelta(seconds=1)
        else:
            year, month = int(key[:4]), int(key[5:7])
            start, end = get_month_range(year, month)

        data = calculate_metrics(start, end)

        values["grossRevenue"][key] = data["grossRevenue"]
        values["expenses"][key] = data["expenses"]
        values["netProfit"][key] = data["netProfit"]
        values["units"][key] = data["unitsSold"]

    # Fill default 0s
    for metric in metrics:
        for key in interval_keys:
            values[metric].setdefault(key, 0)

    # Final response
    graph = [{"metric": metric, "values": values[metric]} for metric in metrics]
    return JsonResponse({"graph": graph}, safe=False)



def profitLossExportXl(request):
    def get_month_range(year, month):
        start_date = datetime(year, month, 1)
        last_day = monthrange(year, month)[1]
        end_date = datetime(year, month, last_day, 23, 59, 59)
        return start_date, end_date

    def gross_revenue(start_date, end_date):
        pipeline = [
            {
                "$match": {
                    "order_date": {"$gte": start_date, "$lte": end_date},
                    "order_status": {"$in": ['Shipped', 'Delivered']}
                }
            },
            {
                "$project": {
                    "order_items": 1,
                    "order_total": 1,
                    "marketplace_id": 1,

                }
            }
        ]
        return list(Order.objects.aggregate(*pipeline))

    def calculate_metrics(start_date, end_date):
        gross_revenue_amt = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        order_total = 0
        other_price = 0
        tax_price = 0
        temp_price = 0
        m_name = ""
        result = gross_revenue(start_date, end_date)
        for order in result:
            gross_revenue_amt += order.get("order_total", 0)
            order_total = order.get("order_total", 0)
            temp_price = 0
            tax_price = 0
            marketplace_id = order.get("marketplace_id", "")
            Marketplace_obj = Marketplace.objects.filter(id = marketplace_id).first()
            m_name = ""
            if Marketplace_obj:
                m_name = Marketplace_obj.name
            for item_id in order.get("order_items", []):
                item_pipeline = [
                    {"$match": {"_id": item_id}},
                    {
                        "$lookup": {
                            "from": "product",
                            "localField": "ProductDetails.product_id",
                            "foreignField": "_id",
                            "as": "product_ins"
                        }
                    },
                    {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                    {
                        "$project": {
                            "price": "$Pricing.ItemPrice.Amount",
                            "tax_price": "$Pricing.ItemTax.Amount",
                            "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                            "sku": "$product_ins.sku"
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item = item_result[0]
                    temp_price += item.get("price", 0)
                    tax_price += item.get("tax_price", 0)
                    total_cogs += item.get("cogs", 0)
                    total_units += 1
                    sku = item.get("sku")
                    if sku:
                        sku_set.add(sku)

            other_price += order_total - temp_price - tax_price

        net_profit = gross_revenue_amt - (other_price + total_cogs)
        margin = (net_profit / gross_revenue_amt * 100) if gross_revenue_amt else 0

        return {
            "Marketplace":m_name,
            "Date and Time":start_date,
            "Gross Revenue": round(gross_revenue_amt, 2),
            "Expenses": round((other_price + total_cogs) , 2),
            "Estimated Payout":0,
            "Net Profit": round(net_profit, 2),
            # "ROI (%)": round((net_profit / (other_price + total_cogs)) * 100, 2) if (other_price + total_cogs) else 0,
            "Units Sold": total_units,
            # "SKU Count": len(sku_set),
            # "Tax Collected": round(tax_price, 2),
            # "Total COGS": round(total_cogs, 2),
            # "Profit Margin (%)": round(margin, 2)
            "PPC Sales": 0 
        }

    def generate_month_keys(start_year, start_month, end_year, end_month):
        months = []
        current = datetime(start_year, start_month, 1)
        end = datetime(end_year, end_month, 1)
        while current <= end:
            months.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=32)
            current = current.replace(day=1)
        return months

    # Date setup
    preset = request.GET.get('preset', 'Last 30 days')
    from_date, to_date = get_date_range(preset)

    # Determine interval
    interval_keys = []
    interval_type = ""

    if preset in ["Today", "Yesterday"]:
        interval_keys = [(from_date + timedelta(hours=i)).strftime("%Y-%m-%d %H:00:00")
                         for i in range(int((to_date - from_date).total_seconds() // 3600) + 1)]
        interval_type = "hour"
    elif preset in ["This Week", "Last Week", "Last 7 days", "Last 14 days", "Last 30 days", "Last 60 days", "Last 90 days"]:
        interval_keys = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d")
                         for i in range((to_date - from_date).days + 1)]
        interval_type = "day"
    else:
        interval_keys = generate_month_keys(from_date.year, from_date.month, to_date.year, to_date.month)
        interval_type = "month"

    # Create Excel workbook
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Profit Loss Report"

    headers = ["Marketplace", "Date and Time", "Gross Revenue", "Expenses", "Estimated Payout", "Net Profit", "Units", "PPC Sales"]
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num)
        cell.value = header
        cell.font = Font(bold=True)
    # ws.append(headers)

    for key in interval_keys:
        if interval_type == "hour":
            start = datetime.strptime(key, "%Y-%m-%d %H:00:00")
            end = start + timedelta(hours=1) - timedelta(seconds=1)
            time_label = start.strftime("%Y-%m-%d %H:00:00")
        elif interval_type == "day":
            start = datetime.strptime(key, "%Y-%m-%d")
            end = start + timedelta(days=1) - timedelta(seconds=1)
            time_label = start.strftime("%Y-%m-%d")
        else:
            year, month = int(key[:4]), int(key[5:7])
            start, end = get_month_range(year, month)
            time_label = f"{year}-{month:02d}"

        row_data = calculate_metrics(start, end)

        ws.append([
            row_data.get("Marketplace", ""),
            time_label,
            row_data.get("Gross Revenue", 0),
            row_data.get("Expenses", 0),
            row_data.get("Estimated Payout", 0),
            row_data.get("Net Profit", 0),
            row_data.get("Units Sold", 0),
            row_data.get("PPC Sales", 0),
        ])

    # Save to BytesIO stream
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    # Build response
    response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=profit_loss_report.xlsx'
    return response


def profitLossChartCsv(request):
    def get_month_range(year, month):
        from calendar import monthrange
        start_date = datetime(year, month, 1)
        last_day = monthrange(year, month)[1]
        end_date = datetime(year, month, last_day, 23, 59, 59)
        return start_date, end_date

    def generate_month_keys(start_year, start_month, end_year, end_month):
        months = []
        current = datetime(start_year, start_month, 1)
        end = datetime(end_year, end_month, 1)
        while current <= end:
            months.append(current.strftime("%Y-%m-%d 00:00:00"))
            current += timedelta(days=32)
            current = current.replace(day=1)
        return months

    def get_date_range(preset):
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        if preset == "Today":
            return today, today + timedelta(days=1) - timedelta(seconds=1)
        elif preset == "Yesterday":
            yesterday = today - timedelta(days=1)
            return yesterday, today - timedelta(seconds=1)
        elif preset == "This Week":
            start = today - timedelta(days=today.weekday())
            return start, today + timedelta(days=6 - today.weekday(), hours=23, minutes=59, seconds=59)
        elif preset == "Last 7 days":
            return today - timedelta(days=6), today + timedelta(hours=23, minutes=59, seconds=59)
        elif preset == "Last 14 days":
            return today - timedelta(days=13), today + timedelta(hours=23, minutes=59, seconds=59)
        elif preset == "Last 30 days":
            return today - timedelta(days=29), today + timedelta(hours=23, minutes=59, seconds=59)
        elif preset == "Last 60 days":
            return today - timedelta(days=59), today + timedelta(hours=23, minutes=59, seconds=59)
        elif preset == "Last 90 days":
            return today - timedelta(days=89), today + timedelta(hours=23, minutes=59, seconds=59)
        return today, today
    def gross_revenue(start_date, end_date):
        pipeline = [
            {
                "$match": {
                    "order_date": {"$gte": start_date, "$lte": end_date},
                    "order_status": {"$in": ['Shipped', 'Delivered']}
                }
            },
            {
                "$project": {
                    "order_items": 1,
                    "order_total": 1,
                    "marketplace_id": 1,

                }
            }
        ]
        return list(Order.objects.aggregate(*pipeline))
    def dummy_calculate_metrics(start_date, end_date):
        gross_revenue_amt = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        order_total = 0
        other_price = 0
        tax_price = 0
        temp_price = 0
        m_name = ""
        result = gross_revenue(start_date, end_date)
        for order in result:
            gross_revenue_amt += order.get("order_total", 0)
            order_total = order.get("order_total", 0)
            temp_price = 0
            tax_price = 0
            marketplace_id = order.get("marketplace_id", "")
            Marketplace_obj = Marketplace.objects.filter(id = marketplace_id).first()
            m_name = ""
            if Marketplace_obj:
                m_name = Marketplace_obj.name
            for item_id in order.get("order_items", []):
                item_pipeline = [
                    {"$match": {"_id": item_id}},
                    {
                        "$lookup": {
                            "from": "product",
                            "localField": "ProductDetails.product_id",
                            "foreignField": "_id",
                            "as": "product_ins"
                        }
                    },
                    {"$unwind": {"path": "$product_ins", "preserveNullAndEmptyArrays": True}},
                    {
                        "$project": {
                            "price": "$Pricing.ItemPrice.Amount",
                            "tax_price": "$Pricing.ItemTax.Amount",
                            "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                            "sku": "$product_ins.sku"
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item = item_result[0]
                    temp_price += item.get("price", 0)
                    tax_price += item.get("tax_price", 0)
                    total_cogs += item.get("cogs", 0)
                    total_units += 1
                    sku = item.get("sku")
                    if sku:
                        sku_set.add(sku)

            other_price += order_total - temp_price - tax_price

        net_profit = gross_revenue_amt - (other_price + total_cogs)
        margin = (net_profit / gross_revenue_amt * 100) if gross_revenue_amt else 0

        return {
            "Marketplace":m_name,
            "Date and Time":start_date,
            "Gross Revenue": round(gross_revenue_amt, 2),
            "Expenses": round((other_price + total_cogs) , 2),
            "Estimated Payout":0,
            "Net Profit": round(net_profit, 2),
            # "ROI (%)": round((net_profit / (other_price + total_cogs)) * 100, 2) if (other_price + total_cogs) else 0,
            "Units Sold": total_units,
            # "SKU Count": len(sku_set),
            # "Tax Collected": round(tax_price, 2),
            # "Total COGS": round(total_cogs, 2),
            # "Profit Margin (%)": round(margin, 2)
            "PPC Sales": 0 
        }

    preset = request.GET.get('preset', 'Last 7 days')
    from_date, to_date = get_date_range(preset)

    hourly_presets = ["Today", "Yesterday"]
    daily_presets = ["This Week", "Last Week", "Last 7 days", "Last 14 days", "Last 30 days", "Last 60 days", "Last 90 days"]

    if preset in hourly_presets:
        interval_keys = [(from_date + timedelta(hours=i)).strftime("%Y-%m-%d %H:00:00")
                         for i in range(0, int((to_date - from_date).total_seconds() // 3600) + 1)]
        interval_type = "hour"
    elif preset in daily_presets:
        interval_keys = [(from_date + timedelta(days=i)).strftime("%Y-%m-%d 00:00:00")
                         for i in range((to_date - from_date).days + 1)]
        interval_type = "day"
    else:
        interval_keys = generate_month_keys(from_date.year, from_date.month, to_date.year, to_date.month)
        interval_type = "month"

    # Prepare CSV
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="profit_loss_{preset.replace(" ", "_").lower()}.csv"'

    writer = csv.writer(response)
    writer.writerow(["Marketplace", "Date and Time", "Gross Revenue", "Expenses", "Estimated Payout", "Net Profit", "Units", "PPC Sales"])

    for key in interval_keys:
        if interval_type == "hour":
            start = datetime.strptime(key, "%Y-%m-%d %H:00:00")
            end = start + timedelta(hours=1) - timedelta(seconds=1)
        elif interval_type == "day":
            start = datetime.strptime(key, "%Y-%m-%d 00:00:00")
            end = start + timedelta(days=1) - timedelta(seconds=1)
        else:
            year, month = int(key[:4]), int(key[5:7])
            start, end = get_month_range(year, month)

        data = dummy_calculate_metrics(start, end)
        print(data)

        writer.writerow([
            data.get("Marketplace", ""),
            data.get("Date and Time", "").strftime("%Y-%m-%d %H:%M:%S") if isinstance(data.get("Date and Time"), datetime) else data.get("Date and Time"),
            data.get("Gross Revenue", 0),
            data.get("Expenses", 0),
            data.get("Estimated Payout", 0),
            data.get("Net Profit", 0),
            data.get("Units Sold", 0),
            data.get("PPC Sales", 0),
        ])

    return response

from rest_framework.parsers import JSONParser # type: ignore

@csrf_exempt
def updateChooseMatrix(request):
    json_req = JSONParser().parse(request)
    name = json_req['name']
    if 'select_all' in json_req and json_req['select_all'] == True:
        update_fields = {
        'select_all': json_req['select_all']}

    else:
        if name == "Today Snapshot":
            update_fields = {
                'select_all': False,
            'gross_revenue': json_req['gross_revenue'],
            'total_cogs': json_req['total_cogs'],
            'profit_margin': json_req['profit_margin'],
            'orders': json_req['orders'],
            'units_sold': json_req['units_sold'],
            'business_value': json_req['business_value'],
            'refund_quantity': json_req['refund_quantity'],
            }
        elif name == "Revenue":
            update_fields = {
            'select_all': False,
            'gross_revenue': json_req['gross_revenue'],
            'units_sold': json_req['units_sold'],
            'acos': json_req['acos'],
            'tacos': json_req['tacos'],
            'refund_quantity': json_req['refund_quantity'],
            'net_profit': json_req['net_profit'],
            'profit_margin': json_req['profit_margin'],
            'refund_amount': json_req['refund_amount'],
            'roas': json_req['roas'],
            'orders': json_req['orders'],
            'ppc_spend': json_req['ppc_spend']
            }
    updated_count = chooseMatrix.objects.filter(name=name).update(**update_fields)

    if updated_count == 0:
        return JsonResponse({'status': 'not found', 'message': f'No entry found with name: {name}'}, status=404)

    return JsonResponse({'status': 'success', 'updated_records': updated_count}, status=200)



def createNotes(self, request):
    try:
        data = JSONParser().parse(request)


        product_id = data.get("product_id")
        user_id = data.get("user_id")
        notes = data.get("notes")

        if not product_id or not user_id or not notes:
            return JsonResponse({"error": "Missing required fields."}, status=400)

        try:
            product = Product.objects.get(id=product_id)
            user_obj = user.objects.get(id=user_id)
        except :
            return JsonResponse({"error": "Product or user not found."}, status=404)

        note = notes_data(product_id=product, user_id=user_obj, notes=notes)
        note.save()

        return JsonResponse({"message": "Note added successfully."}, status=201)

    except :
        return JsonResponse({"error": ""}, status=500)
    
import re
def ListingOptimizationView(request):
    all_products = Product.objects()
    optimized_count = 0
    total_products = all_products.count()

    def is_optimized(product):
        # Title check
        title = product.product_title or ""
        if len(title) < 100 or re.search(r'(?i)(best|free|deal|offer|discount)', title):
            return False

        # Bullet check
        bullets = product.features or []
        if len(bullets) < 5:
            return False
        if any(re.search(r'<|>|🔥|👍|😁|[A-Z]{4,}', b) for b in bullets):
            return False

        # Description check
        description = product.product_description or ""
        if len(description) <= 300:
            return False
        words = re.findall(r'\b\w+\b', description)
        if len(words) != len(set(words)):
            return False

        # Image check
        images = product.image_urls or []
        if not images:
            return False
        if any(
            not img.endswith(('.jpg', '.jpeg', '.png')) or 'watermark' in img.lower()
            for img in images
        ):
            return False

        # UPC check
        upc = product.upc or ""
        if not re.fullmatch(r'\d{12,14}', upc):
            return False

        # Category check
        category = product.category or ""
        if ">" not in category:
            return False

        return True

    for product in all_products:
        if is_optimized(product):
            optimized_count += 1

    return JsonResponse({
        "total_products": total_products,
        "optimized_products": optimized_count,
        "not_optimized_products": total_products - optimized_count
    })



def obtainChooseMatrix(request):
    name = request.GET.get('name')
    item_pipeline = [
                        { "$match": { "name": name } },
                    ]
    item_result = list(chooseMatrix.objects.aggregate(*item_pipeline))
    if item_result:
        del item_result[0]['_id']
        item_result = item_result[0]
        return JsonResponse(item_result,safe=False)
    return JsonResponse({},safe=False)


def InsightsDashboardView(request):

    all_products = Product.objects()
    total_products = all_products.count()
    optimized_count = 0
    refund_alerts = []
    fee_alerts = []

    def is_optimized(product):
        title = product.product_title or ""
        if len(title) < 100 or re.search(r'(?i)(best|free|deal|offer|discount)', title):
            return False

        bullets = product.features or []
        if len(bullets) < 5 or any(re.search(r'<|>|🔥|👍|😁|[A-Z]{4,}', b) for b in bullets):
            return False

        description = product.product_description or ""
        if len(description) <= 300:
            return False
        words = re.findall(r'\b\w+\b', description)
        if len(words) != len(set(words)):
            return False

        images = product.image_urls or []
        if not images or any(not img.endswith(('.jpg', '.jpeg', '.png')) or 'watermark' in img.lower() for img in images):
            return False

        upc = product.upc or ""
        if not re.fullmatch(r'\d{12,14}', upc):
            return False

        category = product.category or ""
        if ">" not in category:
            return False

        return True

    Refund_obj = Refund.objects()
    refunded_product_ids = list(set([i.product_id.id for i in Refund_obj]))
    for product_id in refunded_product_ids:
        product = Product.objects(id=product_id).first()
        if not product:
            continue

        if is_optimized(product):
            optimized_count += 1

        # Count orders using aggregation
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
                    "order_items.ProductDetails.product_id": ObjectId(str(product.id))
                }
            }
        ]
        orders = list(Order.objects.aggregate(*pipeline))
        total_orders = len(orders)
        refund_count = Refund.objects(product_id=product.id).count()
        # total_orders = 8
        if total_orders > 0:
            refund_rate = (refund_count / total_orders) * 100
            if refund_rate > 6:
                refund_alerts.append({
                    "product_id": str(product.id),
                    "title": product.product_title,
                    "refund_rate": round(refund_rate, 2),
                    "message": f"{product.product_title} has exceeded a 6% refund rate. Refund rates are soaring, impacting your profits. Review, analyze, and revise now."
                })

    # Amazon Fee Analysis
    today = datetime.utcnow()
    start_of_this_month = today.replace(day=1)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)

    this_month_fees = Fee.objects(
        marketplace="amazon.com",
        fee_type="storage",
        date__gte=start_of_this_month,
        date__lt=today
    ).sum('amount') or 0.0

    last_month_fees = Fee.objects(
        marketplace="amazon.com",
        fee_type="storage",
        date__gte=start_of_last_month,
        date__lt=start_of_this_month
    ).sum('amount') or 0.0
    print(this_month_fees,last_month_fees)
    if this_month_fees > last_month_fees:
        increase = round(this_month_fees - last_month_fees, 2)
        fee_alerts.append({
            "marketplace": "amazon.com",
            "increase_amount": increase,
            "message": f"Amazon Storage fees have increased by ${increase} for the amazon.com. Storage fees have increased, cutting into your profit margins. Consider optimizing your inventory or fulfillment strategies now."
        })

    return JsonResponse({
        "total_products": total_products,
        "listing_optimization": {
            "optimized_products": optimized_count,
            "not_optimized_products": total_products - optimized_count
        },
        "alerts": {
            "storage_fee_alerts": fee_alerts,
            "refund_alerts": refund_alerts
        }
    })
