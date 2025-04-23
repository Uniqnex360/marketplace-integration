from mongoengine import Q
from omnisight.models import OrderItems,Order,Marketplace,Product
from mongoengine.queryset.visitor import Q
from dateutil.relativedelta import relativedelta
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime,timedelta
from bson.son import SON
from django.http import JsonResponse
from collections import defaultdict
import pytz
from django.http import HttpResponse
import openpyxl
import csv
from collections import OrderedDict, defaultdict
from collections import defaultdict





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
                "order_total" :1
            }
            }
        ]

    result = list(Order.objects.aggregate(*pipeline))
    return result


def refundOrder(start_date, end_date):
    pipeline = [
            {
            "$match": {
                "order_date": {"$gte": start_date, "$lte": end_date},
                "order_status": 'Refunded'
            }
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


def get_metrics_by_date_range(request):
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
    for i in range(1,9):
        day = eight_days_ago + timedelta(days=i)
        day_key = day.strftime("%B %d").lower()
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

        result = grossRevenue(date_range["start"], date_range["end"])
        refund_ins = refundOrder(date_range["start"], date_range["end"])
        if refund_ins != []:
            for ins in refund_ins:
                refund += len(ins['order_items'])
        total_orders = len(result)
        if result != []:
            for ins in result:
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
                                "cogs": {"$ifNull":["$product_ins.cogs",0.0]}
                            }
                        }
                    ]
                    result = list(OrderItems.objects.aggregate(*pipeline))
                    temp_other_price += result[0]['price']
                    total_cogs += result[0]['cogs']
                    total_units += 1
            other_price += ins['order_total'] - temp_other_price

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
    metrics["difference"] = difference
    return metrics


def LatestOrdersTodayAPIView(request):
    today = datetime.utcnow().date()
    start_of_day = datetime.combine(today, datetime.min.time())
    end_of_day = datetime.combine(today, datetime.max.time())

    # 1️⃣ Hourly aggregation: Use order-level date for bucket, sum quantities from items
    hourly_pipeline = [
        {
            "$match": {
                "order_date": { "$gte": start_of_day, "$lte": end_of_day },
                "order_status": { "$in": ["Shipped", "Delivered"] }
            }
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
            "$match": {
                "order_date": { "$gte": start_of_day, "$lte": end_of_day },
                "order_status": { "$in": ["Shipped", "Delivered"] }
            }
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
    # 1️⃣ Compute UTC bounds for “today”
    today = datetime.utcnow().date()
    start_of_day = datetime.combine(today, datetime.min.time())
    end_of_day   = datetime.combine(today, datetime.max.time())

    # 2️⃣ Fetch all Shippped/Delivered orders for today
    qs = Order.objects.filter(
        order_date__gte=start_of_day,
        order_date__lte=end_of_day,
        order_status__in=["Shipped","Delivered"]
    )

    # 3️⃣ Pre‑fill a 24‑slot OrderedDict for every hour
    chart = OrderedDict()
    bucket = start_of_day.replace(minute=0, second=0, microsecond=0)
    for _ in range(24):
        key = bucket.strftime("%Y-%m-%d %H:00:00")
        chart[key] = {"ordersCount": 0, "unitsCount": 0}
        bucket += timedelta(hours=1)

    # 4️⃣ Build the detail array + populate chart
    orders_out = []
    for order in qs:
        # hour bucket for this order
        bk = order.order_date.replace(minute=0, second=0, microsecond=0)\
                              .strftime("%Y-%m-%d %H:00:00")
        if bk in chart:
            chart[bk]["ordersCount"] += 1

        # iterate each OrderItems instance referenced on this order
        # `order.order_items` is already a list of OrderItems documents
        for item in order.order_items:
            sku        = item.ProductDetails.SKU
            asin = item.ProductDetails.ASIN if item.ProductDetails.ASIN is not None else ""
            qty        = item.ProductDetails.QuantityOrdered
            unit_price = item.Pricing.ItemPrice.Amount
            title      = item.ProductDetails.Title
            # lazy‑load the Product doc for image_url
            prod_ref   = item.ProductDetails.product_id
            img_url    = prod_ref.image_url if prod_ref else None

            total_price = round(unit_price * qty, 2)
            purchase_dt = order.order_date.strftime("%Y-%m-%d %H:%M:%S")

            orders_out.append({
                "sellerSku":      sku,
                "asin":           asin,
                "title":          title,
                "quantityOrdered": qty,
                "imageUrl":       img_url,
                "price":          total_price,
                "purchaseDate":   purchase_dt
            })

            # add to units count
            if bk in chart:
                chart[bk]["unitsCount"] += qty

    # 5️⃣ sort orders by most recent purchaseDate
    orders_out.sort(key=lambda o: o["purchaseDate"], reverse=True)
    data = dict()
    data = {
        "orders": orders_out,
        "hourly_order_count": chart
    }
    return data

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


def get_graph_data(start_date, end_date, preset):
    # Determine time buckets based on preset
    if preset in ["Today", "Yesterday"]:
        # Hourly data for 24 hours
        time_buckets = [(start_date + timedelta(hours=i)).replace(minute=0, second=0, microsecond=0) 
                      for i in range(24)]
        time_format = "%Y-%m-%d %H:00:00"
    else:
        # Daily data for the period
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
        refund_ins = refundOrder(bucket_start, bucket_end)
        if refund_ins:
            for ins in refund_ins:
                if ins['order_date'] >= bucket_start and ins['order_date'] < bucket_end:
                    refund_amount += ins['order_total']
                    refund_quantity += len(ins['order_items'])

        # Process each order in the bucket
        for order in bucket_orders:
            gross_revenue += order.order_total
            temp_other_price = 0
            
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
                            "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]}
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                if result:
                    temp_other_price += result[0]['price']
                    total_cogs += result[0]['cogs']
                    total_units += 1
            
            other_price += order.order_total - temp_other_price

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

def RevenueWidgetAPIView(request):
    preset = request.GET.get("preset", "Today")
    start_date, end_date = get_date_range(preset)
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    total_units = 0
    total_orders = 0

    result = grossRevenue(start_date, end_date)
    refund_ins = refundOrder(start_date, end_date)
    refund_quantity_ins = 0
    if refund_ins != []:
        for ins in refund_ins:
            refund += ins['order_total']
            refund_quantity_ins += len(ins['order_items'])
    total_orders = len(result)
    if result != []:
        for ins in result:
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
                            "cogs": {"$ifNull":["$product_ins.cogs",0.0]}
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                temp_other_price += result[0]['price']
                total_cogs += result[0]['cogs']
                total_units += 1
        other_price += ins['order_total'] - temp_other_price

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
    graph_data = get_graph_data(start_date, end_date, preset)
    data = dict()
    data = {
        "total": total,
        "graph": graph_data
    }
    return data




def get_top_products(request):
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

    # Match criteria based on metric
    if metric == "refund":
       match = {
                "order_date": {"$gte": start_date, "$lt": end_date},
                "order_status": "Refunded"
            }
    else:
        match = {
            "order_date": {"$gte": start_date, "$lt": end_date},
            "order_status": {"$in": ['Shipped', 'Delivered']}
        }

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



def calculate_metrics(start_date, end_date):
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    net_profit = 0
    margin = 0
    total_units = 0
    sku_set = set()
 
    result = grossRevenue(start_date, end_date)
    
    other_price = 0
    if result:
        for order in result:
            gross_revenue += order['order_total']
            order_total = order['order_total']
            temp_price = 0
            
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
                            "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                            "sku": "$product_ins.sku"
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item_data = item_result[0]
                    temp_price += item_data['price']
                    total_cogs += item_data['cogs']
                    total_units += 1
                    if item_data.get('sku'):
                        sku_set.add(item_data['sku'])
            # other_price += (order_total - temp_price) + total_cogs
        other_price += order_total - temp_price
        net_profit = gross_revenue - (other_price + total_cogs)
        margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
 
    return {
        "grossRevenue": round(gross_revenue, 2),
        "expenses": round(other_price + total_cogs, 2),
        "netProfit": round(net_profit, 2),
        "roi": round((net_profit / (other_price + total_cogs)) * 100, 2) if other_price + total_cogs > 0 else 0,
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
    
    def to_utc_format(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
 
    def format_period_metrics(label, current_start, current_end, prev_start, prev_end):
        current_metrics = calculate_metrics(current_start, current_end)
        previous_metrics = calculate_metrics(prev_start, prev_end)
 
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
    y_current_start = datetime(target_date.year, target_date.month, target_date.day)
    y_current_end = y_current_start + timedelta(hours=23, minutes=59, seconds=59)
    y_previous_start = y_current_start - timedelta(days=1)
    y_previous_end = y_previous_start + timedelta(hours=23, minutes=59, seconds=59)
 
    # Last 7 Days
    l7_current_start = target_date - timedelta(days=6)
    l7_current_end = y_current_end
    l7_previous_start = l7_current_start - timedelta(days=7)
    l7_previous_end = l7_current_start - timedelta(seconds=1)
 
    # Last 30 Days
    l30_current_start = target_date - timedelta(days=29)
    l30_current_end = y_current_end
    l30_previous_start = l30_current_start - timedelta(days=30)
    l30_previous_end = l30_current_start - timedelta(seconds=1)
 
    ytd_current_start = datetime(target_date.year, 1, 1)
    ytd_current_end = y_current_end
    last_year = target_date.year - 1
    ytd_previous_start = datetime(last_year, 1, 1)
    ytd_previous_end = datetime(last_year, target_date.month, target_date.day, 23, 59, 59)
 
    response_data = {
        "yesterday": format_period_metrics("Yesterday", y_current_start, y_current_end, y_previous_start, y_previous_end),
        "last7Days": format_period_metrics("Last 7 Days", l7_current_start, l7_current_end, l7_previous_start, l7_previous_end),
        "last30Days": format_period_metrics("Last 30 Days", l30_current_start, l30_current_end, l30_previous_start, l30_previous_end),
        "yearToDate": format_period_metrics("Year to Date", ytd_current_start, ytd_current_end, ytd_previous_start, ytd_previous_end),
    }
    return JsonResponse(response_data, safe=False)

def getPeriodWiseDataCustom(request):
    from_date_str = request.GET.get('from_date')  
    to_date_str = request.GET.get('to_date')
    current_date = datetime.utcnow()
 
    def build_pipeline(start_date, end_date):
        return [
            {"$match": {"order_date": {"$gte": start_date, "$lte": end_date}}},
            
            {"$lookup": {
                "from": "order_items",
                "localField": "order_items",
                "foreignField": "_id",  
                "as": "order_items"  
            }},
            
            {"$unwind": "$order_items"},
            
            {"$addFields": {
                "gross_revenue": {"$toDouble": "$order_items.Pricing.ItemPrice.Amount"},
                "expenses": {"$toDouble": "$order_items.Pricing.ItemTax.Amount"}
            }},
            
            {"$addFields": {
                "net_profit": {"$subtract": ["$gross_revenue", "$expenses"]}
            }},
            
            {"$group": {
                "_id": None,
                "total_gross_revenue": {"$sum": "$gross_revenue"},
                "total_expenses": {"$sum": "$expenses"},
                "total_net_profit": {"$sum": "$net_profit"},
                "total_units_sold": {"$sum": "$number_of_items_shipped"},
                "total_refunds": {"$sum": {"$cond": [{"$eq": ["$order_status", "Refunded"]}, 1, 0]}},
                "total_sku_count": {"$sum": 1},
                "total_sessions": {"$sum": 0},
                "total_page_views": {"$sum": 0},
            }}
        ]
 
    def get_period_data(start, end):
        result = list(Order.objects.aggregate(build_pipeline(start, end)))
        print(result)
        if not result:
            return {
                "grossRevenue": 0, "expenses": 0, "netProfit": 0, "unitsSold": 0,
                "refunds": 0, "skuCount": 0, "sessions": 0, "pageViews": 0,
                "unitSessionPercentage": 0, "margin": 0, "roi": 0
            }
 
        r = result[0]
        gross = r.get("total_gross_revenue", 0)
        expenses = r.get("total_expenses", 0)
        profit = r.get("total_net_profit", 0)
        sessions = r.get("total_sessions", 0)
 
        margin = round((profit / gross * 100) if gross else 0, 2)  
        roi = round((profit / expenses * 100) if expenses else 0, 2)  
 
        return {
            "grossRevenue": gross,
            "expenses": expenses,
            "netProfit": profit,
            "unitsSold": r.get("total_units_sold", 0),
            "refunds": r.get("total_refunds", 0),
            "skuCount": r.get("total_sku_count", 0),
            "sessions": sessions,
            "pageViews": r.get("total_page_views", 0),
            "unitSessionPercentage": round((r["total_units_sold"] / sessions * 100) if sessions else 0, 2),
            "margin": margin,
            "roi": roi
        }
 
    def create_period_response(label, cur_from, cur_to, prev_from, prev_to):
        current = get_period_data(cur_from, cur_to)
        previous = get_period_data(prev_from, prev_to)
 
        def with_delta(metric):
            return {
                "current": current[metric],
                "previous": previous[metric],
                "delta": round(current[metric] - previous[metric], 2)
            }
 
        return {
            "label": label,
            "period": {
                "current": {"from": cur_from.isoformat() + "Z", "to": cur_to.isoformat() + "Z"},
                "previous": {"from": prev_from.isoformat() + "Z", "to": prev_to.isoformat() + "Z"}
            },
            "grossRevenue": with_delta("grossRevenue"),
            "expenses": with_delta("expenses"),
            "netProfit": with_delta("netProfit"),
            "roi": with_delta("roi"),
            "unitsSold": with_delta("unitsSold"),
            "refunds": with_delta("refunds"),
            "skuCount": with_delta("skuCount"),
            "sessions": with_delta("sessions"),
            "pageViews": with_delta("pageViews"),
            "unitSessionPercentage": with_delta("unitSessionPercentage"),
            "margin": with_delta("margin")
        }
    current_date = datetime.now()
    today_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    now = current_date
    yesterday_start = today_start - timedelta(days=1)
    yesterday_end = today_start - timedelta(seconds=1)
 
    last_7_start = today_start - timedelta(days=7)
    last_7_prev_start = today_start - timedelta(days=14)
    last_7_prev_end = last_7_start - timedelta(seconds=1)
 
    
 
    try:
        from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
        to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
    except (TypeError, ValueError):
        from_date = today_start - timedelta(days=30)
        to_date = now
 
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration
 
    response_data = {
        "today": create_period_response("Today", today_start, now, yesterday_start, yesterday_end),
        "yesterday": create_period_response("Yesterday", yesterday_start, yesterday_end, yesterday_start - timedelta(days=1), yesterday_end - timedelta(days=1)),
        "last7Days": create_period_response("Last 7 Days", last_7_start, now, last_7_prev_start, last_7_prev_end),
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date),
    }
 
    return JsonResponse(response_data, safe=False)


###################################SELVA working API's###############################################



def getPeriodWiseDataXl(request):
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
                        "order_items": 1,
                        "order_total": 1,
                        "marketplace_name": "$marketplace_ins.name"
                    }
                },
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

        result = grossRevenue(start_date, end_date)
        order_total = 0
        other_price = 0
        marketplace_name = ""
        if result:
            for order in result:
                marketplace_name = order['marketplace_name']
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                
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
                                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])
                # other_price += (order_total - temp_price) + total_cogs
            other_price += order_total - temp_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round(other_price + total_cogs, 2),
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
            "marketplace":marketplace_name
        }

    def create_period_row(label, start, end):
        data = calculate_metrics(start, end)
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

    today = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    last_7_start = today - timedelta(days=7)
    last_30_start = today - timedelta(days=30)
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    period_rows = [
        create_period_row("Yesterday", yesterday, yesterday),
        create_period_row("Last 7 Days", last_7_start, today - timedelta(seconds=1)),
        create_period_row("Last 30 Days", last_30_start, today - timedelta(seconds=1)),
        create_period_row("Month to Date", month_start, today),
        create_period_row("Year to Date", year_start, today),
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

    ws.append(headers)
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
                        "order_items": 1,
                        "order_total": 1,
                        "marketplace_name": "$marketplace_ins.name"
                    }
                },
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

        result = grossRevenue(start_date, end_date)
        order_total = 0
        other_price = 0
        marketplace_name = ""
        if result:
            for order in result:
                marketplace_name = order['marketplace_name']
                gross_revenue += order['order_total']
                order_total = order['order_total']
                temp_price = 0
                
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
                                "cogs": { "$ifNull": ["$product_ins.cogs", 0.0] },
                                "sku": "$product_ins.sku"
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        total_cogs += item_data['cogs']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])
                # other_price += (order_total - temp_price) + total_cogs
            other_price += order_total - temp_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round(other_price + total_cogs, 2),
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
            "marketplace":marketplace_name
        }

    def create_period_row(label, start, end):
        data = calculate_metrics(start, end)
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

    today = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    last_7_start = today - timedelta(days=7)
    last_30_start = today - timedelta(days=30)
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    period_rows = [
        create_period_row("Yesterday", yesterday, yesterday),
        create_period_row("Last 7 Days", last_7_start, today - timedelta(seconds=1)),
        create_period_row("Last 30 Days", last_30_start, today - timedelta(seconds=1)),
        create_period_row("Month to Date", month_start, today),
        create_period_row("Year to Date", year_start, today),
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
            other_price += order_total - temp_price
            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round(other_price + total_cogs, 2),
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
            "shipping_cost":0
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

    current_date = datetime.now()
    today_start = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    now = current_date
    yesterday_start = today_start - timedelta(days=1)
    yesterday_end = today_start - timedelta(seconds=1)
    last_7_start = today_start - timedelta(days=7)
    last_7_prev_start = today_start - timedelta(days=14)
    last_7_prev_end = last_7_start - timedelta(seconds=1)

    from_date_str = request.GET.get('from_date')  
    to_date_str = request.GET.get('to_date')
    
    try:
        from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
        to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
    except (TypeError, ValueError):
        from_date = today_start - timedelta(days=30)
        to_date = now 
    
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration

    response_data = {
        "today": create_period_response("Today", today_start, now, yesterday_start, yesterday_end),
        "yesterday": create_period_response("Yesterday", yesterday_start, yesterday_end, yesterday_start - timedelta(days=1), yesterday_end - timedelta(days=1)),
        "last7Days": create_period_response("Last 7 Days", last_7_start, now, last_7_prev_start, last_7_prev_end),
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date),
    }

    return JsonResponse(response_data, safe=False)


#>>>>>>>>>>>>>>>>......

def allMarketplaceData(request):
    from_str = request.GET.get("from_date")
    to_str = request.GET.get("to_date")
    try:
        from_date = datetime.strptime(from_str, "%Y-%m-%d")
        to_date = datetime.strptime(to_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    except:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=30)

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
                    "_id": 1,
                    "order_items": 1,
                    "order_total": 1,
                    "marketplace_id": 1,
                    "currency": 1
                }
            }
        ]
        return list(Order.objects.aggregate(*pipeline))

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

                other_price += order_total - temp_price

            expenses = total_cogs + other_price
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

                other_price += order_total - temp_price

            net_profit = gross_revenue - (other_price + total_cogs)
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round(other_price + total_cogs, 2),
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
    }

    return JsonResponse(response_data, safe=False)


def getProductPerformanceSummary(request):
    yesterday = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    today = yesterday + timedelta(days=1)

    order_pipeline = [
        {
            "$match": {
                "order_date": {"$gte": yesterday, "$lt": today},
                "order_status": {"$in": ["Shipped", "Delivered"]}
            }
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
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "sku": "$product_ins.sku",
                        "product_name": "$product_ins.product_title",
                        "images": "$product_ins.image_urls"
                    }
                }
            ]
            item_result = list(OrderItems.objects.aggregate(*item_pipeline))
            if item_result:
                item_data = item_result[0]
                sku = item_data.get("sku")
                product_name = item_data.get("product_name", "")
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

        other_price = order_total - temp_price

        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            net_profit = gross - (other_price + cogs)
            margin = (net_profit / gross) * 100 if gross > 0 else 0
            sku_summary[sku]["netProfit"] = round(net_profit, 2)
            sku_summary[sku]["margin"] = round(margin, 2)

    sorted_skus = sorted(sku_summary.values(), key=lambda x: x["grossRevenue"], reverse=True)

    top_3 = sorted_skus[:3]
    least_3 = sorted_skus[-3:] if len(sorted_skus) >= 3 else sorted_skus

    return JsonResponse({
        "top_3_products": top_3,
        "least_3_products": least_3
    })