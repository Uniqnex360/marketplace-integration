from mongoengine import Q
from omnisight.models import OrderItems,Order
from mongoengine.queryset.visitor import Q
from dateutil.relativedelta import relativedelta
from django.views.decorators.csrf import csrf_exempt
from datetime import datetime
from bson.son import SON
from datetime import datetime, timedelta
from django.http import JsonResponse
from collections import defaultdict
import pytz




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
                "order_total" :1
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


# def get_metrics_by_date_range(request):
#     target_date_str = request.GET.get('target_date')
#     # Parse target date and previous day
#     target_date = datetime.strptime(target_date_str, "%d/%m/%Y")
#     previous_date = target_date - timedelta(days=1)
#     eight_days_ago = target_date - timedelta(days=8)

#     # Define the date filters
#     date_filters = {
#         "targeted": {
#             "start": datetime(target_date.year, target_date.month, target_date.day),
#             "end": datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59)
#         },
#         "previous": {
#             "start": datetime(previous_date.year, previous_date.month, previous_date.day),
#             "end": datetime(previous_date.year, previous_date.month, previous_date.day, 23, 59, 59)
#         }
#     }

#     # Define the last 8 days filter as a dictionary with each day's range
#     last_8_days_filter = {}
#     for i in range(1,9):
#         day = eight_days_ago + timedelta(days=i)
#         day_key = day.strftime("%B %d").lower()
#         last_8_days_filter[day_key] = {
#             "start": datetime(day.year, day.month, day.day),
#             "end": datetime(day.year, day.month, day.day, 23, 59, 59)
#         }

#     metrics = {}
#     graph_data = {}

#     for key, date_range in last_8_days_filter.items():
#         gross_revenue = 0
#         pipeline = [
#             {
#             "$match": {
#                 "order_date": {"$gte": date_range["start"], "$lte": date_range["end"]},
#                 "order_status": {"$in": ['Shipped', 'Delivered']}
#             }
#             },
#             {
#             "$group": {
#                 "_id": None,
#                 "order_items": {"$push": "$order_items"}
#             }
#             },
#             {
#             "$project": {
#                 "order_items": {
#                 "$reduce": {
#                     "input": "$order_items",
#                     "initialValue": [],
#                     "in": {"$concatArrays": ["$$value", "$$this"]}
#                 }
#                 }
#             }
#             }
#         ]

#         result = list(Order.objects.aggregate(*pipeline))
#         if result != []:
#             order_items = result[0]["order_items"]
            
#             for ins in order_items:
#                 pipeline = [
#                     {
#                         "$match": {
#                             "_id": ins
#                         }
#                     },
#                     {
#                         "$lookup": {
#                             "from": "product",  # Assuming your product COGS is here
#                             "localField": "ProductDetails.product_id",
#                             "foreignField": "_id",
#                             "as": "product_ins"
#                         }
#                     },
#                     {
#                     "$unwind": {
#                         "path": "$product_ins",
#                         "preserveNullAndEmptyArrays": True  # Ensure the product is not removed if no orders exist
#                     }
#                     },
#                     {
#                         "$project": {
#                             "_id" : 0, 
#                             "price": "$Pricing.ItemPrice.Amount",
#                             "qty": "$ProductDetails.QuantityOrdered",
#                         }
#                     },
#                     {
#                         "$group": {
#                             "_id": None,
#                             "gross_revenue": {"$sum": {"$multiply": ["$price", "$qty"]}},
#                         }
#                     },
#                     {
#                         "$project": {
#                             "_id": 0,
#                             "gross_revenue": 1,
#                         }
#                     }
#                 ]
#                 result = list(OrderItems.objects.aggregate(*pipeline))
#                 for items in result:
#                     gross_revenue += items['gross_revenue']
#         graph_data[key] = {
#             "gross_revenue": round(gross_revenue, 2),
#         }
#     metrics["graph_data"] = graph_data
#     for key, date_range in date_filters.items():
#         gross_revenue = 0
#         total_cogs = 0
#         refund = 0
#         margin = 0
#         net_profit = 0
#         total_units = 0
#         total_orders = 0

#         pipeline = [
#             {
#             "$match": {
#                 "order_date": {"$gte": date_range["start"], "$lte": date_range["end"]},
#                 "order_status": {"$in": ['Shipped', 'Delivered']}
#             }
#             },
#             {
#             "$group": {
#                 "_id": None,
#                 "order_items": {"$push": "$order_items"}
#             }
#             },
#             {
#             "$project": {
#                 "order_items": {
#                 "$reduce": {
#                     "input": "$order_items",
#                     "initialValue": [],
#                     "in": {"$concatArrays": ["$$value", "$$this"]}
#                 }
#                 }
#             }
#             }
#         ]

#         result = list(Order.objects.aggregate(*pipeline))
#         pipeline = [
#             {
#             "$match": {
#                 "order_date": {"$gte": date_range["start"], "$lte": date_range["end"]},
#                 "order_status": {"$in": ['Shipped', 'Delivered']}
#             }
#             },
#             {
#             "$project": {
#                 "_id" : 1
#             }
#             }
#         ]

#         result1 = list(Order.objects.aggregate(*pipeline))
#         total_orders = len(result1)
#         if result != []:
#             order_items = result[0]["order_items"]
            
#             for ins in order_items:
#                 pipeline = [
#                     {
#                         "$match": {
#                             "_id": ins
#                         }
#                     },
#                     {
#                         "$lookup": {
#                             "from": "product",  # Assuming your product COGS is here
#                             "localField": "ProductDetails.product_id",
#                             "foreignField": "_id",
#                             "as": "product_ins"
#                         }
#                     },
#                     {
#                     "$unwind": {
#                         "path": "$product_ins",
#                         "preserveNullAndEmptyArrays": True  # Ensure the product is not removed if no orders exist
#                     }
#                     },
#                     {
#                         "$project": {
#                             "_id" : 0,
#                             "order_total": 1,
#                             "refund": "$refund_total",  
#                             "price": "$Pricing.ItemPrice.Amount",
#                             "qty": "$ProductDetails.QuantityOrdered",
#                             "cogs": "$product_ins.cogs"
#                         }
#                     },
#                     {
#                         "$group": {
#                             "_id": None,
#                             "gross_revenue": {"$sum": {"$multiply": ["$price", "$qty"]}},
#                             "total_cogs": {"$sum": {"$multiply": ["$cogs", "$qty"]}},
#                             "refund": {"$sum": "$refund"},
#                             "total_orders": {"$sum": 1},
#                             "total_units": {"$sum": "$qty"}
#                         }
#                     },
#                     {
#                         "$project": {
#                             "_id": 0,
#                             "gross_revenue": 1,
#                             "total_cogs": 1,
#                             "refund": 1,
#                             "margin": {"$subtract": ["$gross_revenue", "$total_cogs"]},
#                             "net_profit": {"$subtract": [{"$subtract": ["$gross_revenue", "$total_cogs"]}, "$refund"]},
#                             "total_orders": 1,
#                             "total_units": 1
#                         }
#                     }
#                 ]
#                 result = list(OrderItems.objects.aggregate(*pipeline))
#                 for items in result:
#                     gross_revenue += items['gross_revenue']
#                     total_cogs += items['total_cogs']
#                     refund += items['refund']
#                     margin += items['margin']
#                     net_profit += items['net_profit']
#                     total_units += items['total_units']




#         metrics[key] = {
#             "gross_revenue": round(gross_revenue, 2),
#             "total_cogs": round(total_cogs, 2),
#             "refund": round(refund, 2),
#             "margin": round(margin, 2),
#             "net_profit": round(net_profit, 2),
#             "total_orders": round(total_orders, 2),
#             "total_units": round(total_units, 2)
#         }
#     difference = {
#         "gross_revenue": round(metrics["targeted"]["gross_revenue"] - metrics["previous"]["gross_revenue"],2),
#         "total_cogs": round(metrics["targeted"]["total_cogs"] - metrics["previous"]["total_cogs"],2),
#         "refund": round(metrics["targeted"]["refund"] - metrics["previous"]["refund"],2),
#         "margin": round(metrics["targeted"]["margin"] - metrics["previous"]["margin"],2),
#         "net_profit": round(metrics["targeted"]["net_profit"] - metrics["previous"]["net_profit"],2),
#         "total_orders": round(metrics["targeted"]["total_orders"] - metrics["previous"]["total_orders"],2),
#         "total_units": round(metrics["targeted"]["total_units"] - metrics["previous"]["total_units"],2)

#     }
#     metrics["difference"] = difference
#     return metrics



from datetime import datetime, timedelta
from django.http import JsonResponse

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

from collections import OrderedDict, defaultdict

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


def RevenueWidgetAPIView(request):
    preset = request.GET.get("preset", "Today")
    start_date, end_date = get_date_range(preset)
    gross_revenue = 0
    total_cogs = 0
    refund = 0
    margin = 0
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
        margin = (net_profit / gross_revenue) * 100

    # Total values
    total = {
        "gross_revenue": round(gross_revenue, 2),
        "net_profit": round(net_profit, 2),
        "profit_margin": round(margin, 2),
        "orders": round(total_orders, 2),
        "units_sold": round(total_units, 2),
        "refund_amount": round(refund, 2),
        "refund_quantity": refund_quantity_ins
        
    }

    group_format = {
        "Today": { "hour": { "$hour": "$order_date" } },
        "Yesterday": { "hour": { "$hour": "$order_date" } }
    }

    time_group = group_format.get(preset, { "date": { "$dateToString": { "format": "%Y-%m-%d", "date": "$order_date" } } })

    pipeline = [
        {
            "$match": {
                "order_date": { "$gte": start_date, "$lt": end_date }
            }
        },
        { "$unwind": "$order_items" },
        {
            "$project": {
                "order_date": 1,
                "hour": { "$hour": "$order_date" },
                "product_id": "$order_items.ProductDetails.SKU",
                "quantity": "$order_items.ProductDetails.QuantityOrdered",
                "price": "$order_total",
                "cogs": "$order_items.ProductDetails.COGS",  # Assuming this is present
                "refund": "$order_items.ProductDetails.RefundAmount",
                "refunded": "$order_items.ProductDetails.RefundedQuantity"
            }
        },
        {
            "$group": {
                "_id": time_group,
                "gross_revenue": { "$sum": "$price" },
                "total_cogs": { "$sum": { "$multiply": ["$quantity", "$cogs"] } },
                "refund_amount": { "$sum": "$refund" },
                "refund_qty": { "$sum": "$refunded" },
                "orders": { "$sum": 1 },
                "units_sold": { "$sum": "$quantity" }
            }
        },
        {
            "$addFields": {
                "net_profit": { "$subtract": ["$gross_revenue", "$total_cogs"] },
                "profit_margin": {
                    "$cond": [
                        { "$gt": ["$gross_revenue", 0] },
                        { "$multiply": [
                            { "$divide": [
                                { "$subtract": ["$gross_revenue", "$total_cogs"] },
                                "$gross_revenue"
                            ] },
                            100
                        ] },
                        0
                    ]
                }
            }
        },
        { "$sort": { "_id": 1 } }
    ]

    results = list(Order.objects.aggregate(pipeline))


    graph_data = []

    for item in results:
        # total["gross_revenue"] += item.get("gross_revenue", 0)
        # total["net_profit"] += item.get("net_profit", 0)
        # total["orders"] += item.get("orders", 0)
        # total["units_sold"] += item.get("units_sold", 0)
        # total["refund_amount"] += item.get("refund_amount", 0)
        # total["refund_quantity"] += item.get("refund_qty", 0)

        graph_data.append({
            "time": item["_id"].get("hour") if "hour" in item["_id"] else item["_id"].get("date"),
            "gross_revenue": item.get("gross_revenue", 0),
            "net_profit": item.get("net_profit", 0),
            "profit_margin": round(item.get("profit_margin", 0), 2),
            "orders": item.get("orders", 0),
            "units_sold": item.get("units_sold", 0),
            "refund_amount": item.get("refund_amount", 0),
            "refund_quantity": item.get("refund_qty", 0),
        })

    # Calculate final profit margin
    total["profit_margin"] = round((total["net_profit"] / total["gross_revenue"]) * 100, 2) if total["gross_revenue"] else 0
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
            "$limit": 5
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



def revenue_widget(request):
    # Get range from query param
    date_range = request.GET.get("range", "Today")

    # Define date range
    now = datetime.utcnow()
    start_date = end_date = now

    if date_range == "Today":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "Yesterday":
        start_date = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
    elif date_range == "Last 7 Days":
        start_date = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "Last 30 Days":
        start_date = (now - timedelta(days=29)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "This Month":
        start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif date_range == "Last Month":
        last_month = now.replace(day=1) - timedelta(days=1)
        start_date = last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = last_month.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Main filters
    date_filter = {"order_date": {"$gte": start_date}}
    if end_date:
        date_filter["order_date"]["$lte"] = end_date

    # Aggregate Gross Revenue
    gross_pipeline = [
        {"$match": date_filter},
        {"$group": {"_id": None, "total": {"$sum": "$Pricing.totalAmount"}}}
    ]
    gross_result = list(Order.objects.aggregate(*gross_pipeline))
    gross_revenue = gross_result[0]["total"] if gross_result else 0

    # Refund Amount
    refund_pipeline = [
        {"$match": date_filter},
        {"$group": {"_id": None, "total": {"$sum": "$Refund.refundAmount"}}}
    ]
    refund_result = list(Order.objects.aggregate(*refund_pipeline))
    refund_amount = refund_result[0]["total"] if refund_result else 0

    # COGS (Cost of Goods Sold)
    cogs_pipeline = [
        {"$match": date_filter},
        {"$unwind": "$OrderItems"},
        {"$project": {
            "cost": {
                "$multiply": [
                    "$OrderItems.ProductDetails.cogs",
                    "$OrderItems.Quantity"
                ]
            }
        }},
        {"$group": {"_id": None, "total": {"$sum": "$cost"}}}
    ]
    cogs_result = list(Order.objects.aggregate(*cogs_pipeline))
    cogs = cogs_result[0]["total"] if cogs_result else 0

    # Units Sold
    units_pipeline = [
        {"$match": date_filter},
        {"$unwind": "$OrderItems"},
        {"$group": {"_id": None, "total": {"$sum": "$OrderItems.Quantity"}}}
    ]
    units_result = list(Order.objects.aggregate(*units_pipeline))
    units_sold = units_result[0]["total"] if units_result else 0

    # Orders count
    total_orders = Order.objects.filter(**date_filter).count()

    # Profit & Margin
    net_profit = gross_revenue - cogs - refund_amount
    profit_margin = (net_profit / gross_revenue * 100) if gross_revenue else 0

    # Chart breakdown based on range
    if date_range in ["Today", "Yesterday"]:
        # Hourly breakdown
        group_format = "%H"
        label_field = "hour"
        add_fields = {"hour": {"$hour": "$orderDate"}}
    else:
        # Daily breakdown
        group_format = "%Y-%m-%d"
        label_field = "date"
        add_fields = {"date": {"$dateToString": {"format": group_format, "date": "$orderDate"}}}

    chart_pipeline = [
        {"$match": date_filter},
        {"$addFields": add_fields},
        {"$group": {
            "_id": f"${label_field}",
            "grossRevenue": {"$sum": "$Pricing.totalAmount"},
            "refundAmount": {"$sum": "$Refund.refundAmount"},
            "orderCount": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    chart_result = list(Order.objects.aggregate(*chart_pipeline))

    # Final response
    response = {
        "grossRevenue": round(gross_revenue, 2),
        "refundAmount": round(refund_amount, 2),
        "cogs": round(cogs, 2),
        "netProfit": round(net_profit, 2),
        "profitMargin": round(profit_margin, 2),
        "orders": total_orders,
        "unitsSold": units_sold,
        "chartData": chart_result
    }
    return JsonResponse(response)



def get_latest_orders(request):
    limit = request.GET.get('limit')
    # Define the aggregation pipeline
    pipeline = [
        {
            '$sort': {'OrderStatus.StatusDate': -1}
        },
        {
            '$limit': limit
        },
        {
            '$project': {
                'OrderId': 1,
                'Platform': 1,
                'ProductDetails.Title': 1,
                'ProductDetails.SKU': 1,
                'ProductDetails.ASIN': 1,
                'OrderStatus.Status': 1,
                'OrderStatus.StatusDate': 1
            }
        }
    ]

    # Execute the aggregation pipeline
    orders = list(OrderItems.objects.aggregate(*pipeline))

    return orders



def compare_revenue_with_past(request):
    filter_date = request.GET.get('filter_date')

    # Get today's date
    today = datetime.now().date()

    # Convert filter_date string to datetime object
    past_date = datetime.strptime(filter_date, '%Y-%m-%d').date()

    # Helper function to calculate revenue for a specific date
    def calculate_revenue(date):
        start = datetime.combine(date, datetime.min.time())
        end = datetime.combine(date, datetime.max.time())

        pipeline = [
            {
                '$match': {
                    'OrderStatus.StatusDate': {'$gte': start, '$lte': end}
                }
            },
            {
                '$group': {
                    '_id': None,
                    'revenue': {
                        '$sum': {
                            '$multiply': [
                                '$Pricing.ItemPrice.Amount',
                                '$ProductDetails.QuantityOrdered'
                            ]
                        }
                    }
                }
            }
        ]

        result = list(OrderItems.objects.aggregate(*pipeline))
        return result[0]['revenue'] if result else 0

    # Calculate revenues
    today_revenue = calculate_revenue(today)
    past_revenue = calculate_revenue(past_date)

    # Calculate percentage change
    if past_revenue > 0:
        percentage_change = ((today_revenue - past_revenue) / past_revenue) * 100
    else:
        percentage_change = float('inf') if today_revenue > 0 else 0

    return {
        'today_revenue': today_revenue,
        'past_revenue': past_revenue,
        'percentage_change': percentage_change
    }



def get_product_revenue_details(request):
    sku = request.GET.get('sku')
    # Define the aggregation pipeline
    pipeline = [
        {
            '$match': {
                'ProductDetails.SKU': sku
            }
        },
        {
            '$group': {
                '_id': '$ProductDetails.SKU',
                'product_title': {'$first': '$ProductDetails.Title'},
                'total_units_sold': {'$sum': '$ProductDetails.QuantityOrdered'},
                'gross_revenue': {
                    '$sum': {
                        '$multiply': [
                            '$Pricing.ItemPrice.Amount',
                            '$ProductDetails.QuantityOrdered'
                        ]
                    }
                },
                'total_cogs': {
                    '$sum': {
                        '$multiply': [
                            '$Pricing.PromotionDiscount.Amount',
                            '$ProductDetails.QuantityOrdered'
                        ]
                    }
                }
            }
        },
        {
            '$addFields': {
                'net_profit': {'$subtract': ['$gross_revenue', '$total_cogs']},
                'profit_margin': {
                    '$cond': {
                        'if': {'$gt': ['$gross_revenue', 0]},
                        'then': {
                            '$multiply': [
                                {'$divide': ['$net_profit', '$gross_revenue']},
                                100
                            ]
                        },
                        'else': 0
                    }
                }
            }
        }
    ]

    # Execute the aggregation pipeline
    result = list(OrderItems.objects.aggregate(*pipeline))

    if result:
        return result[0]
    else:
        return {
            'product_title': None,
            'total_units_sold': 0,
            'gross_revenue': 0,
            'total_cogs': 0,
            'net_profit': 0,
            'profit_margin': 0
        }



def calculate_date_range(preset):
    now = datetime.now(pytz.UTC)
    if preset == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now
        prev_start = start - timedelta(days=1)
        prev_end = start
        interval = "hour"
    elif preset == "last_7_days":
        end = now
        start = now - timedelta(days=7)
        prev_end = start
        prev_start = prev_end - timedelta(days=7)
        interval = "day"
    elif preset == "this_month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now
        prev_end = start
        prev_start = (start - relativedelta(months=1)).replace(day=1)
        interval = "day"
    else:
        # Default to last 30 days
        end = now
        start = now - timedelta(days=30)
        prev_end = start
        prev_start = prev_end - timedelta(days=30)
        interval = "day"
    return start, end, prev_start, prev_end, interval

def group_by_interval(dt, interval):
    if interval == "day":
        return dt.strftime('%Y-%m-%d')
    elif interval == "week":
        return dt.strftime('%Y-W%U')
    elif interval == "month":
        return dt.strftime('%Y-%m')
    return dt.strftime('%Y-%m-%d')

def aggregate_financials(start_date, end_date, interval):
    data = defaultdict(lambda: {
        "gross": 0.0,
        "net": 0.0,
        "units": 0,
        "orders": 0,
        "refund_amount": 0.0,
        "refund_quantity": 0
    })

    queryset = OrderItems.objects(
        Q(OrderStatus__StatusDate__gte=start_date) &
        Q(OrderStatus__StatusDate__lte=end_date)
    )

    seen_orders = set()
    for item in queryset:
        order_time = item.OrderStatus.StatusDate
        bucket = group_by_interval(order_time, interval)
        price = item.Pricing.ItemPrice.Amount
        quantity = item.ProductDetails.QuantityOrdered
        sku = item.ProductDetails.SKU
        order_id = item.OrderId
        status = item.OrderStatus.Status

        gross = price * quantity
        cogs = item.ProductDetails.product_id.cogs if item.ProductDetails.product_id else 0.0
        net = (price - cogs) * quantity

        if order_id not in seen_orders:
            data[bucket]["orders"] += 1
            seen_orders.add(order_id)

        data[bucket]["gross"] += gross
        data[bucket]["net"] += net
        data[bucket]["units"] += quantity

        if status in ["Returned", "Canceled"]:
            data[bucket]["refund_amount"] += gross
            data[bucket]["refund_quantity"] += quantity

    return data

def merge_metric(data, metric):
    result = []
    for key in sorted(data.keys()):
        result.append({
            "date": key,
            "value": round(data[key][metric], 2)
        })
    return result

def get_current_previous_values(data, metric):
    values = [v[metric] for v in data.values()]
    return round(sum(values), 2)

def revenue_widget_api(request):
    preset = request.GET.get("preset", "last_7_days")
    start, end, prev_start, prev_end, interval = calculate_date_range(preset)

    current_data = aggregate_financials(start, end, interval)
    previous_data = aggregate_financials(prev_start, prev_end, interval)

    response = {
        "metrics": {
            "grossRevenue": {
                "currentValue": get_current_previous_values(current_data, "gross"),
                "previousValue": get_current_previous_values(previous_data, "gross"),
                "currentChart": merge_metric(current_data, "gross"),
                "previousChart": merge_metric(previous_data, "gross"),
            },
            "netProfit": {
                "currentValue": get_current_previous_values(current_data, "net"),
                "previousValue": get_current_previous_values(previous_data, "net"),
                "currentChart": merge_metric(current_data, "net"),
                "previousChart": merge_metric(previous_data, "net"),
            },
            "margin": {
                "currentValue": round(
                    (get_current_previous_values(current_data, "net") /
                     get_current_previous_values(current_data, "gross")) * 100
                    if get_current_previous_values(current_data, "gross") else 0.0, 2
                ),
                "previousValue": round(
                    (get_current_previous_values(previous_data, "net") /
                     get_current_previous_values(previous_data, "gross")) * 100
                    if get_current_previous_values(previous_data, "gross") else 0.0, 2
                ),
                "currentChart": [],
                "previousChart": [],
            },
            "orders": {
                "currentValue": get_current_previous_values(current_data, "orders"),
                "previousValue": get_current_previous_values(previous_data, "orders"),
                "currentChart": merge_metric(current_data, "orders"),
                "previousChart": merge_metric(previous_data, "orders"),
            },
            "units": {
                "currentValue": get_current_previous_values(current_data, "units"),
                "previousValue": get_current_previous_values(previous_data, "units"),
                "currentChart": merge_metric(current_data, "units"),
                "previousChart": merge_metric(previous_data, "units"),
            },
            "refundAmount": {
                "currentValue": get_current_previous_values(current_data, "refund_amount"),
                "previousValue": get_current_previous_values(previous_data, "refund_amount"),
                "currentChart": merge_metric(current_data, "refund_amount"),
                "previousChart": merge_metric(previous_data, "refund_amount"),
            },
            "refundQuantity": {
                "currentValue": get_current_previous_values(current_data, "refund_quantity"),
                "previousValue": get_current_previous_values(previous_data, "refund_quantity"),
                "currentChart": merge_metric(current_data, "refund_quantity"),
                "previousChart": merge_metric(previous_data, "refund_quantity"),
            },
            "ppcSpend": {
                "currentValue": 0.0,
                "previousValue": 0.0,
                "currentChart": [],
                "previousChart": [],
            },
        },
        "netProfitCalculation": {
            "grossRevenue": get_current_previous_values(current_data, "gross"),
            "productCOGS": round(
                get_current_previous_values(current_data, "gross") -
                get_current_previous_values(current_data, "net"), 2
            ),
            "netProfit": get_current_previous_values(current_data, "net"),
        },
        "dateRange": {
            "preset": preset,
            "current": {
                "start": start.isoformat(),
                "end": end.isoformat()
            },
            "previous": {
                "start": prev_start.isoformat(),
                "end": prev_end.isoformat()
            },
            "granularity": interval
        }
    }
    return JsonResponse(response)
