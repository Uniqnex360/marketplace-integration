from mongoengine import Q
from datetime import datetime, timedelta
from omnisight.models import OrderItems,Order
from datetime import datetime, timedelta
from mongoengine.queryset.visitor import Q



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
        pipeline = [
            {
            "$match": {
                "order_date": {"$gte": date_range["start"], "$lte": date_range["end"]},
                "order_status": {"$in": ['Shipped', 'Delivered']}
            }
            },
            {
            "$group": {
                "_id": None,
                "order_items": {"$push": "$order_items"}
            }
            },
            {
            "$project": {
                "order_items": {
                "$reduce": {
                    "input": "$order_items",
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", "$$this"]}
                }
                }
            }
            }
        ]

        result = list(Order.objects.aggregate(*pipeline))
        if result != []:
            order_items = result[0]["order_items"]
            
            for ins in order_items:
                pipeline = [
                    {
                        "$match": {
                            "_id": ins
                        }
                    },
                    {
                        "$lookup": {
                            "from": "product",  # Assuming your product COGS is here
                            "localField": "ProductDetails.product_id",
                            "foreignField": "_id",
                            "as": "product_ins"
                        }
                    },
                    {
                    "$unwind": {
                        "path": "$product_ins",
                        "preserveNullAndEmptyArrays": True  # Ensure the product is not removed if no orders exist
                    }
                    },
                    {
                        "$project": {
                            "_id" : 0, 
                            "price": "$Pricing.ItemPrice.Amount",
                            "qty": "$ProductDetails.QuantityOrdered",
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "gross_revenue": {"$sum": {"$multiply": ["$price", "$qty"]}},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "gross_revenue": 1,
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                for items in result:
                    gross_revenue += items['gross_revenue']
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
        total_orders  = 0
        total_units = 0
        pipeline = [
            {
            "$match": {
                "order_date": {"$gte": date_range["start"], "$lte": date_range["end"]},
                "order_status": {"$in": ['Shipped', 'Delivered']}
            }
            },
            {
            "$group": {
                "_id": None,
                "order_items": {"$push": "$order_items"}
            }
            },
            {
            "$project": {
                "order_items": {
                "$reduce": {
                    "input": "$order_items",
                    "initialValue": [],
                    "in": {"$concatArrays": ["$$value", "$$this"]}
                }
                }
            }
            }
        ]

        result = list(Order.objects.aggregate(*pipeline))
        if result != []:
            order_items = result[0]["order_items"]
            
            for ins in order_items:
                pipeline = [
                    {
                        "$match": {
                            "_id": ins
                        }
                    },
                    {
                        "$lookup": {
                            "from": "product",  # Assuming your product COGS is here
                            "localField": "ProductDetails.product_id",
                            "foreignField": "_id",
                            "as": "product_ins"
                        }
                    },
                    {
                    "$unwind": {
                        "path": "$product_ins",
                        "preserveNullAndEmptyArrays": True  # Ensure the product is not removed if no orders exist
                    }
                    },
                    {
                        "$project": {
                            "_id" : 0,
                            "order_total": 1,
                            "refund": "$refund_total",  
                            "price": "$Pricing.ItemPrice.Amount",
                            "qty": "$ProductDetails.QuantityOrdered",
                            "cogs": "$product_ins.cogs"
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "gross_revenue": {"$sum": {"$multiply": ["$price", "$qty"]}},
                            "total_cogs": {"$sum": {"$multiply": ["$cogs", "$qty"]}},
                            "refund": {"$sum": "$refund"},
                            "total_orders": {"$sum": 1},
                            "total_units": {"$sum": "$qty"}
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "gross_revenue": 1,
                            "total_cogs": 1,
                            "refund": 1,
                            "margin": {"$subtract": ["$gross_revenue", "$total_cogs"]},
                            "net_profit": {"$subtract": [{"$subtract": ["$gross_revenue", "$total_cogs"]}, "$refund"]},
                            "total_orders": 1,
                            "total_units": 1
                        }
                    }
                ]
                result = list(OrderItems.objects.aggregate(*pipeline))
                for items in result:
                    gross_revenue += items['gross_revenue']
                    total_cogs += items['total_cogs']
                    refund += items['refund']
                    margin += items['margin']
                    net_profit += items['net_profit']
                    total_orders  += items['total_orders']
                    total_units += items['total_units']




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
        "total_units": round(metrics["targeted"]["total_units"] - metrics["previous"]["total_units"],2)

    }
    metrics["difference"] = difference
    return metrics





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
