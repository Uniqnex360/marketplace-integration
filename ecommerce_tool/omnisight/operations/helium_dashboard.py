from mongoengine import Q
from omnisight.models import OrderItems,Order,Marketplace,Product,CityDetails,user,notes_data,chooseMatrix,Fee,Refund,Brand
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
from django.db.models import Sum, Q
from omnisight.operations.helium_utils import calculate_metricss, get_date_range, grossRevenue, get_previous_periods, refundOrder,AnnualizedRevenueAPIView,getOrdersListBasedonProductId, getproductIdListBasedonbrand, getdaywiseproductssold, pageViewsandSessionCount, getproductIdListBasedonManufacture,totalRevenueCalculation,get_graph_data
from ecommerce_tool.crud import DatabaseModel
from omnisight.operations.common_utils import calculate_listing_score
import threading
from concurrent.futures import ThreadPoolExecutor



@csrf_exempt
def get_metrics_by_date_range(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id',None)
    target_date_str = json_request.get('target_date')
    brand_id = json_request.get('brand_id',None)
    product_id = json_request.get('product_id',None)
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    
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

    def process_date_range(key, date_range, results):
        gross_revenue = 0
        result = grossRevenue(date_range["start"], date_range["end"], marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)
        if result != []:
            for ins in result:
                gross_revenue += ins['order_total']
        results[key] = {
            "gross_revenue": round(gross_revenue, 2),
        }

    results = {}
    threads = []
    for key, date_range in last_8_days_filter.items():
        thread = threading.Thread(target=process_date_range, args=(key, date_range, results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Ensure the results are in the same order as the keys in last_8_days_filter
    graph_data = {key: results[key] for key in last_8_days_filter.keys()}
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
        temp_other_price = 0
        vendor_funding = 0

        result = grossRevenue(date_range["start"], date_range["end"],marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        refund_ins = refundOrder(date_range["start"], date_range["end"],marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        if refund_ins != []:
            for ins in refund_ins:
                refund += len(ins['order_items'])
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
                        if ins['marketplace_name'] == "Amazon":
                            total_cogs += result[0]['total_cogs']
                        else:
                            total_cogs += result[0]['w_total_cogs']
                        total_units += 1
                        vendor_funding += result[0]['vendor_funding']
            # other_price += ins['order_total'] - temp_other_price - tax_price

            net_profit = (temp_other_price - total_cogs) + vendor_funding
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
    # metrics['targeted']["business_value"] = AnnualizedRevenueAPIView(target_date)
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
        # if item_result['business_value'] == False:
        #     del metrics['targeted']["business_value"]
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


# @csrf_exempt
# def LatestOrdersTodayAPIView(request):
#     json_request = JSONParser().parse(request)
#     marketplace_id = json_request.get('marketplace_id', None)
#     product_id = json_request.get('product_id', [])
#     brand_id = json_request.get('brand_id', [])
#     manufacturer_name = json_request.get('manufacturer_name', [])
#     fulfillment_channel = json_request.get('fulfillment_channel',None)
#     # 1️⃣ Compute bounds for "today" based on the user's local timezone
#     user_timezone = json_request.get('timezone', 'US/Pacific')  # Default to US/Pacific if no timezone is provided
#     local_tz = timezone(user_timezone)

#     now = datetime.now(local_tz)
#     # For a 24-hour period ending now
#     start_of_day = now - timedelta(hours=24)
#     end_of_day = now

#     # 2️⃣ Fetch all Shipped/Delivered orders for the 24-hour period
#     match = dict()
#     match['order_date__gte'] = start_of_day
#     match['order_date__lte'] = end_of_day
#     match['order_status__in'] = ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']
#     if fulfillment_channel:
#         match['fulfillment_channel'] = fulfillment_channel
#     if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
#         match['marketplace_id'] = ObjectId(marketplace_id)

#     if manufacturer_name != None and manufacturer_name != "" and manufacturer_name != []:
#         ids = getproductIdListBasedonManufacture(manufacturer_name)
#         match["_id"] = {"$in": ids}

#     elif product_id != None and product_id != "" and product_id != []:
#         product_id = [ObjectId(pid) for pid in product_id]
#         ids = getOrdersListBasedonProductId(product_id)
#         match["id__in"] = ids

#     elif brand_id != None and brand_id != "" and brand_id != []:
#         brand_id = [ObjectId(bid) for bid in brand_id]
#         ids = getproductIdListBasedonbrand(brand_id)
#         match["id__in"] = ids

#     qs = DatabaseModel.list_documents(Order.objects,match)

#     # 3️⃣ Pre-fill a 24-slot OrderedDict for every hour in the time range
#     chart = OrderedDict()
#     bucket = start_of_day.replace(minute=0, second=0, microsecond=0)
#     for _ in range(25):  # 25 to include the current hour
#         key = bucket.strftime("%Y-%m-%d %H:00:00")
#         chart[key] = {"ordersCount": 0, "unitsCount": 0}
#         bucket += timedelta(hours=1)

#     # 4️⃣ Build the detail array + populate chart
#     orders_out = []
#     for order in qs:
#         # Convert order_date to user's timezone for consistent bucketing
#         order_local_time = order.order_date.astimezone(local_tz)
        
#         # hour bucket for this order
#         bk = order_local_time.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00:00")
        
#         # Only process if the bucket exists in our chart
#         if bk in chart:
#             chart[bk]["ordersCount"] += 1
#             try:
#                 # iterate each OrderItems instance referenced on this order
#                 for item in order.order_items:
#                     sku = item.ProductDetails.SKU
#                     asin = item.ProductDetails.ASIN if hasattr(item.ProductDetails, 'ASIN') and item.ProductDetails.ASIN is not None else ""
#                     qty = item.ProductDetails.QuantityOrdered
#                     unit_price = item.Pricing.ItemPrice.Amount
#                     title = item.ProductDetails.Title
#                     # lazy-load the Product doc for image_url
#                     prod_ref = item.ProductDetails.product_id
#                     img_url = prod_ref.image_url if prod_ref else None

#                     total_price = round(unit_price * qty, 2)
#                     purchase_dt = order_local_time.strftime("%Y-%m-%d %H:%M:%S")

#                     orders_out.append({
#                         "sellerSku": sku,
#                         "asin": asin,
#                         "title": title,
#                         "quantityOrdered": qty,
#                         "imageUrl": img_url,
#                         "price": total_price,
#                         "purchaseDate": purchase_dt
#                     })

#                     # add to units count
#                     chart[bk]["unitsCount"] += qty
#             except:
#                 pass

#     # 5️⃣ sort orders by most recent purchaseDate
#     orders_out.sort(key=lambda o: o["purchaseDate"], reverse=True)
    
#     # Convert chart to list format for easier frontend consumption
#     chart_list = [{"hour": hour, **data} for hour, data in chart.items()]
    
#     data = {
#         "orders": orders_out,
#         "hourly_order_count": chart_list
#     }
#     return data

@csrf_exempt
def LatestOrdersTodayAPIView(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    product_id = json_request.get('product_id', [])
    brand_id = json_request.get('brand_id', [])
    manufacturer_name = json_request.get('manufacturer_name', [])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    # 1️⃣ Compute bounds for "today" based on the user's local timezone
    user_timezone = json_request.get('timezone', 'US/Pacific')  # Default to US/Pacific if no timezone is provided
    local_tz = timezone(user_timezone)

    now = datetime.now(local_tz)
    # For a 24-hour period ending now
    start_of_day = now - timedelta(hours=24)
    end_of_day = now

    # 2️⃣ Fetch all Shipped/Delivered orders for the 24-hour period
    match = dict()
    match['order_date'] = {"$gte": start_of_day, "$lte": end_of_day}
    match['order_status'] = {"$in": ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']}
    if fulfillment_channel:
        match['fulfillment_channel'] = fulfillment_channel
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    if manufacturer_name != None and manufacturer_name != "" and manufacturer_name != []:
        ids = getproductIdListBasedonManufacture(manufacturer_name,start_of_day, end_of_day)
        match["_id"] = {"$in": ids}
    
    elif product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        ids = getOrdersListBasedonProductId(product_id,start_of_day, end_of_day)
        match["_id"] = {"$in": ids}

    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        ids = getproductIdListBasedonbrand(brand_id,start_of_day, end_of_day)
        match["_id"] = {"$in": ids}

    pipeline = [
            {
                "$match": match
            },
            {
                "$project": {
                    "_id" : 1,
                    "order_date": 1,
                    "order_items": 1
                }
            }
        ]
    qs = list(Order.objects.aggregate(*pipeline))


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
        order_local_time = order['order_date'].astimezone(local_tz)
        
        # hour bucket for this order
        bk = order_local_time.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:00:00")
        
        # Only process if the bucket exists in our chart
        if bk in chart:
            chart[bk]["ordersCount"] += 1
            try:
                # iterate each OrderItems instance referenced on this order
                
                item_pipeline = [
                    {"$match": {"_id": {"$in": order['order_items']}}},
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
                        "$group": {
                            "_id": {
                                "id" : "$product_ins._id",
                                "sku": {"$ifNull": ["$product_ins.sku", ""]},
                                "asin": {"$ifNull": ["$product_ins.product_id", ""]},
                                "title": "$product_ins.product_title",
                                "imageUrl": "$product_ins.image_url",
                                "purchaseDate": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d %H:%M:%S",
                                        "date": order_local_time
                                    }
                                }
                            },
                            "quantityOrdered": {"$sum": "$ProductDetails.QuantityOrdered"},
                            "unitPrice": {"$first": "$Pricing.ItemPrice.Amount"},
                            "totalPrice": {
                                "$sum": {
                                    "$multiply": ["$Pricing.ItemPrice.Amount", "$ProductDetails.QuantityOrdered"]
                                }
                            }
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id" : "$_id.id",
                            "sku": "$_id.sku",
                            "asin": "$_id.asin",
                            "title": "$_id.title",
                            "imageUrl": "$_id.imageUrl",
                            "quantityOrdered": "$quantityOrdered",
                            "unitPrice": "$unitPrice",
                            "totalPrice": {"$round": ["$totalPrice", 2]},
                            "purchaseDate": "$_id.purchaseDate"
                        }
                    }
                ]

                item_results = list(OrderItems.objects.aggregate(*item_pipeline))
                for item in item_results:
                    orders_out.append({
                        "id" : str(item["id"]),
                        "sellerSku": item["sku"],
                        "asin": item["asin"],
                        "title": item["title"],
                        "quantityOrdered": item["quantityOrdered"],
                        "imageUrl": item["imageUrl"],
                        "price": item["totalPrice"],
                        "purchaseDate": item["purchaseDate"]
                    })

                    # add to units count
                    chart[bk]["unitsCount"] += item["quantityOrdered"]

            except:
                pass

    # 5️⃣ sort orders by most recent purchaseDate
    orders_out.sort(key=lambda o: o["purchaseDate"], reverse=True)
    
    # Convert chart to list format for easier frontend consumption
    chart_list = [{"hour": hour, **data} for hour, data in chart.items()]
    
    data = {
        "orders": orders_out,
        "hourly_order_count": chart_list
    }
    return data

@csrf_exempt
def RevenueWidgetAPIView(request):
    from django.utils import timezone
    json_request = JSONParser().parse(request)
    preset = json_request.get("preset", "Today")
    compare_startdate = json_request.get("compare_startdate")
    compare_enddate = json_request.get("compare_enddate")
    marketplace_id = json_request.get("marketplace_id", None)
    product_id = json_request.get("product_id", None)
    brand_id = json_request.get("brand_id", None)
    manufacturer_name = json_request.get("manufacturer_name", None)
    fulfillment_channel = json_request.get("fulfillment_channel", None)

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        start_date, end_date = get_date_range(preset)

    comapre_past = get_previous_periods(start_date, end_date)

    # Use threading to fetch data concurrently

    def fetch_total():
        return totalRevenueCalculation(start_date, end_date, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)

    def fetch_graph_data():
        return get_graph_data(start_date, end_date, preset, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)

    def fetch_compare_total():
        return totalRevenueCalculation(compare_startdate, compare_enddate, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)

    def fetch_compare_graph_data():
        return get_graph_data(compare_startdate, compare_enddate, initial, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_total = executor.submit(fetch_total)
        future_graph_data = executor.submit(fetch_graph_data)

        compare_total = None
        compare_graph = None
        if compare_startdate != None and compare_startdate != "":
            compare_startdate = datetime.strptime(compare_startdate, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
            compare_enddate = datetime.strptime(compare_enddate, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=0)
            initial = "Today" if compare_startdate.date() == compare_enddate.date() else None

            future_compare_total = executor.submit(fetch_compare_total)
            future_compare_graph_data = executor.submit(fetch_compare_graph_data)

    # Wait for results
    total = future_total.result()
    graph_data = future_graph_data.result()

    if compare_startdate != None and compare_startdate != "":
        compare_total = future_compare_total.result()
        compare_graph = future_compare_graph_data.result()

    data = {
        "total": total,
        "graph": graph_data,
        "comapre_past": comapre_past
    }

    if compare_total:
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
        data['compare_graph'] = compare_graph

    # Apply filters based on chooseMatrix
    name = "Revenue"
    item_pipeline = [
        {"$match": {"name": name}}
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
        if item_result['refund_quantity'] == False:
            del data['total']["refund_quantity"]
        if item_result['refund_amount'] == False:
            del data['total']["refund_amount"]
        if item_result['net_profit'] == False:
            del data['total']["net_profit"]
        if item_result['profit_margin'] == False:
            del data['total']["profit_margin"]
        if item_result['orders'] == False:
            del data['total']["orders"]

    return data


@csrf_exempt
def get_top_products(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', None)
    product_id = json_request.get('product_id', None)
    metric = json_request.get("sortBy", "units_sold")  # 'price', 'refund', etc.
    preset = json_request.get("preset", "Today")  # today, yesterday, last_7_days

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
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
    match['order_status'] = {"$in": ['Shipped', 'Delivered','Acknowledged','Pending','Unshipped','PartiallyShipped']}
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if metric == "refund":
        match['order_status'] = "Refunded"
    if product_id != None and product_id != "" and product_id != []:
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
                "_id": "$product_ins._id",
                "id": {"$first": {"$toString":"$product_ins._id"}},
                "product": {"$first": "$product_ins.product_title"},
                "asin": {"$first": "$product_ins.product_id"},
                "sku": {"$first": "$product_ins.sku"},
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
                "id" : 1,
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


def getPreviousDateRange(start_date, end_date):

    duration = end_date - start_date
    previous_start_date = start_date - duration - timedelta(days=1)
    previous_end_date = start_date - timedelta(days=1)

    return previous_start_date.strftime("%Y-%m-%d"), previous_end_date.strftime("%Y-%m-%d")
   

@csrf_exempt
def get_products_with_pagination(request):
    json_request = JSONParser().parse(request)
    pipeline = list()
    match = dict()
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', None)
    product_id = json_request.get('product_id', None)
    manufacturer_name = json_request.get('manufacturer_name',[])
    page = int(json_request.get("page", 1))
    page_size = int(json_request.get("page_size", 10))
    preset = json_request.get("preset", "Today")
    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    parent = json_request.get('parent',True)
    sort_by = json_request.get('sort_by') 
    sort_by_value = json_request.get('sort_by_value')
    parent_search = json_request.get('parent_search')
    sku_search = json_request.get('sku_search')
    search_query = json_request.get('sku_search')
    


    if start_date != None and start_date != "":
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        start_date, end_date = get_date_range(preset)  
    today_start_date, today_end_date = get_date_range("Today")

    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    
    
    if product_id != None and product_id != "" and product_id != []:
        product_id = [ObjectId(pid) for pid in product_id]
        match["_id"] = {"$in": product_id}

    elif brand_id != None and brand_id != "" and brand_id != []:
        brand_id = [ObjectId(bid) for bid in brand_id]
        match["brand_id"] = {"$in": brand_id}

    elif manufacturer_name != None and manufacturer_name != "" and manufacturer_name != []:
        match["manufacturer_name"] = {"$in": manufacturer_name}


    if parent_search:
        match["parent_sku"] = {"$regex": parent_search, "$options": "i"}

    if sku_search:
        match["sku"] = {"$regex": sku_search, "$options": "i"}

    if search_query:
        match['product_title'] = {"$regex": search_query, "$options": "i"}

    if match != {}:
        pipeline.append({
            "$match": match
        })
    total_products = 0
    products = []
    if parent != None and parent != "" and parent == True:
        pipeline.extend([
            
                        {
                            "$group": {
                                "_id": 0,
                                "parent_sku_list" : {"$addToSet": "$parent_sku"},
                            }
                        },
                        {
                            "$project" : {
                                "_id" :0,
                                "parent_sku_list" : 1,
                            }
                        }
                
                        ])

        # Execute the pipeline
        result = list(Product.objects.aggregate(*pipeline))
        if result != []:
            sku_list = result[0]['parent_sku_list']
            total_products = len(sku_list)
            parent_sku = sku_list[((page - 1) ):(page-1)+10] 
            
            for ins in parent_sku:
                p_dict = {}
                stock = 0
                price_range = []
                cogs = 0
                pipeline = [
                    {"$match": {"parent_sku": ins}},
                    {
                        "$project": {
                            "_id": {"$toString": "$_id"},
                            "quantity": {"$ifNull": ["$quantity", 0]},
                            "price": {"$ifNull": ["$price", 0.0]},
                            "product_title": {"$ifNull": ["$product_title", ""]},
                            "image_url": {"$ifNull": ["$image_url", ""]},
                            "parent_sku": {"$ifNull": ["$parent_sku", ""]},
                            "marketplace_id": {"$ifNull": ["$marketplace_id", None]},
                            "total_cogs": {"$ifNull": ["$total_cogs", 0.0]},
                            "w_total_cogs": {"$ifNull": ["$w_total_cogs", 0.0]},
                            "category": {"$ifNull": ["$category", ""]}
                        }
                    },
                     {
                        "$lookup" : {
                        "from" : "marketplace",
                        "localField" : "marketplace_id",
                        "foreignField" : "_id",
                        "as" : "marketplace_ins"
                        }
                    },
                    {
                        "$unwind" : "$marketplace_ins"
                    },
                     {
                        "$project": {
                            "_id": 0,
                            "id": {"$toString": "$_id"},
                            "product_id" : {"$ifNull": ["$product_id", ""]},
                            "quantity": {"$ifNull": ["$quantity", 0]},
                            "price": {"$ifNull": ["$price", 0.0]},
                            "product_title": {"$ifNull": ["$product_title", ""]},
                            "image_url": {"$ifNull": ["$image_url", ""]},
                            "parent_sku": {"$ifNull": ["$parent_sku", ""]},
                            "marketplace_name": {"$ifNull": ["$marketplace_ins.name", ""]},
                            "total_cogs": {"$ifNull": ["$total_cogs", 0.0]},
                            "w_total_cogs": {"$ifNull": ["$w_total_cogs", 0.0]},
                            "category": {"$ifNull": ["$category", ""]},
                            "vendor_funding" : {"$ifNull" : ['$vendor_funding',0]}
                        }
                    }
                ]
                p_list = list(Product.objects.aggregate(*pipeline))
                p_exist = False
                total_salesForToday = 0
                total_unitsSoldForToday = 0
                total_grossRevenue = 0
                total_netprofit = 0
                total_margin = 0
                total_unitsSoldForPeriod = 0
                total_grossRevenueforPeriod = 0
                total_netProfitforPeriod = 0
                total_marginforPeriod = 0

                for p_ins in p_list:
                    current_sales_today = 0
                    current_units = 0
                    current_revenue = 0
                    current_netprofit = 0
                    current_margin = 0

                    previous_units = 0
                    previous_revenue = 0
                    previous_netprofit = 0
                    previous_margin = 0

                    stock += p_ins['quantity']
                    price_range.append(p_ins['price'])
                    if p_ins['marketplace_name'] == "Amazon":
                        temp_cogs = p_ins['total_cogs']
                        cogs += p_ins['total_cogs']
                    else:
                        cogs += p_ins['w_total_cogs']
                        temp_cogs = p_ins['w_total_cogs']


                    if p_exist == False:
                        p_dict = {
                            "id": str(p_ins['id']),
                            "title" : p_ins['product_title'],
                            "imageUrl" : p_ins['image_url'],
                            "parent_sku" : p_ins['parent_sku'],
                            "marketplace" : p_ins['marketplace_name'],
                            "category" : p_ins['category'],
                            "product_id" : p_ins['product_id']
                        }
                        p_exist = True




                    today_ins = getdaywiseproductssold(today_start_date, today_end_date,p_ins['id'],False)
                    for t_ins in today_ins:
                        current_sales_today += t_ins['total_price']
                    pr_ins = getdaywiseproductssold(start_date, end_date, p_ins['id'], False)
                    compare_start, compare_end = getPreviousDateRange(start_date, end_date)
                    compare_ins = getdaywiseproductssold(compare_start, compare_end, p_ins['id'], False)
                    # p_compare
                    for p in pr_ins:
                        current_units += p['total_quantity']
                        current_revenue += p['total_price']
                    current_revenue = round(current_revenue, 2)
                    current_netprofit = round(((current_revenue - (temp_cogs * current_units)) + (p_ins['vendor_funding'] * current_units)), 2)
                    current_margin = round((current_netprofit / current_revenue) * 100 if current_revenue > 0 else 0, 2)

                    for c in compare_ins:
                        previous_units += c['total_quantity']
                        previous_revenue += c['total_price']

                    
                    
                    previous_netprofit = ((previous_revenue - (temp_cogs * previous_units)) + (p_ins['vendor_funding'] * previous_units))
                    previous_margin = (previous_netprofit / previous_revenue) * 100 if previous_revenue > 0 else 0
                    previous_netprofit = round((previous_netprofit - current_netprofit), 2)
                    previous_margin = round((previous_margin - current_margin), 2)

                    previous_revenue = round((previous_revenue - current_revenue), 2)
                    previous_units = previous_units - current_units


                    total_salesForToday += current_sales_today
                    total_unitsSoldForToday += current_units
                    total_grossRevenue += current_revenue
                    total_netprofit += current_netprofit
                    total_margin += current_margin
                    total_unitsSoldForPeriod += previous_units
                    total_grossRevenueforPeriod += previous_revenue
                    total_netProfitforPeriod += previous_netprofit
                    total_marginforPeriod += previous_margin



                p_dict['sku_count'] = len(p_list)
                p_dict['stock'] = stock
                p_dict['price_start'] = min(price_range) if price_range else 0
                p_dict['price_end'] = max(price_range) if price_range else 0
                p_dict['cogs'] = cogs
                p_dict['salesForToday'] = total_salesForToday
                
                
                p_dict['unitsSoldForToday'] = total_unitsSoldForToday
                p_dict['unitsSoldForPeriod'] = total_unitsSoldForPeriod
                p_dict['refunds'] = 0
                p_dict['refundsforPeriod'] = 0
                p_dict['refundsAmount'] = 0
                p_dict['refundsAmountforPeriod'] = 0
                p_dict['grossRevenue'] = total_grossRevenue
                p_dict['grossRevenueforPeriod'] = total_grossRevenueforPeriod
                p_dict['netProfit'] = total_netprofit
                p_dict['netProfitforPeriod'] = total_netProfitforPeriod
                p_dict['margin'] = total_margin
                p_dict['marginforPeriod'] = total_marginforPeriod
                p_dict['totalchannelFees'] = cogs
                products.append(p_dict)


        response_data = {
            "total_products": total_products,
            "page": page,
            "page_size": page_size,
            "products": products,
            "tab_type" : "parent"
        }

    else:
        # Define the pipeline for pagination and data fetching
        if sort_by:
            sort_stage = {"$sort": {sort_by: int(sort_by_value)}}
            # Insert sort stage before $facet
            pipeline.append(sort_stage)
        pipeline.extend([
            {
            "$facet": {
                "total_count": [{"$count": "count"}],
                "products": [
                {"$skip": (page - 1) * page_size},  # Correct skip logic for pagination
                {"$limit": page_size},  # Limit based on page size
                {
                    "$lookup": {
                    "from": "marketplace",
                    "localField": "marketplace_id",
                    "foreignField": "_id",
                    "as": "marketplace_ins"
                    }
                },
                {
                    "$unwind": "$marketplace_ins"
                },
                {
                    "$project": {
                    "_id": 0,
                    "id": {"$toString": "$_id"},
                    "product_id": {"$ifNull": ["$product_id", "N/A"]},
                    "parent_sku": {"$ifNull": ["$sku", "N/A"]},
                    "imageUrl": {"$ifNull": ["$image_url", "N/A"]},
                    "title": {"$ifNull": ["$product_title", "N/A"]},
                    "marketplace": {"$ifNull": ["$marketplace_ins.name", "N/A"]},
                    "fulfillmentChannel": {
                        "$cond": {
                        "if": {"$eq": ["$fullfillment_by_channel", True]},
                        "then": "FBA",
                        "else": "FBM"
                        }
                    },
                    "price": {"$ifNull": [{"$round": ["$price", 2]}, "0.0"]},
                    "stock": {"$ifNull": ["$quantity", 0]},
                    "listingScore": {"$ifNull": ["$listing_quality_score", 0]},
                    "cogs": {
                        "$round": [
                        {
                            "$cond": {
                            "if": {"$eq": ["$marketplace_ins.name", "Amazon"]},
                            "then": {"$ifNull": ["$total_cogs", 0]},
                            "else": {"$ifNull": ["$w_total_cogs", 0]}
                            }
                        },
                        2
                        ]
                    },
                    "category": {"$ifNull": ["$category", "N/A"]},
                    "salesForToday": {"$ifNull": ["$salesForToday", 0]},
                    "salesForTodayPeriod": {"$ifNull": ["$unitsSoldForPeriod", 0]},
                    "unitsSoldForToday": {"$ifNull": ["$unitsSoldForToday", 0]},
                    "unitsSoldForPeriod": {"$ifNull": ["$unitsSoldForPeriod", 0]},
                    "refunds": {"$ifNull": ["$refunds", 0]},
                    "refundsforPeriod": {"$ifNull": ["$refunds", 0]},
                    "refundsAmount": {"$ifNull": ["$refundsAmount", 0]},
                    "refundsAmountforPeriod": {"$ifNull": ["$refundsAmount", 0]},
                    "grossRevenue": {"$ifNull": ["$grossProfit", 0]},
                    "grossRevenueforPeriod": {"$ifNull": ["$grossProfit", 0]},
                    "netProfit": {"$ifNull": ["$netProfit", 0]},
                    "netProfitforPeriod": {"$ifNull": ["$netProfit", 0]},
                    "margin": {"$ifNull": ["$margin", "0%"]},
                    "marginforPeriod": {"$ifNull": ["$margin", "0%"]},
                    "vendor_funding": {"$ifNull": ["$vendor_funding", 0]},
                    "totalchannelFees": {
                        "$round": [
                        {
                            "$cond": {
                            "if": {"$eq": ["$marketplace_ins.name", "Amazon"]},
                            "then": {"$sum": ["$referral_fee", "$a_shipping_cost"]},
                            "else": {"$sum": ["$walmart_fee", "$w_shiping_cost"]}
                            }
                        },
                        2
                        ]
                    },
                    }
                }
                ]
            }
            }
        ])

        # Execute the pipeline
        result = list(Product.objects.aggregate(*pipeline))

        # Extract total count and products
        total_products = result[0]["total_count"][0]["count"] if result[0]["total_count"] else 0
        products = result[0]["products"]
        def process_product(ins):
            today_ins = getdaywiseproductssold(today_start_date, today_end_date,ins['id'],False)
            for t_ins in today_ins:
                ins['salesForToday'] += t_ins['total_price']
            p_ins = getdaywiseproductssold(start_date, end_date, ins['id'], False)
            compare_start, compare_end = getPreviousDateRange(start_date, end_date)
            compare_ins = getdaywiseproductssold(compare_start, compare_end, ins['id'], False)
            # p_compare
            for p in p_ins:
                ins['unitsSoldForToday'] += p['total_quantity']
                ins['grossRevenue'] += p['total_price']
            ins['grossRevenue'] = round(ins['grossRevenue'], 2)
            ins['netprofit'] = round(((ins['grossRevenue'] - (ins['cogs'] * ins['unitsSoldForToday'])) + (ins['vendor_funding'] * ins['unitsSoldForToday'])), 2)
            ins['margin'] = round((ins['netprofit'] / ins['grossRevenue']) * 100 if ins['grossRevenue'] > 0 else 0, 2)

            previous_units = 0
            previous_revenue = 0
            previous_netprofit = 0
            previous_margin = 0
            for c in compare_ins:
                previous_units += c['total_quantity']
                previous_revenue += c['total_price']

            ins['unitsSoldForPeriod'] = previous_units
            ins['grossRevenueforPeriod'] = round((previous_revenue - ins['grossRevenue']), 2)
            previous_netprofit = ((previous_revenue - (ins['cogs'] * previous_units)) + (ins['vendor_funding'] * previous_units))
            previous_margin = (previous_netprofit / previous_revenue) * 100 if previous_revenue > 0 else 0
            ins['netProfitforPeriod'] = round((previous_netprofit - ins['netprofit']), 2)
            ins['marginforPeriod'] = round((previous_margin - ins['margin']), 2)

        threads = []
        for ins in products:
            thread = threading.Thread(target=process_product, args=(ins,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Prepare response data
        response_data = {
            "total_products": total_products,
            "page": page,
            "page_size": page_size,
            "products": products,
            "tab_type" : "sku"
        }

    return JsonResponse(response_data, safe=False)


########################--------------------------------------------------------------------------------------------------------##########

@csrf_exempt
def getPeriodWiseData(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id', [])
    manufacturer_name = json_request.get('manufacturer_name', [])
    fulfillment_channel = json_request.get('fulfillment_channel', None)

    def to_utc_format(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    def format_period_metrics(label, current_start, current_end, prev_start, prev_end):
        current_metrics = calculate_metricss(current_start, current_end, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)
        previous_metrics = calculate_metricss(prev_start, prev_end, marketplace_id, brand_id, product_id, manufacturer_name, fulfillment_channel)

        if label in ['Today', 'Yesterday']:
            period = {
                "current": { "from": to_utc_format(current_start) },
                "previous": { "from": to_utc_format(prev_start) }
            }
        else:
            period = {
                "current": { "from": to_utc_format(current_start), "to": to_utc_format(current_end) },
                "previous": { "from": to_utc_format(prev_start), "to": to_utc_format(prev_end) }
            }

        output = {
            "label": label,
            "period": period
        }

        for key in current_metrics:
            output[key] = {
                "current": current_metrics[key],
                "previous": previous_metrics[key],
                "delta": round(current_metrics[key] - previous_metrics[key], 2)
            }

        return output

    # Date ranges
    yes_current_start, yes_current_end = get_date_range("Yesterday")
    yes_previous_start = yes_current_start - timedelta(days=1)
    yes_previous_end = yes_current_start - timedelta(seconds=1)

    l7_current_start, l7_current_end = get_date_range("Last 7 days")
    l7_previous_start = l7_current_start - timedelta(days=7)
    l7_previous_end = l7_current_start - timedelta(seconds=1)

    l30_current_start, l30_current_end = get_date_range("Last 30 days")
    l30_previous_start = l30_current_start - timedelta(days=30)
    l30_previous_end = l30_current_start - timedelta(seconds=1)

    y_current_start, y_current_end = get_date_range("This Year")
    ly_current_start, ly_current_end = get_date_range("Last Year")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            "yesterday": executor.submit(format_period_metrics, "Yesterday", yes_current_start, yes_current_end, yes_previous_start, yes_previous_end),
            "last7Days": executor.submit(format_period_metrics, "Last 7 Days", l7_current_start, l7_current_end, l7_previous_start, l7_previous_end),
            "last30Days": executor.submit(format_period_metrics, "Last 30 Days", l30_current_start, l30_current_end, l30_previous_start, l30_previous_end),
            "yearToDate": executor.submit(format_period_metrics, "Year to Date", y_current_start, y_current_end, ly_current_start, ly_current_end),
        }
        response_data = {key: future.result() for key, future in futures.items()}

    return JsonResponse(response_data, safe=False)

@csrf_exempt
def getPeriodWiseDataXl(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id')
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id', [])
    manufacturer_name = json_request.get('manufacturer_name', [])
    fulfillment_channel = json_request.get('fulfillment_channel')

    # Define periods and date ranges
    periods = {
        "Yesterday": get_date_range("Yesterday"),
        "Last 7 Days": get_date_range("Last 7 days"),
        "Last 30 Days": get_date_range("Last 30 days"),
        "Month to Date": get_date_range("This Month"),
        "Year to Date": get_date_range("This Year"),
    }

    def create_row(label, start, end):
        data = calculate_metricss(
            start, end,
            marketplace_id,
            brand_id,
            product_id,
            manufacturer_name,
            fulfillment_channel,
            include_extra_fields=True
        )
        return [
            label,
            data.get("seller", ""),
            data.get("marketplace", ""),
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

    # Use threads to process all periods in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            label: executor.submit(create_row, label, start, end)
            for label, (start, end) in periods.items()
        }
        period_rows = [futures[label].result() for label in periods]

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

    # Write headers
    for col_num, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_num, value=header)
        cell.font = Font(bold=True)

    # Write data rows
    for row in period_rows:
        ws.append(row)

    # Auto-adjust column widths
    for col in ws.columns:
        max_length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col)
        col_letter = col[0].column_letter
        ws.column_dimensions[col_letter].width = max_length + 2

    # Return Excel as response
    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=PeriodWiseMetrics.xlsx'
    wb.save(response)
    return response

@csrf_exempt
def exportPeriodWiseCSV(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id', [])
    manufacturer_name = json_request.get('manufacturer_name', [])
    fulfillment_channel = json_request.get('fulfillment_channel', None)

    # Define time periods and their date ranges
    periods = {
        "Yesterday": get_date_range("Yesterday"),
        "Last 7 Days": get_date_range("Last 7 days"),
        "Last 30 Days": get_date_range("Last 30 days"),
        "Month to Date": get_date_range("This Month"),
        "Year to Date": get_date_range("This Year"),
    }

    def create_row(label, start, end):
        data = calculate_metricss(
            start, end,
            marketplace_id,
            brand_id,
            product_id,
            manufacturer_name,
            fulfillment_channel,
            include_extra_fields=True
        )

        return [
            label,
            data.get("seller", ""),
            data.get("marketplace", ""),
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            str(data["grossRevenue"]),
            str(data["expenses"]),
            str(data["netProfit"]),
            str(data["roi"]),
            str(data["unitsSold"]),
            str(data["refunds"]),
            str(data["skuCount"]),
            str(data["sessions"]),
            str(data["pageViews"]),
            str(data["unitSessionPercentage"]),
            str(data["margin"])
        ]

    # Parallel processing of metric calculations
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            label: executor.submit(create_row, label, start, end)
            for label, (start, end) in periods.items()
        }
        period_rows = [futures[label].result() for label in periods]

    headers = [
        "Period", "Seller", "Marketplace", "Start Date", "End Date",
        "Gross Revenue", "Expenses", "Net Profit", "ROI %",
        "Units Sold", "Refunds", "SKU Count", "Sessions",
        "Page Views", "Unit Session %", "Margin %"
    ]

    # Generate CSV response
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="PeriodWiseMetrics.csv"'
    writer = csv.writer(response)
    writer.writerow(headers)
    writer.writerows(period_rows)

    return response

def to_utc_format(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

@csrf_exempt
def getPeriodWiseDataCustom(request):
    json_request = JSONParser().parse(request)

    # Extract filters
    filters = {
        "marketplace_id": json_request.get("marketplace_id"),
        "brand_id": json_request.get("brand_id", []),
        "product_id": json_request.get("product_id", []),
        "manufacturer_name": json_request.get("manufacturer_name", []),
        "fulfillment_channel": json_request.get("fulfillment_channel")
    }

    preset = json_request.get("preset")
    start_date = json_request.get("start_date")
    end_date = json_request.get("end_date")

    # Determine custom or preset date range
    try:
        if start_date:
            from_date = datetime.strptime(start_date, "%Y-%m-%d")
            to_date = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            from_date, to_date = get_date_range(preset)
    except Exception:
        return JsonResponse({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

    # Compute previous period
    duration = to_date - from_date
    prev_from, prev_to = from_date - duration, to_date - duration

    # Get base periods
    today_start, today_end = get_date_range("Today")
    yesterday_start, yesterday_end = get_date_range("Yesterday")
    last7_start, last7_end = get_date_range("Last 7 days")
    last7_prev_start = today_start - timedelta(days=14)
    last7_prev_end = last7_start - timedelta(seconds=1)

    # Helper to generate period response
    def period_response(label, cur_from, cur_to, prev_from, prev_to):
        def format_metrics(metric):
            return {
                "current": current[metric],
                "previous": previous[metric],
                "delta": round(current[metric] - previous[metric], 2)
            }

        current = calculate_metricss(cur_from, cur_to, **filters)
        previous = calculate_metricss(prev_from, prev_to, **filters)

        date_ranges = {
            "current": {"from": to_utc_format(cur_from)},
            "previous": {"from": to_utc_format(prev_from)}
        }

        if label not in ['Today', 'Yesterday']:
            date_ranges["current"]["to"] = to_utc_format(cur_to)
            date_ranges["previous"]["to"] = to_utc_format(prev_to)

        summary_metrics = [
            "grossRevenue", "netProfit", "expenses", "unitsSold", "refunds", "skuCount",
            "sessions", "pageViews", "unitSessionPercentage", "margin", "roi", "orders"
        ]
        summary = {metric: format_metrics(metric) for metric in summary_metrics}

        def net_profit_calc(metrics):
            return {
                "gross": metrics["grossRevenue"],
                "totalCosts": metrics["expenses"],
                "productRefunds": metrics["refunds"],
                "totalTax": metrics.get("tax_price", 0),
                "totalTaxWithheld": 0,
                "ppcProductCost": 0,
                "ppcBrandsCost": 0,
                "ppcDisplayCost": 0,
                "ppcStCost": 0,
                "cogs": metrics.get("total_cogs", 0),
                "product_cost": metrics.get("product_cost", 0),
                "shipping_cost": metrics.get("shipping_cost", 0),
            }

        return {
            "dateRanges": date_ranges,
            "summary": summary,
            "netProfitCalculation": {
                "current": net_profit_calc(current),
                "previous": net_profit_calc(previous),
            }
        }

    response_data = {
        "today": period_response("Today", today_start, today_end, yesterday_start, yesterday_end),
        "yesterday": period_response("Yesterday", yesterday_start, yesterday_end, yesterday_start - timedelta(days=1), yesterday_end - timedelta(days=1)),
        "last7Days": period_response("Last 7 Days", last7_start, last7_end, last7_prev_start, last7_prev_end),
        "custom": period_response(preset, from_date, to_date, prev_from, prev_to),
    }

    return JsonResponse(response_data, safe=False)


@csrf_exempt
def allMarketplaceData(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)


    def grouped_marketplace_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        orders = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
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
            temp_price = 0
            vendor_funding = 0

            sku_set = set()

            m_obj = Marketplace.objects(id=mp_id)
            marketplace = m_obj[0].name if m_obj else ""
            
    
            for order in orders:
                gross_revenue += order["order_total"]
                order_total = order["order_total"]
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
                                "sku": "$product_ins.sku",
                                "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                                "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                                "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        if order['marketplace_name'] == "Amazon":
                            total_cogs += item_data['total_cogs']
                        else:
                            total_cogs += item_data['w_total_cogs']
                        vendor_funding += item_data['vendor_funding'] 
                        total_product_cost += item_data['price']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            # other_price += order_total - temp_price - tax_price

            expenses = total_cogs 
            net_profit = (temp_price - expenses) + vendor_funding
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
        return [
                {
                    "image": (
                        "https://i.pinimg.com/originals/01/ca/da/01cada77a0a7d326d85b7969fe26a728.jpg"
                        if marketplace == "Amazon" else
                        "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcRzjtf8dzq48TtkzeRYx2-_li3gTCkstX2juA&s"
                        if marketplace == "Walmart" else ""
                    ),
                    "marketplace": marketplace,
                    "currency_list": data["currency_list"]
                }
                for marketplace, data in marketplace_metrics.items()
            ]


    def calculate_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        temp_price = 0
        sku_set = set()

        result = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        order_total = 0
        other_price = 0
        tax_price = 0
        vendor_funding = 0

        if result:
            for order in result:
                gross_revenue += order['order_total']
                order_total = order['order_total']
                
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
                                "sku": "$product_ins.sku",
                                "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                                "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                                "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        if order['marketplace_name'] == "Amazon":
                            total_cogs += item_data['total_cogs']
                        else:
                            total_cogs += item_data['w_total_cogs']
                        vendor_funding += item_data['vendor_funding']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            # other_price += order_total - temp_price - tax_price

            net_profit = (temp_price - total_cogs) + vendor_funding
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (total_cogs)) * 100, 2) if total_cogs > 0 else 0,
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

    def create_period_response(label, cur_from, cur_to, prev_from, prev_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        current = calculate_metrics(cur_from, cur_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        previous = calculate_metrics(prev_from, prev_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

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
            "marketplace_list": grouped_marketplace_metrics(cur_from, cur_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        }

    current_date = datetime.now()
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration

    response_data = {
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel),
        "from_date":from_date,
        "to_date":to_date
    }

    return JsonResponse(response_data, safe=False)

#------------------------------------------------------------
@csrf_exempt
def getProductPerformanceSummary(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)

    orders = grossRevenue(from_date, to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
    
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
        fulfillmentChannel = ""
        temp_price = 0.0
        total_cogs = 0.0
        sku_set = set()
        tax_price = 0
        vendor_funding = 0
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
                        "fulfillmentChannel": {
                            "$cond": {
                            "if": {"$eq": ["$product_ins.fullfillment_by_channel", True]},
                            "then": "FBA",
                            "else": "FBM"
                            }
                            },
                        "images": "$product_ins.image_url",
                        "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                        "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                        "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
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
                if order['marketplace_name'] == "Amazon":
                    cogs = item_data.get("total_cogs", 0.0) + item_data.get("vendor_funding", 0.0)
                else:
                    cogs = item_data.get("w_total_cogs", 0.0) + item_data.get("vendor_funding", 0.0)
                 
                temp_price += price
                total_cogs += cogs
                fulfillmentChannel = item_data.get("fulfillmentChannel", 0.0)
                if sku:
                    sku_set.add(sku)

                    sku_summary[sku]["sku"] = sku
                    sku_summary[sku]["product_name"] = product_name
                    sku_summary[sku]["images"] = images
                    sku_summary[sku]["unitsSold"] += 1
                    sku_summary[sku]["grossRevenue"] += price
                    sku_summary[sku]["totalCogs"] += cogs
                    sku_summary[sku]["fulfillmentChannel"] = fulfillmentChannel

        # other_price = order_total - temp_price - tax_price

        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            net_profit = gross - cogs
            margin = (net_profit / gross) * 100 if gross > 0 else 0
            sku_summary[sku]["netProfit"] = round(net_profit, 2)
            sku_summary[sku]["margin"] = round(margin, 2)

    sorted_skus = sorted(sku_summary.values(), key=lambda x: x["unitsSold"], reverse=True)
    top_3 = sorted_skus[:3]
    least_3 = sorted_skus[-3:] if len(sorted_skus) >= 3 else sorted_skus

    return JsonResponse({
        "top_3_products": top_3,
        "least_3_products": least_3
    })

@csrf_exempt
def downloadProductPerformanceSummary(request):
    action = request.GET.get("action", "").lower()
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)

    orders = grossRevenue(from_date, to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
 
    sku_summary = defaultdict(lambda: {
        "sku": "",
        "product_name": "",
        "images": "",
        "unitsSold": 0,
        "grossRevenue": 0.0,
        "totalCogs": 0.0,
        "netProfit": 0.0,
        "margin": 0.0,
        "Trend" :"",
        "vendor_funding" : 0      
    })
 
    for order in orders:
        order_total = order.get("order_total", 0.0)
        item_ids = order.get("order_items", [])
        m_name = order.get("marketplace_name", "")
        fulfillment_channel = ""
        temp_price = 0.0
        total_cogs = 0.0
        tax_price = 0
        vendor_funding = 0
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
                        "tax_price": "$Pricing.ItemPrice.tax_price",
                        "cogs": {"$ifNull": ["$product_ins.cogs", 0.0]},
                        "sku": "$product_ins.sku",
                        "fulfillmentChannel": {
                            "$cond": {
                            "if": {"$eq": ["$product_ins.fullfillment_by_channel", True]},
                            "then": "FBA",
                            "else": "FBM"
                            }
                            },
                        "product_name": "$product_ins.product_title",
                        "images": "$product_ins.image_urls",
                        "asin": "$product_ins.asin",
                        "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                        "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                        "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
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
                if order['marketplace_name'] == "Amazon":
                    cogs = item_data.get("total_cogs", 0.0)
                else:
                    cogs = item_data.get("w_total_cogs", 0.0)
                temp_price += price
                total_cogs += cogs
                vendor_funding += item_data.get("vendor_funding", 0.0)
                fulfillment_channel = item_data.get("fulfillmentChannel", 0.0)
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
                    sku_summary[sku]['vendor_funding'] += vendor_funding
 
        # other_price = order_total - temp_price - tax_price
 
        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            vendor_funding = sku_summary[sku]['vendor_funding']
            net_profit = (gross - cogs) + vendor_funding
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
 
 
@csrf_exempt
def downloadProductPerformanceCSV(request):
    action = request.GET.get("action", "").lower()
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)

    orders = grossRevenue(from_date, to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
 
    sku_summary = defaultdict(lambda: {
        "sku": "",
        "product_name": "",
        "images": "",
        "unitsSold": 0,
        "grossRevenue": 0.0,
        "totalCogs": 0.0,
        "netProfit": 0.0,
        "margin": 0.0,
        "Trend":"",
        "vendor_funding" : 0
    })
 
    for order in orders:
        order_total = order.get("order_total", 0.0)
        item_ids = order.get("order_items", [])
        fulfillment_channel = ""
        temp_price = 0.0
        total_cogs = 0.0
        tax_price = 0
        vendor_funding = 0
        sku_set = set()
        m_name = order.get("marketplace_name", "")

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
                        "fulfillmentChannel": {
                            "$cond": {
                            "if": {"$eq": ["$product_ins.fullfillment_by_channel", True]},
                            "then": "FBA",
                            "else": "FBM"
                            }
                            },
                        "asin": "$product_ins.asin",
                        "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                        "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                        "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
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
                if order['marketplace_name'] == "Amazon":
                    cogs = item_data.get("total_cogs", 0.0)
                else:
                    cogs = item_data.get("w_total_cogs", 0.0)
                temp_price += price
                total_cogs += cogs
                vendor_funding += item_data.get("vendor_funding", 0.0)
                fulfillment_channel = item_data.get("fulfillmentChannel", 0.0)
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
                    sku_summary[sku]['vendor_funding'] += vendor_funding
 
        # other_price = order_total - temp_price - tax_price
 
        for sku in sku_set:
            gross = sku_summary[sku]["grossRevenue"]
            cogs = sku_summary[sku]["totalCogs"]
            vendor_funding = sku_summary[sku]['vendor_funding']
            net_profit = (gross - cogs) + vendor_funding
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
 


@csrf_exempt
def allMarketplaceDataxl(request):
    # from_str = request.GET.get("from_date")
    # to_str = request.GET.get("to_date")

    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)

    # try:
    #     from_date = datetime.strptime(from_str, "%Y-%m-%d")
    #     to_date = datetime.strptime(to_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    # except:
    #     to_date = datetime.now()
    #     from_date = to_date - timedelta(days=30)

    def grouped_marketplace_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        orders = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
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
            temp_price = 0
            vendor_funding = 0
            sku_set = set()

            m_obj = Marketplace.objects(id=mp_id)
            marketplace = m_obj[0].name if m_obj else ""

            for order in orders:
                gross_revenue += order["order_total"]
                order_total = order["order_total"]
                
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
                                "sku": "$product_ins.sku",
                                "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                                "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                                "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        if order['marketplace_name'] == "Amazon":
                            total_cogs += item_data['total_cogs'] 
                        else:
                            total_cogs += item_data['w_total_cogs']
                        vendor_funding += item_data['vendor_funding']
                        total_product_cost += item_data['price']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            # other_price += order_total - temp_price - tax_price

            expenses = total_cogs
            net_profit = (temp_price - expenses) + vendor_funding
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
    data_rows = grouped_marketplace_metrics(from_date, to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

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

@csrf_exempt
def downloadMarketplaceDataCSV(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)


    def grouped_marketplace_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        orders = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
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
            vendor_funding = 0
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
                                "sku": "$product_ins.sku",
                                "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                                "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                                "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        if order['marketplace_name'] == "Amazon":
                            total_cogs += item_data['total_cogs'] 
                        else:
                            total_cogs += item_data['w_total_cogs']
                        vendor_funding += item_data['vendor_funding']
                        total_product_cost += item_data['price']
                        total_units += 1
                        if item_data.get('sku'):
                            sku_set.add(item_data['sku'])

            # other_price += order_total - temp_price - tax_price

            expenses = total_cogs 
            net_profit = (total_product_cost - expenses) + vendor_funding
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
    metrics = grouped_marketplace_metrics(from_date, to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

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

@csrf_exempt
def getCitywiseSales(request):
    json_request = JSONParser().parse(request)
    level = json_request.get("level", "city").lower()  
    action = json_request.get("action", "all").lower()  
    preset = json_request.get("preset", "Yesterday")
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        yesterday_start = datetime.strptime(start_date, '%Y-%m-%d')
        yesterday_end = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        yesterday_start, yesterday_end = get_date_range(preset)


    orders = grossRevenue(yesterday_start, yesterday_end,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)


    grouped_data = defaultdict(lambda: {"units": 0, "gross": 0.0, "city": "", "state": "", "country": ""})

    for entry in orders:
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



@csrf_exempt
def exportCitywiseSalesExcel(request):
    json_request = JSONParser().parse(request)
    level = json_request.get("level", "city").lower()  
    action = json_request.get("action", "all").lower()  
    preset = json_request.get("preset", "Yesterday")
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        yesterday_start = datetime.strptime(start_date, '%Y-%m-%d')
        yesterday_end = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        yesterday_start, yesterday_end = get_date_range(preset)

    orders = grossRevenue(yesterday_start, yesterday_end,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

    grouped_data = defaultdict(lambda: {"units": 0, "gross": 0.0, "city": "", "state": "", "country": ""})

    for entry in orders:
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
                    key = f"{geo.city}|{geo.state_id}|USA"
                    city_population[key] += geo.population
                if level in ["state"]:
                    key = f"{geo.state_id}|USA"
                    state_population[key] += geo.population
                if level == "country":
                    country_population['USA'] += geo.population
    data_rows = []
    headers = ["Date From", "Date To", "Country", "Gross Revenue", "Units Sold"]
    for key, values in grouped_data.items():
        row = [yesterday_end.strftime("%b %d, %Y"), yesterday_start.strftime("%b %d, %Y")]

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

@csrf_exempt
def downloadCitywiseSalesCSV(request):
    json_request = JSONParser().parse(request)
    level = json_request.get("level", "city").lower()  
    action = json_request.get("action", "all").lower()  
    preset = json_request.get("preset", "Yesterday")
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        yesterday_start = datetime.strptime(start_date, '%Y-%m-%d')
        yesterday_end = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        yesterday_start, yesterday_end = get_date_range(preset)

    orders = grossRevenue(yesterday_start, yesterday_end,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
    grouped_data = defaultdict(lambda: {"units": 0, "gross": 0.0, "city": "", "state": "", "country": ""})

    for entry in orders:
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
        row = [yesterday_end.strftime("%b %d, %Y"), yesterday_start.strftime("%b %d, %Y")]

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
    # other_price = 0
    tax_price = 0
    temp_price = 0
    vendor_funding = 0
    if result:
        for order in result:
            gross_revenue += order['order_total']
            order_total = order['order_total']
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
                            "category": "$product_ins.category",
                            "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                            "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                            "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item_data = item_result[0]
                    temp_price += item_data['price']
                    tax_price += item_data['tax_price']
                    if order['marketplace_name'] == "Amazon":
                        total_cogs += item_data['total_cogs'] 
                    else:
                        total_cogs += item_data['w_total_cogs']
                    vendor_funding += item_data['vendor_funding']
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

        # other_price += order_total - temp_price - tax_price
        net_profit = (temp_price -  total_cogs) + vendor_funding
        margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0

    return {
        "grossRevenue": round(gross_revenue, 2),
        "expenses": round(total_cogs, 2),
        "netProfit": round(net_profit, 2),
        "roi": round((net_profit / (total_cogs)) * 100, 2) if total_cogs > 0 else 0,
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



@csrf_exempt
def getProfitAndLossDetails(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)
    
    def calculate_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        gross_revenue = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        product_categories = {}
        product_completeness = {"complete": 0, "incomplete": 0}

        result = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        order_total = 0
        # other_price = 0
        tax_price = 0
        temp_price = 0
        vendor_funding = 0
        if result:
            for order in result:
                gross_revenue += order['order_total']
                order_total = order['order_total']
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
                                "category": "$product_ins.category",
                                "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                            "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                            "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                            }
                        }
                    ]
                    item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                    if item_result:
                        item_data = item_result[0]
                        temp_price += item_data['price']
                        tax_price += item_data['tax_price']
                        if order['marketplace_name'] == "Amazon":
                            total_cogs += item_data['total_cogs'] 
                        else:
                            total_cogs += item_data['w_total_cogs']
                        vendor_funding += item_data['vendor_funding']
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
                        if item_data['price'] and item_data['total_cogs'] and item_data['sku']:
                            product_completeness["complete"] += 1
                        else:
                            product_completeness["incomplete"] += 1

            # other_price += order_total - temp_price - ta x_price
            net_profit = (temp_price - total_cogs) + vendor_funding
            margin = (net_profit / gross_revenue) * 100 if gross_revenue > 0 else 0
            
        return {
            "grossRevenue": round(gross_revenue, 2),
            "expenses": round((total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (total_cogs)) * 100, 2) if total_cogs > 0 else 0,
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
            "productCompleteness": product_completeness,  # Added product completeness data
            'base_price':temp_price,
        }

    def create_period_response(label, cur_from, cur_to, prev_from, prev_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel,preset):
        current = calculate_metrics(cur_from, cur_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        previous = calculate_metrics(prev_from, prev_to,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

        def with_delta(metric):
            return {
                "current": current[metric],
                "previous": previous[metric],
                "delta": round(current[metric] - previous[metric], 2)
            }
        if preset in ['Today', 'Yesterday']:
            return {
                "dateRanges": {
                    "current": {"from": to_utc_format(cur_from),},
                    "previous": {"from": to_utc_format(prev_from),}
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
                        "base_price": current["base_price"],
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
                        "base_price": current["base_price"],
                        "shipping_cost": previous["shipping_cost"],
                    }
                },
                "charts": {
                    "productDistribution": current["productCategories"],  # Bar chart data
                    "productCompleteness": current["productCompleteness"]  # Pie chart data
                }
            }
        else:
            return {
                "dateRanges": {
                    "current": {"from": to_utc_format(cur_from),"to": to_utc_format(cur_to)},
                    "previous": {"from": to_utc_format(prev_from),"to": to_utc_format(prev_to)}
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
                        "base_price": current["base_price"],
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
                        "base_price": current["base_price"],
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

    
    
    custom_duration = to_date - from_date
    prev_from_date = from_date - custom_duration
    prev_to_date = to_date - custom_duration

    response_data = {
        "custom": create_period_response("Custom", from_date, to_date, prev_from_date, prev_to_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel,preset),
    }

    return JsonResponse(response_data, safe=False)


@csrf_exempt
def profit_loss_chart(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset')

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)
    def get_month_range(year, month):
        start_date = datetime(year, month, 1)
        last_day = monthrange(year, month)[1]
        end_date = datetime(year, month, last_day, 23, 59, 59)
        return start_date, end_date


    def calculate_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
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
        # other_price = 0
        tax_price = 0
        temp_price = 0
        vendor_funding = 0

        result = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        for order in result:
            gross_revenue_amt += order.get("order_total", 0)
            order_total = order.get("order_total", 0)
            # temp_price = 0
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
                            "category": "$product_ins.category",
                            "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                            "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                            "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item = item_result[0]
                    temp_price += item.get("price", 0)
                    tax_price += item.get("tax_price", 0)
                    
                    if order['marketplace_name'] == "Amazon":
                        total_cogs += item.get("total_cogs", 0) 
                    else:
                        total_cogs += item.get("w_total_cogs", 0)
                    vendor_funding += item.get("vendor_funding", 0)
                    total_units += 1
                    sku = item.get("sku")
                    if sku:
                        sku_set.add(sku)
                    category = item.get("category", "Unknown")
                    product_categories[category] = product_categories.get(category, 0) + 1
                    if item.get("price") and item.get("total_cogs") and sku:
                        product_completeness["complete"] += 1
                    else:
                        product_completeness["incomplete"] += 1

            # other_price += order_total - temp_price - tax_price

        net_profit = (temp_price - total_cogs) + vendor_funding
        margin = (net_profit / gross_revenue_amt * 100) if gross_revenue_amt else 0

        return {
            "grossRevenue": round(gross_revenue_amt, 2),
            "expenses": round((total_cogs) , 2),
            "netProfit": round(net_profit, 2),
            "roi": round((net_profit / (total_cogs)) * 100, 2) if (total_cogs) else 0,
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

        data = calculate_metrics(start, end,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

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


@csrf_exempt
def profitLossExportXl(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset', "Last 30 days")

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)
    def get_month_range(year, month):
        start_date = datetime(year, month, 1)
        last_day = monthrange(year, month)[1]
        end_date = datetime(year, month, last_day, 23, 59, 59)
        return start_date, end_date

    def calculate_metrics(start_date, end_date):
        gross_revenue_amt = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        order_total = 0
        # other_price = 0
        tax_price = 0
        temp_price = 0
        vendor_funding = 0
        m_name = ""
        result = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        for order in result:
            gross_revenue_amt += order.get("order_total", 0)
            order_total = order.get("order_total", 0)
            # temp_price = 0
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
                            "sku": "$product_ins.sku",
                            "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                            "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                            "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item = item_result[0]
                    temp_price += item.get("price", 0)
                    tax_price += item.get("tax_price", 0)
                    if order['marketplace_name'] == "Amazon":
                        total_cogs += item.get("total_cogs", 0) 
                    else:
                        total_cogs += item.get("w_total_cogs", 0)
                    vendor_funding += item.get("vendor_funding", 0)
                    total_units += 1
                    sku = item.get("sku")
                    if sku:
                        sku_set.add(sku)

            # other_price += order_total - temp_price - tax_price

        net_profit = (temp_price - total_cogs) + vendor_funding
        margin = (net_profit / gross_revenue_amt * 100) if gross_revenue_amt else 0

        return {
            "Marketplace":m_name,
            "Date and Time":start_date,
            "Gross Revenue": round(gross_revenue_amt, 2),
            "Expenses": round((total_cogs) , 2),
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

        row_data = calculate_metrics(start, end,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

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

@csrf_exempt
def profitLossChartCsv(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id', None)
    brand_id = json_request.get('brand_id', [])
    product_id = json_request.get('product_id',[])
    manufacturer_name = json_request.get('manufacturer_name',[])
    fulfillment_channel = json_request.get('fulfillment_channel',None)
    preset = json_request.get('preset',"Last 7 days")

    start_date = json_request.get("start_date", None)
    end_date = json_request.get("end_date", None)
    if start_date != None and start_date != "":
        from_date = datetime.strptime(start_date, '%Y-%m-%d')
        to_date = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        from_date, to_date = get_date_range(preset)
        
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
    
    def dummy_calculate_metrics(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel):
        gross_revenue_amt = 0
        total_cogs = 0
        refund = 0
        net_profit = 0
        margin = 0
        total_units = 0
        sku_set = set()
        order_total = 0
        # other_price = 0
        tax_price = 0
        vendor_funding  =0 
        temp_price = 0
        m_name = ""
        result = grossRevenue(start_date, end_date,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)
        for order in result:
            gross_revenue_amt += order.get("order_total", 0)
            order_total = order.get("order_total", 0)
            # temp_price = 0
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
                            "sku": "$product_ins.sku",
                            "total_cogs" : {"$ifNull":["$product_ins.total_cogs",0]},
                            "w_total_cogs" : {"$ifNull":["$product_ins.w_total_cogs",0]},
                            "vendor_funding" : {"$ifNull":["$product_ins.vendor_funding",0]},
                        }
                    }
                ]
                item_result = list(OrderItems.objects.aggregate(*item_pipeline))
                if item_result:
                    item = item_result[0]
                    temp_price += item.get("price", 0)
                    tax_price += item.get("tax_price", 0)
                    if order['marketplace_name'] == "Amazon":
                        total_cogs += item.get("total_cogs", 0) 
                    else:
                        total_cogs += item.get("w_total_cogs", 0)
                    vendor_funding += item.get("vendor_funding", 0)
                    total_units += 1
                    sku = item.get("sku")
                    if sku:
                        sku_set.add(sku)

            # other_price += order_total - temp_price - tax_price

        net_profit = (temp_price - total_cogs) + vendor_funding
        margin = (net_profit / gross_revenue_amt * 100) if gross_revenue_amt else 0

        return {
            "Marketplace":m_name,
            "Date and Time":start_date,
            "Gross Revenue": round(gross_revenue_amt, 2),
            "Expenses": round((total_cogs) , 2),
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

        data = dummy_calculate_metrics(start, end,marketplace_id,brand_id,product_id,manufacturer_name,fulfillment_channel)

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
    
    if name == "Today Snapshot":
        if 'select_all' in json_req and json_req['select_all'] == True:
            update_fields = {
            'select_all': json_req['select_all'], 'gross_revenue': True,
                'total_cogs': True,
                'profit_margin': True,
                'orders': True,
                'units_sold': True,
                'refund_quantity': True,}
        else:
            update_fields = {
                'select_all': False,
            'gross_revenue': json_req['gross_revenue'],
            'total_cogs': json_req['total_cogs'],
            'profit_margin': json_req['profit_margin'],
            'orders': json_req['orders'],
            'units_sold': json_req['units_sold'],
            'refund_quantity': json_req['refund_quantity'],
            }
    elif name == "Revenue":
        if 'select_all' in json_req and json_req['select_all'] == True:
            update_fields = {
                'select_all': json_req['select_all'], 'gross_revenue': True,
                'units_sold': True,
                'acos': True,
                'tacos': True,
                'refund_quantity': True,
                'net_profit': True,
                'profit_margin': True,
                'refund_amount': True,
                'roas': True,
                'orders': True,
                'ppc_spend': True
            }
        else:
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
    listing_optimization_alerts = []
    product_performance_alerts = []  # New alert list for Product Performance
    
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

    def get_image_alerts(product):
        alerts = []
        images = product.image_urls or []
        if not images:
            alerts.append("No main image found. Please upload a clear product image with a white background.")
        else:
            main_image = images[0]
            if not (main_image.endswith('.jpg') or main_image.endswith('.jpeg') or main_image.endswith('.png')):
                alerts.append("Main image format should be JPG or PNG for better clarity and compatibility.")
            if 'white' not in main_image.lower():
                alerts.append("Update your main image background to white. This enhances your product's visual appeal and professionalism while meeting Amazon's requirements.")
            if 'small' in main_image.lower() or 'thumbnail' in main_image.lower():
                alerts.append("Update your main image size so that it is clear and of high quality for your potential customers.")
        return alerts

    Refund_obj = Refund.objects()
    # refunded_product_ids = list(set([i.product_id.id for i in Refund_obj]))
    refunded_product_ids = list(set([i.id for i in all_products][:2]))
    for product_id in refunded_product_ids:
        product = Product.objects(id=product_id).first()
        if not product:
            continue

        if is_optimized(product):
            optimized_count += 1

        alerts = get_image_alerts(product)
        if alerts:
            listing_optimization_alerts.append({
                "product_id": str(product.id),
                "title": product.product_title,
                "messages": alerts
            })

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
        else:
            refund_rate = 0 
        if refund_rate > 6:
            refund_alerts.append({
                "product_id": str(product.id),
                "title": product.product_title,
                "refund_rate": round(refund_rate, 2),
                "message": f"{product.product_title} has exceeded a 6% refund rate. Refund rates are soaring, impacting your profits. Review, analyze, and revise now."
            })

        # **New alert for Product Performance if refund rate has decreased by 6% or more**
        if refund_rate <= 6:
            product_performance_alerts.append({
                "product_id": str(product.id),
                "title": product.product_title,
                "refund_rate": round(refund_rate, 2),
                "message": f"Refund rates for {product.product_title} have decreased by an impressive 6% or more. Your dedication is driving results, it’s time to take a closer look at your strategy."
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

    if this_month_fees > last_month_fees:
        increase = round(this_month_fees - last_month_fees, 2)
        fee_alerts.append({
            "marketplace": "amazon.com",
            "increase_amount": increase,
            "message": f"Amazon Storage fees have increased by ${increase} for amazon.com. Storage fees have increased, cutting into your profit margins. Consider optimizing your inventory or fulfillment strategies now."
        })

    inventory_alerts = []

    for product in all_products:
        # Safely access the attribute using getattr
        days_remaining = getattr(product, 'days_of_inventory_remaining', 999)

        if days_remaining <= 45:
            reorder_by_date = datetime.date.today() + datetime.timedelta(days=days_remaining)

            if days_remaining <= 38:
                alert_message = f"Order more inventory now to avoid running out of stock. You have {days_remaining} days of inventory remaining."
            else:
                alert_message = f"Order more inventory by {reorder_by_date.strftime('%B %d, %Y')} to avoid running out of stock. You have {days_remaining} days of inventory remaining."

            inventory_alerts.append({
                "title": getattr(product, 'title', ''),
                "asin": getattr(product, 'asin', ''),
                "sku": getattr(product, 'sku', ''),
                "fulfillment_channel": getattr(product, 'fulfillment_channel', ''),
                "days_left": days_remaining,
                "reorder_by": reorder_by_date.isoformat(),
                "inventory_alert": alert_message
            })

    return JsonResponse({
    "total_products": total_products,
    "listing_optimization": {
        "optimized_products": optimized_count,
        "not_optimized_products": total_products - optimized_count
    },
    "insights_by_category": {
        "Listing Optimization": len(listing_optimization_alerts),
        "Product Performance": len(refund_alerts) + len(product_performance_alerts),
        "Inventory": len(fee_alerts) + len(inventory_alerts),
        "Refunds": len(refund_alerts),
        "Keyword": 42  # Placeholder until real keyword logic is added
    },
    "alerts_feed": [  # 🆕 Unified feed-style list for UI rendering
        *[
            {
                "type": "Listing Optimization",
                "title": alert["title"],
                "date": datetime.today(),  # Optional: Add if you track creation date
                "message": msg
            }
            for alert in listing_optimization_alerts
            for msg in alert["messages"]
        ],
        *[
            {
                "type": "Refunds",
                "title": alert["title"],
                "date":  datetime.today(),
                "message": alert["message"]
            }
            for alert in refund_alerts
        ],
        *[
            {
                "type": "Product Performance",
                "title": alert["title"],
                "date":  datetime.today(),
                "message": alert["message"]
            }
            for alert in product_performance_alerts
        ],
        *[
            {
                "type": "Inventory",
                "title": alert["title"],
                "date":  datetime.today(),
                "message": alert["inventory_alert"]
            }
            for alert in inventory_alerts
        ],
        *[
            {
                "type": "Inventory",
                "title": "Storage Fee Alert",
                "date":  datetime.today(),
                "message": alert["message"]
            }
            for alert in fee_alerts
        ],
        *[
            # {
            #     "type": "Keyword",
            #     "title": "Keyword Ranking",
            #     "date": "2024-11-30",
            #     "message": "Your keyword “nuriva memory pill” went from page 1, position 8, to position 13 over the past 4 days. Review the listing for issues or review your PPC campaigns."
            # },
            # {
            #     "type": "Keyword",
            #     "title": "Keyword Ranking",
            #     "date": "2024-11-24",
            #     "message": "Your keyword “neuriva brain supplement” went from page 1, position 9, to position 11 over the past 4 days. Review the listing for issues or review your PPC campaigns."
            # }
        ]
    ]
})




###########################-----MY Products APIS-----##############################


def productsDetailsPageSummary(request):
    product_id = request.GET.get('product_id')
    pipeline = [
        {
            "$match": {
                "_id": ObjectId(product_id)
            }
        },
        {
            "$project": {
                "_id": 0,
                "sku": "$sku",
                "asin" : {"$ifNull" : ["$product_id",""]},
                "product_title" : {"$ifNull" : ["$product_title",""]},
                "image_url" : {"$ifNull" : ["$image_url",""]},

                "price" : {"$ifNull" : ["$price",0.0]},
                "stock" : {"$ifNull" : ["$quantity",0]},
                "review_count" : {"$ifNull" : ["$review_count",0]},
                "age" : {"$ifNull" : ["$age",0]},
                "listing_quality_score" : {"$ifNull" : ["$listing_quality_score",0.0]},
                "currency" : {"$ifNull" : ["$currency",""]},
            }
        }
    ]
    item_result = list(Product.objects.aggregate(*pipeline))
    if item_result:
        item_result = item_result[0]
        return item_result
    return {}

    


def format_date_label(preset, start_date, end_date):
    if preset == "Today":
        return start_date.strftime("%B %d, %Y")
    elif preset == "Yesterday":
        return start_date.strftime("%B %d, %Y")
    elif preset in ["Last 7 Days", "Last 30 Days"]:
        return f"{start_date.strftime('%B %d, %Y')} - {end_date.strftime('%B %d, %Y')}"
    else:
        return f"{start_date.strftime('%B %d, %Y')} - {end_date.strftime('%B %d, %Y')}"
    

def getdaywiseproductssold_dict(start_date, end_date, product_id, is_hourly=False):
    results = getdaywiseproductssold(start_date, end_date, product_id, is_hourly)
    return {item["date"]: item for item in results}


def get_val_from_dict(date_obj, data_dict):
    date_str = date_obj.strftime("%Y-%m-%d")
    entry = data_dict.get(date_str)
    if entry:
        return entry["total_quantity"], float(entry["total_price"])
    return 0, 0.0


def sum_period_from_dict(start_day, end_day, data_dict):
    qty, price = 0, 0.0
    day = start_day
    while day <= end_day:
        q, p = get_val_from_dict(day, data_dict)
        qty += q
        price += p
        day += timedelta(days=1)
    return qty, round(price, 2)


def calc_diff_trend(current, previous):
    diff = round(current - previous, 2)
    trend = "up" if diff > 0 else "down" if diff < 0 else "neutral"
    return diff, trend


def productsSalesOverview(request):
    from django.utils import timezone

    product_id = request.GET.get("product_id")
    preset = request.GET.get("preset", "").strip().title()  # Normalize preset
    now = timezone.now()

    login_date = now.date()
    yesterday = login_date - timedelta(days=1)
    prev_day = yesterday - timedelta(days=1)

    last_7days_start = login_date - timedelta(days=7)
    last_7days_end = login_date - timedelta(days=1)
    prev_7days_start = last_7days_start - timedelta(days=7)
    prev_7days_end = last_7days_start - timedelta(days=1)

    label = None
    filled_graph = []

    # Preload last 15 days of data for stats
    stats_data_dict = getdaywiseproductssold_dict(
        datetime.combine(login_date - timedelta(days=15), datetime.min.time()),
        datetime.combine(login_date - timedelta(days=1), datetime.max.time()),
        product_id,
        is_hourly=False
    )

    if preset:
        is_hourly = preset in ["Today", "Yesterday"]
        if preset == "Today":
            start_date = datetime.combine(login_date, datetime.min.time())
            end_date = now
        elif preset == "Yesterday":
            start_date = datetime.combine(yesterday, datetime.min.time())
            end_date = datetime.combine(yesterday, datetime.max.time())
        elif preset == "Last 7 Days":
            start_date = datetime.combine(last_7days_start, datetime.min.time())
            end_date = datetime.combine(last_7days_end, datetime.max.time())
        elif preset == "Last 30 Days":
            start_date = datetime.combine(login_date - timedelta(days=30), datetime.min.time())
            end_date = datetime.combine(login_date - timedelta(days=1), datetime.max.time())
        else:
            start_date = end_date = None

        if start_date and end_date:
            label = format_date_label(preset, start_date, end_date)
            graph_data = getdaywiseproductssold(start_date, end_date, product_id, is_hourly)

            # Normalize date formats
            for item in graph_data:
                raw_date = item.get("date")
                try:
                    dt = datetime.strptime(raw_date, "%Y-%m-%d %H:00") if is_hourly else datetime.strptime(raw_date, "%Y-%m-%d")
                    item["date"] = dt.strftime("%Y-%m-%d %H:00:00") if is_hourly else dt.strftime("%Y-%m-%d")
                except Exception:
                    continue

            sales_dict = {item["date"]: item for item in graph_data}

            if is_hourly:
                base_date = start_date.strftime("%Y-%m-%d")
                hour_range = range(0, 25 if preset == "Today" else 24)
                for hour in hour_range:
                    time_str = f"{base_date} {hour:02d}:00:00"
                    filled_graph.append(sales_dict.get(time_str, {
                        "date": time_str,
                        "total_quantity": 0,
                        "total_price": 0.0
                    }))
            else:
                current = start_date.date()
                while current <= end_date.date():
                    date_str = current.strftime("%Y-%m-%d")
                    filled_graph.append(sales_dict.get(date_str, {
                        "date": date_str,
                        "total_quantity": 0,
                        "total_price": 0.0
                    }))
                    current += timedelta(days=1)

    # Final metrics using preloaded daywise data
    y_qty, y_price = get_val_from_dict(yesterday, stats_data_dict)
    p_qty, p_price = get_val_from_dict(prev_day, stats_data_dict)
    curr_qty, curr_price = sum_period_from_dict(last_7days_start, last_7days_end, stats_data_dict)
    prev_qty, prev_price = sum_period_from_dict(prev_7days_start, prev_7days_end, stats_data_dict)

    units = {
        "yesterday": {
            "value": y_qty,
            "difference": calc_diff_trend(y_qty, p_qty)[0],
            "trend": calc_diff_trend(y_qty, p_qty)[1],
        },
        "previous_day": {
            "value": p_qty,
            "difference": calc_diff_trend(p_qty, 0)[0],
            "trend": calc_diff_trend(p_qty, 0)[1],
        },
        "last_7_days": {
            "value": curr_qty,
            "difference": calc_diff_trend(curr_qty, prev_qty)[0],
            "trend": calc_diff_trend(curr_qty, prev_qty)[1],
        }
    }

    sales = {
        "yesterday": {
            "value": y_price,
            "difference": calc_diff_trend(y_price, p_price)[0],
            "trend": calc_diff_trend(y_price, p_price)[1],
        },
        "previous_day": {
            "value": p_price,
            "difference": calc_diff_trend(p_price, 0)[0],
            "trend": calc_diff_trend(p_price, 0)[1],
        },
        "last_7_days": {
            "value": curr_price,
            "difference": calc_diff_trend(curr_price, prev_price)[0],
            "trend": calc_diff_trend(curr_price, prev_price)[1],
        }
    }

    return {
        "label": label,
        "units": units,
        "sales": sales,
        "graph": filled_graph
    }


def productsListingQualityScore(request):
    product_id = request.GET.get('product_id')
    product_doc = DatabaseModel.get_document(Product.objects,{"id" : ObjectId(product_id)})
    product_dict = product_doc.to_mongo().to_dict()
    listing_data = calculate_listing_score(product_dict)
    DatabaseModel.update_documents(Product.objects,{"id" : ObjectId(product_id)},{"listing_quality_score" : listing_data['final_score']})
    scoreData = {
        "asin": product_dict.get("product_id",""),
        "imageUrl": product_dict.get("image_url",""),
        "title": product_dict.get("product_title",""),  
        "productUrl": product_dict.get("product_url",""),
        "metricData": {
            "titleStrangeSymbols": {
                "metric": "titleStrangeSymbols",
                "metricTitle": "Title does not contain symbols or emojis",
                "metricTooltip": "Emojis and symbols can hamper readability",
                "passed": listing_data['rules_checks'][0]
            },
            "titleLength": {
                "metric": "titleLength",
                "metricTitle": "Title contains 150+ characters",
                "metricTooltip": "Maximized number of relevant keywords in your title improves discoverability",
                "passed": listing_data['rules_checks'][1]
            },
            "qtyBullets": {
                "metric": "qtyBullets",
                "metricTitle": "5+ bullet points",
                "metricTooltip": "Maximized number of bullet points can help improve discoverability",
                "passed": listing_data['rules_checks'][2]
            },
            "lengthBullets": {
                "metric": "lengthBullets",
                "metricTitle": "150+ characters in each bullet point",
                "metricTooltip": "Maximized number of relevant keywords in bullet points helps improve discoverability",
                "passed": listing_data['rules_checks'][3]
            },
            "capitalizedBullets": {
                "metric": "capitalizedBullets",
                "metricTitle": "First letter of bullet points is capitalized",
                "metricTooltip": "Capitalized first letter of first word in each bullet point improves readability",
                "passed": listing_data['rules_checks'][4]
            },
            "allCapsBullets": {
                "metric": "allCapsBullets",
                "metricTitle": "Bullet points are not in all caps",
                "metricTooltip": "Amazon TOS discourages using all caps",
                "passed": listing_data['rules_checks'][5]
            },
            "ebcAndDescription": {
                "metric": "ebcAndDescription",
                "metricTitle": "1,000+ characters in description or A+ content",
                "metricTooltip": "Maximized number of relevant keywords in description helps improve discoverability",
                "passed": listing_data['rules_checks'][6]
            },
            "imageResolution": {
                "metric": "imageResolution",
                "metricTitle": "1000 x 1000 px +",
                "metricTooltip": "Images at least 1000 x 1000px enable zoom feature",
                "passed": listing_data['rules_checks'][7]
            },
            "imageBackground": {
                "metric": "imageBackground",
                "metricTitle": "Main image is on a white background",
                "metricTooltip": "Amazon TOS requires main image to be on a white background",
                "passed": listing_data['rules_checks'][8]
            },
            "imagesQty": {
                "metric": "imagesQty",
                "metricTitle": "7+ images",
                "metricTooltip": "Increased number of images can help drive conversions",
                "passed": listing_data['rules_checks'][9]
            },
            "videosQty": {
                "metric": "videosQty",
                "metricTitle": "Includes video",
                "metricTooltip": "Videos can help users learn more about the product and increase conversions",
                "passed": listing_data['rules_checks'][10]
            },
            "reviewQty": {
                "metric": "reviewQty",
                "metricTitle": "20+ reviews",
                "metricTooltip": "Increased number of reviews can increase product credibility with shoppers",
                "passed": listing_data['rules_checks'][11]
            },
            "reviewRating": {
                "metric": "reviewRating",
                "metricTitle": "4+ average star ratings",
                "metricTooltip": "Increased number of positive, 4+ star ratings can increase product credibility with shoppers",
                "passed": listing_data['rules_checks'][12]
            }
        },
        "totalScore": listing_data['final_score']
    }
    return scoreData


def productsTrafficandConversions(request):
    data = dict()
    preset = request.GET.get('preset')
    product_id = request.GET.get('product_id')
    product_obj = DatabaseModel.get_document(Product.objects,{"id" : ObjectId(product_id)},['product_id'])
    data['asin'] = product_obj.product_id
    # Calculate date ranges
    start_date, end_date = get_date_range(preset)
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    if start_date and end_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        start_date, end_date = get_date_range(preset)

    data['date'] = start_date.strftime("%b %d, %Y") + " - " + end_date.strftime("%b %d, %Y")
    
    
    # Get daily sales data using existing function
    daily_sales = getdaywiseproductssold(start_date, end_date, product_id)
    view_and_sales = pageViewsandSessionCount(start_date,end_date,product_id)
    #UNITS SOLD DATA
    data['total_units_sold'] = sum(item['total_quantity'] for item in daily_sales)
    data['average_units_sold'] = 0
    units_sold_graph = []
    for item in daily_sales:
        units_sold_graph.append({
            "date": item['date'],
            "units": item['total_quantity'],
            "average" : 0
        })
    data['units_sold_graph'] = units_sold_graph

    #SESSION WISE DATA
    data['total_sessions'] = sum(item['session_count'] for item in view_and_sales)
    data['average_sessions'] = 0
    sessions_graph = []
    for item in view_and_sales:
        sessions_graph.append({
            "date": item['date'],
            "sessions": item['session_count'],
            "average" : 0
        })
    data['sessions_graph'] = sessions_graph
    #PAGE VIEWS DATA
    data['total_page_views'] = sum(item['page_views'] for item in view_and_sales)
    data['average_page_views'] = 0
    page_views_graph = []
    for item in view_and_sales:
        page_views_graph.append({
            "date": item['date'],
            "page_views": item['page_views'],
            "average" : 0
        })
    data['page_views_graph'] = page_views_graph
    
    
    return data



##################################-----------------------Dashboard Filter API-----------------------###############################################

@csrf_exempt
def getSKUlist(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id')
    search_query = json_request.get('search_query')
    brand_id = json_request.get('brand_id')
    manufacturer_name = json_request.get('manufacturer_name')
    match =dict()
    pipeline = []

    if search_query != None and search_query != "":
        search_query = search_query.strip() 
        match["sku"] = {"$regex": search_query, "$options": "i"}

    
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)

    if brand_id != None and brand_id != "" and brand_id != [] and brand_id != "custom":
        brand_list = [ObjectId(i) for i in brand_id]
        match['brand_id'] = {"$in":brand_list}

    if manufacturer_name != None and manufacturer_name != "" and manufacturer_name != [] and manufacturer_name != "custom":
        match['manufacturer_name'] = {"$in":manufacturer_name}

    if match != {}:
        pipeline.append({"$match": match})

    pipeline.extend([
        {
            "$project": {
                "_id": 0,
                "id": {"$toString": "$_id"},
                "sku": "$sku",
            }
        }
    ])
    if match =={}:
        pipeline.append({
            "$limit": 10}  # Randomly select 10 documents
        )
    sku_list = list(Product.objects.aggregate(*pipeline))
    return sku_list

@csrf_exempt
def getproductIdlist(request):
    json_request = JSONParser().parse(request)
    marketplace_id = json_request.get('marketplace_id')
    brand_id = json_request.get('brand_id')
    search_query = json_request.get('search_query')
    match =dict()
    pipeline = []
    manufacturer_name = json_request.get('manufacturer_name')

    if search_query != None and search_query != "":
        search_query = search_query.strip() 
        match["product_id"] = {"$regex": search_query, "$options": "i"}

    
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if brand_id != None and brand_id != "" and brand_id != [] and brand_id != "custom":
        brand_list = [ObjectId(i) for i in brand_id]
        match['brand_id'] = {"$in":brand_list}

    if manufacturer_name != None and manufacturer_name != "" and manufacturer_name != [] and manufacturer_name != "custom":
        match['manufacturer_name'] = {"$in":manufacturer_name}
        
    if match != {}:
        pipeline.append({"$match": match})

    pipeline.extend([
        {
            "$project": {
                "_id": 0,
                "id" : {"$toString": "$_id"},
                "Asin": "$product_id",
            }
        }
    ])
    if match =={}:
        pipeline.append({
            "$sample": {"size": 10}  # Randomly select 10 documents
        })
    asin_list = list(Product.objects.aggregate(*pipeline))
    return asin_list


def getBrandListforfilter(request):
    data = dict()
    marketplace_id = request.GET.get('marketplace_id')
    search_query = request.GET.get('search_query')
    skip = int(request.GET.get('limit',1))
    match =dict()
    pipeline = []


    if search_query != None and search_query != "":
        search_query = search_query.strip() 
        match["name"] = {"$regex": search_query, "$options": "i"}

    
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if match != {}:
        pipeline.append({"$match": match})
    pipeline.extend([
        {
            "$project" : {
                "_id" : 0,
                "id" : {"$toString" : "$_id"},
                "name" : 1,
                
            }
        },
        {
            "$sort" : {
                "name" : 1
            }
        },
        {
            "$skip" : skip
        },
        {
            "$limit" : 10
        }
    ])
    
    brand_list = list(Brand.objects.aggregate(*(pipeline)))
    data['brand_list'] = brand_list
    return data

def obtainManufactureNames(request):
    marketplace_id = request.GET.get('marketplace_id')
    search_query = request.GET.get('search_query')
    match =dict()
    pipeline = []

    if search_query != None and search_query != "":
        search_query = search_query.strip() 
        match["manufacturer_name"] = {"$regex": search_query, "$options": "i"}

    
    if marketplace_id != None and marketplace_id != "" and marketplace_id != "all" and marketplace_id != "custom":
        match['marketplace_id'] = ObjectId(marketplace_id)
    if match != {}:
        pipeline.append({"$match": match})
    pipeline.extend([
        {
            "$group" : {
                "_id" : None,
                "manufacturer_name_list" : { "$addToSet": "$manufacturer_name" }
            }
        },
        {
            "$project": {
                "_id": 0,
                "manufacturer_name_list": 1
            }
        }
    ])
    Product_list = list(Product.objects.aggregate(*pipeline))
    data = dict()
    data['manufacturer_name_list'] = []
    if Product_list:
        data['manufacturer_name_list'] = Product_list[0]['manufacturer_name_list']
    return  JsonResponse(data,safe=False)



def InsightsProductWise(request):
    product_id = request.GET.get('product_id')
    optimized_count = 0
    refund_alerts = []
    fee_alerts = []
    listing_optimization_alerts = []
    product_performance_alerts = []  # New alert list for Product Performance

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

    def get_image_alerts(product):
        alerts = []
        images = product.image_urls or []
        if not images:
            alerts.append("No main image found. Please upload a clear product image with a white background.")
        else:
            main_image = images[0]
            if not (main_image.endswith('.jpg') or main_image.endswith('.jpeg') or main_image.endswith('.png')):
                alerts.append("Main image format should be JPG or PNG for better clarity and compatibility.")
            if 'white' not in main_image.lower():
                alerts.append("Update your main image background to white. This enhances your product's visual appeal and professionalism while meeting Amazon's requirements.")
            if 'small' in main_image.lower() or 'thumbnail' in main_image.lower():
                alerts.append("Update your main image size so that it is clear and of high quality for your potential customers.")
        return alerts

    Refund_obj = Refund.objects(product_id=product_id)
    refunded_product_ids = list(set([i.product_id.id for i in Refund_obj]))
    for product_id in refunded_product_ids:
        product = Product.objects(id=product_id).first()
        if not product:
            continue

        if is_optimized(product):
            optimized_count += 1

        alerts = get_image_alerts(product)
        if alerts:
            listing_optimization_alerts.append({
                "product_id": str(product.id),
                "title": product.product_title,
                "messages": alerts
            })

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

            # **New alert for Product Performance if refund rate has decreased by 6% or more**
            if refund_rate <= 6:
                product_performance_alerts.append({
                    "product_id": str(product.id),
                    "title": product.product_title,
                    "refund_rate": round(refund_rate, 2),
                    "message": f"Refund rates for {product.product_title} have decreased by an impressive 6% or more. Your dedication is driving results, it’s time to take a closer look at your strategy."
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

    if this_month_fees > last_month_fees:
        increase = round(this_month_fees - last_month_fees, 2)
        fee_alerts.append({
            "marketplace": "amazon.com",
            "increase_amount": increase,
            "message": f"Amazon Storage fees have increased by ${increase} for amazon.com. Storage fees have increased, cutting into your profit margins. Consider optimizing your inventory or fulfillment strategies now."
        })

    inventory_alerts = []
    product_obj = Product.objects(id=product_id).first()

    days_remaining = getattr(product_obj, 'days_of_inventory_remaining', 999)

    if days_remaining <= 45:
        reorder_by_date = datetime.date.today() + datetime.timedelta(days=days_remaining)

        if days_remaining <= 38:
            alert_message = f"Order more inventory now to avoid running out of stock. You have {days_remaining} days of inventory remaining."
        else:
            alert_message = f"Order more inventory by {reorder_by_date.strftime('%B %d, %Y')} to avoid running out of stock. You have {days_remaining} days of inventory remaining."

        inventory_alerts.append({
            "title": getattr(product_obj, 'title', ''),
            "asin": getattr(product_obj, 'asin', ''),
            "sku": getattr(product_obj, 'sku', ''),
            "fulfillment_channel": getattr(product_obj, 'fulfillment_channel', ''),
            "days_left": days_remaining,
            "reorder_by": reorder_by_date.isoformat(),
            "inventory_alert": alert_message
        })

    return JsonResponse({
    "alerts_feed": [
        *[
            {
                "type": "Listing Optimization",
                "title": alert["title"],
                "date": datetime.today(),
                "message": msg
            }
            for alert in listing_optimization_alerts
            for msg in alert["messages"]
        ],
        *[
            {
                "type": "Refunds",
                "title": alert["title"],
                "date":  datetime.today(),
                "message": alert["message"]
            }
            for alert in refund_alerts
        ],
        *[
            {
                "type": "Product Performance",
                "title": alert["title"],
                "date":  datetime.today(),
                "message": alert["message"]
            }
            for alert in product_performance_alerts
        ],
        *[
            {
                "type": "Inventory",
                "title": alert["title"],
                "date":  datetime.today(),
                "message": alert["inventory_alert"]
            }
            for alert in inventory_alerts
        ],
        *[
            {
                "type": "Inventory",
                "title": "Storage Fee Alert",
                "date":  datetime.today(),
                "message": alert["message"]
            }
            for alert in fee_alerts
        ],
        *[
            # {
            #     "type": "Keyword",
            #     "title": "Keyword Ranking",
            #     "date": "2024-11-30",
            #     "message": "Your keyword “nuriva memory pill” went from page 1, position 8, to position 13 over the past 4 days. Review the listing for issues or review your PPC campaigns."
            # },
            # {
            #     "type": "Keyword",
            #     "title": "Keyword Ranking",
            #     "date": "2024-11-24",
            #     "message": "Your keyword “neuriva brain supplement” went from page 1, position 9, to position 11 over the past 4 days. Review the listing for issues or review your PPC campaigns."
            # }
        ]
    ]
})

