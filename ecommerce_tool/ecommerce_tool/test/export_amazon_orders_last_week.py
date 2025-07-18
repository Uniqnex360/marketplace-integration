from datetime import datetime, timedelta
import pytz
import math
from mongoengine import connect
from omnisight.models import Order, Product, Fee  # adjust import paths as needed

# Connect to MongoDB
connect(
    db='ecommerce_db',
    host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/',
    port=27017
)

# Get today's date range in US/Pacific timezone
pacific = pytz.timezone("US/Pacific")
now_pacific = datetime.now(pacific)
start_of_day = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_day = now_pacific.replace(hour=23, minute=59, second=59, microsecond=999999)

# Convert to UTC for MongoDB queries
start_utc = start_of_day.astimezone(pytz.utc)
end_utc = end_of_day.astimezone(pytz.utc)

print(f"\n📅 Calculating for: {start_of_day.strftime('%Y-%m-%d')} (Pacific Time)")
print(f"🔍 UTC Query Range: {start_utc} → {end_utc}\n")

# ---------------------------------------
# 📦 Step 1: Fetch Orders for Today
# ---------------------------------------
orders_today = Order.objects(order_date__gte=start_utc, order_date__lte=end_utc)
print(f"🧾 Orders Found: {orders_today.count()}")

# ---------------------------------------
# 🧾 Step 2: Collect Items and SKUs
# ---------------------------------------
sku_qty_map = {}  # sku: total_quantity

for order in orders_today:
    # Access order_items which contains OrderItems references
    for order_item in order.order_items:
        # Access the ProductDetails embedded document
        if order_item.ProductDetails:
            sku = order_item.ProductDetails.SKU
            qty = order_item.ProductDetails.QuantityOrdered or 0
            
            if sku in sku_qty_map:
                sku_qty_map[sku] += qty
            else:
                sku_qty_map[sku] = qty

# ---------------------------------------
# 📦 Step 3: Fetch Products by SKU
# ---------------------------------------
products = Product.objects(sku__in=list(sku_qty_map.keys()))

# ---------------------------------------
# 💰 Step 4: Calculate Expenses
# ---------------------------------------
total_cogs = 0
manual_shipping = 0
amazon_shipping = 0
walmart_shipping = 0

for product in products:
    sku = product.sku
    qty = sku_qty_map.get(sku, 0)

    # Use the correct field names from your schema and handle None values
    total_cogs += (product.total_cogs if product.total_cogs is not None else 0) * qty
    manual_shipping += (product.shipping_cost if product.shipping_cost is not None else 0) * qty
    amazon_shipping += (product.a_shipping_cost if product.a_shipping_cost is not None else 0) * qty
    walmart_shipping += (product.w_shiping_cost if product.w_shiping_cost is not None else 0) * qty  # Note: keeping the typo as in schema

# ---------------------------------------
# 💸 Step 5: Fetch Fees for the Day
# ---------------------------------------
fees_today = Fee.objects(date__gte=start_utc, date__lte=end_utc)
total_fees = sum(f.amount if f.amount is not None else 0 for f in fees_today)

# ---------------------------------------
# 📊 Step 6: Output Summary
# ---------------------------------------
print(f"\n🧮 Total COGS (from Orders):       ${total_cogs:,.2f}")
print(f"🚚 Manual Shipping (Product):      ${manual_shipping:,.2f}")
print(f"🚚 Amazon Shipping (Product):      ${amazon_shipping:,.2f}")
print(f"🚚 Walmart Shipping (Product):     ${walmart_shipping:,.2f}")
print(f"💸 Total Fees (from Fee logs):     ${total_fees:,.2f}")

grand_total = total_cogs + manual_shipping + amazon_shipping + walmart_shipping + total_fees
print(f"\n💰 Grand Total Expense Today:      ${grand_total:,.2f}")

# ---------------------------------------
# 🔍 Debug Information
# ---------------------------------------
print(f"\n🔍 Debug Info:")
print(f"   • SKUs found in orders: {len(sku_qty_map)}")
print(f"   • Products found in database: {products.count()}")
print(f"   • Fee records found: {fees_today.count()}")
if sku_qty_map:
    print(f"   • Sample SKU quantities: {dict(list(sku_qty_map.items())[:3])}")