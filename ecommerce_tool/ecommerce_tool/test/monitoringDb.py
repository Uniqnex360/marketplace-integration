from datetime import datetime, timedelta
from pytz import timezone
from mongoengine.queryset.visitor import Q
from omnisight.models import Order, DailyMetrics
from omnisight.operations.helium_utils import refundOrder

def refresh_today_metrics():
    """
    Refreshes global DailyMetrics for today by checking if any Order was updated today.
    """

    # Timezone setup
    tz = timezone("US/Pacific")
    now = datetime.now(tz)

    # Define today's range
    today_start = tz.localize(datetime(now.year, now.month, now.day, 0, 0, 0))
    today_end = tz.localize(datetime(now.year, now.month, now.day, 23, 59, 59))

    # Convert to UTC for querying Mongo
    start_utc = today_start.astimezone(timezone("UTC"))
    end_utc = today_end.astimezone(timezone("UTC"))

    print(f"[INFO] Refreshing global DailyMetrics for date: {start_utc.date()}...")

    # Check if there are any Orders updated today
    order_updates = Order.objects(
        Q(last_update_date__gte=start_utc) &
        Q(last_update_date__lte=end_utc)
    )

    if not order_updates:
        print("[SKIP] No order updates for today. No changes to DailyMetrics.")
        return

    # Orders were updated → perform aggregation
    print(f"[PROCESS] Calculating DailyMetrics from {len(order_updates)} updated orders...")

    result = gross_revenue(
        start_utc,
        end_utc,
        marketplace_id=None,
        brand_id=None,
        product_id=None,
        manufacturer_name=[],
        fulfillment_channel=None,
        timezone_str="US/Pacific"
    )

    refund_data = refundOrder(
        start_utc,
        end_utc,
        marketplace_id=None,
        brand_id=None,
        product_id=None,
        manufacturer_name=[],
        fulfillment_channel=None
    )

    # Aggregate values from result
    gross_revenue = sum(order['order_total'] for order in result)
    total_units = sum(order['items_order_quantity'] for order in result)
    total_orders = len(set(order.get('purchase_order_id') or str(order['_id']) for order in result))
    refund_qty = sum(len(r['order_items']) for r in refund_data)

    # TODO: Fill with actual logic from your side
    net_profit = 0.0
    margin = 0.0
    total_tax = 0.0
    total_cogs = 0.0

    # Save global daily metrics (no brand_id/marketplace_id)
    DailyMetrics.objects(
        brand_id=None,
        marketplace_id=None,
        date=start_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    ).update_one(
        set__gross_revenue=gross_revenue,
        set__net_profit=net_profit,
        set__total_orders=total_orders,
        set__total_units=total_units,
        set__total_tax=total_tax,
        set__refund=refund_qty,
        set__margin=margin,
        set__total_cogs=total_cogs,
        set__updated_at=datetime.utcnow(),
        upsert=True
    )

    print(f"[✔ SAVED] Global DailyMetrics for {start_utc.date()} updated.")

if __name__ == "__main__":
    refresh_today_metrics()