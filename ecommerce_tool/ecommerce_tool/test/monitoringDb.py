from datetime import datetime, timedelta
from omnisight.models import *
from mongoengine import connect
import os


connect(
    db=os.getenv('DATABASE_NAME'),
    # username=os.getenv('DATABASE_USER'),
    # password=os.getenv('DATABASE_PASSWORD'),
    host=os.getenv('DATABASE_HOST'),
    # alias='default'
)
def has_orders_updated_within_last_hour():
    one_hour_ago = datetime.utcnow()
    result = Order.objects(
        updated_at__gte=one_hour_ago - timedelta(hours=1)
    ).first() is not None

    if result:
        print(f"[{datetime.utcnow()}] Orders updated within the last hour.")
    else:
        print(f"[{datetime.utcnow()}] No orders updated within the last hour.")

    return result

# Example call (for testing)
if __name__ == "__main__":
    has_orders_updated_within_last_hour()
