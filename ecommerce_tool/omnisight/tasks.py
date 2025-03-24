from celery import shared_task
from omnisight.operations.walmart_operations import syncRecentWalmartOrders
from omnisight.operations.amazon_operations import syncRecentAmazonOrders

@shared_task
def sync_orders():
    print("Orders Sync starting........................")
    walmart_orders = syncRecentWalmartOrders()
    amazon_orders = syncRecentAmazonOrders()
    
    return f"Synced {len(walmart_orders) + len(amazon_orders)} orders"
