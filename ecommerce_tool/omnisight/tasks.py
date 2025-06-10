from celery import shared_task
from omnisight.operations.walmart_operations import syncRecentWalmartOrders
from omnisight.operations.amazon_operations import syncRecentAmazonOrders,sync_inventory, syncPageviews

@shared_task
def sync_orders():
    print("Orders Sync starting........................")
    walmart_orders = syncRecentWalmartOrders()
    amazon_orders = syncRecentAmazonOrders()
    print("Orders Sync completed........................")
    
    return f"Synced {len(walmart_orders) + len(amazon_orders)} orders"


@shared_task
def sync_walmart_orders():
    print("Orders Sync starting........................")
    walmart_orders = syncRecentWalmartOrders()
    print("Orders Sync completed........................")
    
    return f"Synced {len(walmart_orders)} orders"

@shared_task
def sync_products():
    print("PageViews Sync starting........................")
    syncPageviews()
    return True



@shared_task
def sync_inventry():
    print("PageViews Sync starting........................")
    sync_inventory()
    print("Inventory Sync completed........................")
    return True