import os
from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'ecommerce_tool.settings')

app = Celery(
    'ecommerce_tool',
    broker='redis://localhost:6379/0',  # Redis as the broker
    backend='redis://localhost:6379/0',  # Optional: Redis for result storage
)

# Load task modules from Django settings
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-discover tasks from Django apps
app.autodiscover_tasks()

from celery.schedules import crontab

app.conf.beat_schedule = {
    'sync-orders-every-30-minutes': {
        'task': 'omnisight.tasks.sync_orders',
        'schedule': crontab(minute='*/30'),  # Every 30 minutes
    },
    'sync-products-every-hour': {
        'task': 'omnisight.tasks.sync_products',  # Replace with your actual task path
        'schedule': crontab(minute=0),  # Every hour at minute 0
    },
}

app.conf.timezone = 'UTC'
