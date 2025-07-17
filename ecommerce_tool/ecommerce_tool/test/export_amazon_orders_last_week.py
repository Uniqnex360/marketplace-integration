import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil import tz
from mongoengine import connect
from omnisight.models import (
    Order, OrderItems, Product, Marketplace, Fee, Refund, 
    pageview_session_count, user, custom_order
)
from bson import ObjectId
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderReportGenerator:
    def __init__(self):
        """Initialize the report generator"""
        self.connect_to_database()
        self.marketplace_cache = {}
        self.product_cache = {}
    
    def connect_to_database(self):
        """Connect to MongoDB database"""
        try:
            connect(
    db='ecommerce_db',  # ← This must match the name of the target database
    host='mongodb://plmp_admin:admin%401234@54.86.75.104:27017/ecommerce_db',
    authentication_source='admin'  # ← Use the auth DB where the user is created
)  # Replace with your actual database name
            logger.info("✅ Connected to MongoDB")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def validate_order_data(self, order) -> bool:
        """Validate essential order data"""
        required_fields = ['purchase_order_id', 'order_date', 'order_total']
        return all(hasattr(order, field) and getattr(order, field) is not None for field in required_fields)
    
    def get_marketplace_cached(self, marketplace_id: ObjectId) -> Optional[object]:
        """Get marketplace with caching"""
        if marketplace_id not in self.marketplace_cache:
            try:
                marketplace = Marketplace.objects(id=marketplace_id).first()
                self.marketplace_cache[marketplace_id] = marketplace
            except Exception as e:
                logger.warning(f"Could not fetch marketplace {marketplace_id}: {e}")
                self.marketplace_cache[marketplace_id] = None
        
        return self.marketplace_cache[marketplace_id]
    
    def get_product_cached(self, product_id: ObjectId) -> Optional[object]:
        """Get product with caching"""
        if product_id not in self.product_cache:
            try:
                product = Product.objects(id=product_id).first()
                self.product_cache[product_id] = product
            except Exception as e:
                logger.warning(f"Could not fetch product {product_id}: {e}")
                self.product_cache[product_id] = None
        
        return self.product_cache[product_id]
    
    def get_orders_from_db(self, start_date: datetime, end_date: datetime, marketplace_name: str = None) -> List[object]:
        """Fetch orders from database within date range with improved querying"""
        try:
            # Use MongoDB native date objects for better performance
            filters = {
                'order_date__gte': start_date,
                'order_date__lte': end_date
            }
            
            # Add marketplace filter if specified
            if marketplace_name:
                marketplace = Marketplace.objects(name=marketplace_name).first()
                if marketplace:
                    filters['marketplace_id'] = marketplace
                else:
                    logger.warning(f"Marketplace '{marketplace_name}' not found")
                    return []
            
            logger.info(f"🔍 Querying orders from {start_date.date()} to {end_date.date()}")
            
            # Fetch orders
            orders = Order.objects(**filters).order_by('order_date')
            
            # Batch populate marketplace references
            marketplace_ids = set()
            for order in orders:
                if hasattr(order, 'marketplace_id') and order.marketplace_id:
                    marketplace_ids.add(order.marketplace_id)
            
            # Pre-fetch all marketplaces
            if marketplace_ids:
                marketplaces = {mp.id: mp for mp in Marketplace.objects(id__in=list(marketplace_ids))}
                for order in orders:
                    if hasattr(order, 'marketplace_id') and order.marketplace_id in marketplaces:
                        order.marketplace = marketplaces[order.marketplace_id]
            
            # Validate orders
            valid_orders = [order for order in orders if self.validate_order_data(order)]
            invalid_count = len(orders) - len(valid_orders)
            
            if invalid_count > 0:
                logger.warning(f"⚠️ Skipped {invalid_count} orders with invalid data")
            
            logger.info(f"📦 Found {len(valid_orders)} valid orders")
            
            return valid_orders
            
        except Exception as e:
            logger.error(f"❌ Error fetching orders: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_order_items_batch(self, order_item_ids: List[ObjectId]) -> Dict[ObjectId, object]:
        """Fetch order items in batch for better performance"""
        try:
            order_items = OrderItems.objects(id__in=order_item_ids)
            
            # Create lookup dictionary
            items_dict = {item.id: item for item in order_items}
            
            # Batch fetch product details
            product_ids = set()
            for item in order_items:
                if hasattr(item, 'ProductDetails') and hasattr(item.ProductDetails, 'product_id'):
                    product_ids.add(item.ProductDetails.product_id)
            
            if product_ids:
                products = {p.id: p for p in Product.objects(id__in=list(product_ids))}
                for item in order_items:
                    if (hasattr(item, 'ProductDetails') and 
                        hasattr(item.ProductDetails, 'product_id') and
                        item.ProductDetails.product_id in products):
                        item.ProductDetails.product = products[item.ProductDetails.product_id]
            
            return items_dict
            
        except Exception as e:
            logger.error(f"❌ Error fetching order items batch: {e}")
            return {}
    
    def calculate_product_cogs(self, product) -> float:
        """Calculate total COGS for a product with improved logic"""
        if not product:
            return 0.0
        
        # Check if pre-calculated COGS exists
        if hasattr(product, 'total_cogs') and product.total_cogs and product.total_cogs > 0:
            return float(product.total_cogs)
        
        if hasattr(product, 'w_total_cogs') and product.w_total_cogs and product.w_total_cogs > 0:
            return float(product.w_total_cogs)
        
        # Calculate COGS from components
        total_cogs = 0.0
        
        # Base costs
        cost_fields = [
            'product_cost', 'shipping_cost', 'a_shipping_cost', 'w_shiping_cost',
            'channel_fee', 'fullfillment_by_channel_fee', 'referral_fee', 'walmart_fee'
        ]
        
        for field in cost_fields:
            if hasattr(product, field):
                value = getattr(product, field)
                if value is not None:
                    total_cogs += float(value)
        
        return total_cogs
    
    def get_additional_fees(self, start_date: datetime, end_date: datetime, marketplace_name: str = None) -> List[object]:
        """Get additional fees from Fee collection"""
        try:
            filters = {
                'date__gte': start_date,
                'date__lte': end_date
            }
            
            if marketplace_name:
                filters['marketplace'] = marketplace_name
            
            fees = Fee.objects(**filters)
            logger.info(f"📊 Found {len(fees)} additional fees")
            return list(fees)
            
        except Exception as e:
            logger.error(f"❌ Error fetching fees: {e}")
            return []
    
    def get_refunds(self, start_date: datetime, end_date: datetime) -> List[object]:
        """Get refunds within date range"""
        try:
            refunds = Refund.objects(
                date__gte=start_date,
                date__lte=end_date
            )
            logger.info(f"🔄 Found {len(refunds)} refunds")
            return list(refunds)
            
        except Exception as e:
            logger.error(f"❌ Error fetching refunds: {e}")
            return []
    
    def process_order_data(self, orders: List[object], additional_fees: List[object], refunds: List[object]) -> List[Dict]:
        """Process orders and create detailed report data with improved performance"""
        report_data = []
        
        # Create lookup dictionary for refunds by product ID
        refund_amounts = {}
        for refund in refunds:
            try:
                product_id = str(refund.product_id.id) if hasattr(refund, 'product_id') and refund.product_id else None
                if product_id:
                    refund_amounts[product_id] = refund_amounts.get(product_id, 0) + 1
            except Exception as e:
                logger.warning(f"Error processing refund: {e}")
                continue
        
        # Get all order item IDs for batch processing
        all_order_item_ids = []
        for order in orders:
            order_items = getattr(order, 'order_items', [])
            all_order_item_ids.extend(order_items)
        
        # Batch fetch all order items
        order_items_dict = self.get_order_items_batch(all_order_item_ids)
        
        # Process each order
        for order in orders:
            try:
                order_items = getattr(order, 'order_items', [])
                
                if not order_items:
                    # Create basic record for orders without items
                    row_data = self.create_basic_order_record(order, refund_amounts)
                    report_data.append(row_data)
                    continue
                
                # Process each order item
                for order_item_id in order_items:
                    try:
                        order_item = order_items_dict.get(order_item_id)
                        if not order_item:
                            logger.warning(f"Order item {order_item_id} not found")
                            continue
                        
                        row_data = self.create_order_record_with_item(order, order_item, refund_amounts)
                        report_data.append(row_data)
                        
                    except Exception as e:
                        logger.error(f"Error processing order item {order_item_id}: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Error processing order {getattr(order, 'purchase_order_id', 'Unknown')}: {e}")
                continue
        
        logger.info(f"📋 Processed {len(report_data)} order records")
        return report_data
    
    def create_basic_order_record(self, order, refund_amounts: Dict[str, int]) -> Dict:
        """Create a basic order record without item details"""
        # Get marketplace name safely
        marketplace_name = ''
        try:
            if hasattr(order, 'marketplace'):
                marketplace_name = order.marketplace.name if order.marketplace else ''
        except:
            pass
        
        # Safe attribute access with defaults
        def safe_get(obj, attr, default=''):
            try:
                return getattr(obj, attr, default) or default
            except:
                return default
        
        # Get shipping information safely
        shipping_info = safe_get(order, 'shipping_information', {})
        if not isinstance(shipping_info, dict):
            shipping_info = {}
        
        return {
            'Order_ID': safe_get(order, 'purchase_order_id') or safe_get(order, 'merchant_order_id'),
            'Customer_Order_ID': safe_get(order, 'merchant_order_id'),
            'Order_Date': order.order_date,
            'Marketplace': marketplace_name,
            'Channel': safe_get(order, 'sales_channel'),
            'Order_Status': safe_get(order, 'order_status'),
            'Fulfillment_Channel': safe_get(order, 'fulfillment_channel'),
            'Is_Prime': safe_get(order, 'is_prime', False),
            'Is_Business_Order': safe_get(order, 'is_business_order', False),
            'Product_Title': '',
            'SKU': '',
            'ASIN': '',
            'Product_Condition': '',
            'Brand': '',
            'Category': '',
            'Quantity_Ordered': safe_get(order, 'items_order_quantity', 0),
            'Quantity_Shipped': safe_get(order, 'number_of_items_shipped', 0),
            'Unit_Price': 0.0,
            'Item_Tax': 0.0,
            'Promotion_Discount': 0.0,
            'Gross_Revenue': float(safe_get(order, 'order_total', 0.0)),
            'Net_Revenue': float(safe_get(order, 'order_total', 0.0)),
            'Unit_COGS': 0.0,
            'Total_COGS': 0.0,
            'Gross_Profit': float(safe_get(order, 'order_total', 0.0)),
            'Profit_Margin_%': 0.0,
            'Refund_Count': 0,
            'Currency': safe_get(order, 'currency', 'USD'),
            'Ship_Method': safe_get(order, 'ship_service_level'),
            'Carrier': '',
            'Tracking_Number': '',
            'Ship_Date': '',
            'Ship_City': shipping_info.get('City', ''),
            'Ship_State': shipping_info.get('StateOrRegion', ''),
            'Ship_PostalCode': shipping_info.get('PostalCode', ''),
            'Ship_CountryCode': shipping_info.get('CountryCode', ''),
            'Ship_Service_Level': safe_get(order, 'ship_service_level'),
            'Order_Total': float(safe_get(order, 'order_total', 0.0)),
            'Items_Order_Quantity': safe_get(order, 'items_order_quantity', 0),
            'Shipping_Price': float(safe_get(order, 'shipping_price', 0.0)),
            'Number_Items_Shipped': safe_get(order, 'number_of_items_shipped', 0),
        }
    
    def create_order_record_with_item(self, order, order_item, refund_amounts: Dict[str, int]) -> Dict:
        """Create an order record with item details"""
        # Get product details safely
        product = None
        try:
            if hasattr(order_item, 'ProductDetails') and hasattr(order_item.ProductDetails, 'product'):
                product = order_item.ProductDetails.product
        except:
            pass
        
        # Get marketplace name safely
        marketplace_name = ''
        try:
            if hasattr(order, 'marketplace'):
                marketplace_name = order.marketplace.name if order.marketplace else ''
        except:
            pass
        
        # Safe attribute access
        def safe_get(obj, attr, default=''):
            try:
                return getattr(obj, attr, default) or default
            except:
                return default
        
        def safe_float(value, default=0.0):
            try:
                return float(value) if value is not None else default
            except:
                return default
        
        # Get shipping information safely
        shipping_info = safe_get(order, 'shipping_information', {})
        if not isinstance(shipping_info, dict):
            shipping_info = {}
        
        # Basic order information
        row_data = {
            'Order_ID': safe_get(order, 'purchase_order_id') or safe_get(order, 'merchant_order_id'),
            'Customer_Order_ID': safe_get(order, 'merchant_order_id'),
            'Order_Date': order.order_date,
            'Marketplace': marketplace_name,
            'Channel': safe_get(order, 'sales_channel'),
            'Order_Status': safe_get(order, 'order_status'),
            'Fulfillment_Channel': safe_get(order, 'fulfillment_channel'),
            'Is_Prime': safe_get(order, 'is_prime', False),
            'Is_Business_Order': safe_get(order, 'is_business_order', False),
        }
        
        # Product information
        if product:
            row_data.update({
                'Product_Title': safe_get(product, 'title'),
                'SKU': safe_get(product, 'sku'),
                'ASIN': safe_get(product, 'asin'),
                'Product_Condition': safe_get(product, 'condition'),
                'Brand': safe_get(product, 'brand_name'),
                'Category': safe_get(product, 'category'),
            })
        else:
            row_data.update({
                'Product_Title': safe_get(order_item, 'title'),
                'SKU': safe_get(order_item, 'sku'),
                'ASIN': safe_get(order_item, 'asin'),
                'Product_Condition': safe_get(order_item, 'condition'),
                'Brand': '',
                'Category': '',
            })
        
        # Quantity and pricing
        quantity_ordered = safe_get(order_item, 'quantity_ordered', 0)
        quantity_shipped = safe_get(order_item, 'quantity_shipped', 0)
        
        # Revenue calculations
        item_price = safe_float(safe_get(order_item, 'item_price', 0.0))
        item_tax = safe_float(safe_get(order_item, 'item_tax', 0.0))
        promotion_discount = safe_float(safe_get(order_item, 'promotion_discount', 0.0))
        
        gross_revenue = item_price * quantity_ordered
        net_revenue = gross_revenue - promotion_discount
        
        # COGS calculations
        unit_cogs = self.calculate_product_cogs(product) if product else 0.0
        total_cogs = unit_cogs * quantity_shipped
        
        # Profit calculations
        gross_profit = net_revenue - total_cogs
        profit_margin = (gross_profit / net_revenue * 100) if net_revenue > 0 else 0.0
        
        # Refund information
        product_id_str = str(product.id) if product else None
        refund_count = refund_amounts.get(product_id_str, 0)
        
        # Add financial data
        row_data.update({
            'Quantity_Ordered': quantity_ordered,
            'Quantity_Shipped': quantity_shipped,
            'Unit_Price': item_price,
            'Item_Tax': item_tax,
            'Promotion_Discount': promotion_discount,
            'Gross_Revenue': gross_revenue,
            'Net_Revenue': net_revenue,
            'Unit_COGS': unit_cogs,
            'Total_COGS': total_cogs,
            'Gross_Profit': gross_profit,
            'Profit_Margin_%': round(profit_margin, 2),
            'Refund_Count': refund_count,
            'Currency': safe_get(order, 'currency', 'USD'),
        })
        
        # Shipping information
        row_data.update({
            'Ship_Method': safe_get(order, 'ship_service_level'),
            'Carrier': safe_get(order_item, 'carrier'),
            'Tracking_Number': safe_get(order_item, 'tracking_number'),
            'Ship_Date': safe_get(order_item, 'ship_date'),
            'Ship_City': shipping_info.get('City', ''),
            'Ship_State': shipping_info.get('StateOrRegion', ''),
            'Ship_PostalCode': shipping_info.get('PostalCode', ''),
            'Ship_CountryCode': shipping_info.get('CountryCode', ''),
            'Ship_Service_Level': safe_get(order, 'ship_service_level'),
            'Order_Total': safe_float(safe_get(order, 'order_total', 0.0)),
            'Items_Order_Quantity': safe_get(order, 'items_order_quantity', 0),
            'Shipping_Price': safe_float(safe_get(order, 'shipping_price', 0.0)),
            'Number_Items_Shipped': safe_get(order, 'number_of_items_shipped', 0),
        })
        
        return row_data
    
    def create_summary_data(self, report_data: List[Dict]) -> Dict:
        """Create summary statistics with improved calculations"""
        if not report_data:
            return {}
        
        df = pd.DataFrame(report_data)
        
        # Convert Order_Date to datetime if it's string
        if not df.empty and isinstance(df['Order_Date'].iloc[0], str):
            df['Order_Date'] = pd.to_datetime(df['Order_Date'])
        
        # Calculate metrics
        summary = {
            'Total_Orders': len(df['Order_ID'].unique()) if not df.empty else 0,
            'Total_Items': len(df),
            'Total_Quantity_Ordered': df['Quantity_Ordered'].sum(),
            'Total_Quantity_Shipped': df['Quantity_Shipped'].sum(),
            'Total_Gross_Revenue': df['Gross_Revenue'].sum(),
            'Total_Net_Revenue': df['Net_Revenue'].sum(),
            'Total_COGS': df['Total_COGS'].sum(),
            'Total_Gross_Profit': df['Gross_Profit'].sum(),
            'Total_Refunds': df['Refund_Count'].sum(),
            'Total_Order_Value': df['Order_Total'].sum(),
            'Total_Shipping_Cost': df['Shipping_Price'].sum(),
        }
        
        # Calculate averages safely
        if not df.empty:
            order_values = df.groupby('Order_ID')['Net_Revenue'].sum()
            summary['Average_Order_Value'] = order_values.mean() if not order_values.empty else 0.0
            summary['Average_Profit_Margin'] = df['Profit_Margin_%'].mean()
        else:
            summary['Average_Order_Value'] = 0.0
            summary['Average_Profit_Margin'] = 0.0
        
        # Add additional metrics
        summary['Order_Fill_Rate'] = (summary['Total_Quantity_Shipped'] / summary['Total_Quantity_Ordered'] * 100) if summary['Total_Quantity_Ordered'] > 0 else 0.0
        summary['Revenue_per_Item'] = summary['Total_Net_Revenue'] / summary['Total_Items'] if summary['Total_Items'] > 0 else 0.0
        summary['COGS_Percentage'] = (summary['Total_COGS'] / summary['Total_Net_Revenue'] * 100) if summary['Total_Net_Revenue'] > 0 else 0.0
        
        return summary
    
    def generate_excel_report(self, start_date: datetime, end_date: datetime, marketplace_name: str = None) -> Optional[str]:
        """Generate comprehensive Excel report with enhanced features"""
        try:
            # Fetch data
            logger.info("📊 Generating order report from database...")
            orders = self.get_orders_from_db(start_date, end_date, marketplace_name)
            additional_fees = self.get_additional_fees(start_date, end_date, marketplace_name)
            refunds = self.get_refunds(start_date, end_date)
            
            if not orders:
                logger.warning("⚠️ No orders found for the specified date range")
                return None
            
            # Process data
            report_data = self.process_order_data(orders, additional_fees, refunds)
            summary_data = self.create_summary_data(report_data)
            
            # Create filename
            date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d') if start_date.date() != end_date.date() else None
            marketplace_str = f"_{marketplace_name}" if marketplace_name else ""
            
            if end_date_str:
                filename = f"order_report_{date_str}_to_{end_date_str}{marketplace_str}.xlsx"
            else:
                filename = f"order_report_{date_str}{marketplace_str}.xlsx"
            
            # Create Excel file with multiple sheets
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                # Main report sheet
                df_report = pd.DataFrame(report_data)
                
                # Convert Order_Date to string for Excel
                if not df_report.empty and 'Order_Date' in df_report.columns:
                    df_report['Order_Date'] = df_report['Order_Date'].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if hasattr(x, 'strftime') else str(x)
                    )
                
                df_report.to_excel(writer, sheet_name='Order_Details', index=False)
                
                # Summary sheet
                df_summary = pd.DataFrame([summary_data])
                df_summary.to_excel(writer, sheet_name='Summary', index=False)
                
                # Additional sheets only if we have data
                if not df_report.empty:
                    # Marketplace breakdown
                    if 'Marketplace' in df_report.columns:
                        marketplace_summary = df_report.groupby('Marketplace').agg({
                            'Order_ID': 'nunique',
                            'Quantity_Ordered': 'sum',
                            'Gross_Revenue': 'sum',
                            'Net_Revenue': 'sum',
                            'Total_COGS': 'sum',
                            'Gross_Profit': 'sum',
                            'Profit_Margin_%': 'mean'
                        }).reset_index()
                        marketplace_summary.columns = ['Marketplace', 'Unique_Orders', 'Total_Qty_Ordered', 
                                                     'Total_Gross_Revenue', 'Total_Net_Revenue', 
                                                     'Total_COGS', 'Total_Gross_Profit', 'Avg_Profit_Margin_%']
                        marketplace_summary.to_excel(writer, sheet_name='Marketplace_Summary', index=False)
                    
                    # Product performance
                    if 'SKU' in df_report.columns and 'Product_Title' in df_report.columns:
                        product_summary = df_report.groupby(['SKU', 'Product_Title']).agg({
                            'Quantity_Ordered': 'sum',
                            'Gross_Revenue': 'sum',
                            'Net_Revenue': 'sum',
                            'Total_COGS': 'sum',
                            'Gross_Profit': 'sum',
                            'Profit_Margin_%': 'mean',
                            'Refund_Count': 'sum'
                        }).reset_index().sort_values('Net_Revenue', ascending=False)
                        product_summary.to_excel(writer, sheet_name='Product_Performance', index=False)
                    
                    # Order Status breakdown
                    if 'Order_Status' in df_report.columns:
                        status_summary = df_report.groupby('Order_Status').agg({
                            'Order_ID': 'nunique',
                            'Quantity_Ordered': 'sum',
                            'Net_Revenue': 'sum',
                            'Gross_Profit': 'sum'
                        }).reset_index()
                        status_summary.columns = ['Order_Status', 'Unique_Orders', 'Total_Qty_Ordered', 
                                                'Total_Net_Revenue', 'Total_Gross_Profit']
                        status_summary.to_excel(writer, sheet_name='Order_Status_Summary', index=False)
                    
                    # Daily performance (if date range > 1 day)
                    if (end_date - start_date).days > 0:
                        df_report['Order_Date_Only'] = pd.to_datetime(df_report['Order_Date']).dt.date
                        daily_summary = df_report.groupby('Order_Date_Only').agg({
                            'Order_ID': 'nunique',
                            'Quantity_Ordered': 'sum',
                            'Net_Revenue': 'sum',
                            'Gross_Profit': 'sum'
                        }).reset_index()
                        daily_summary.columns = ['Date', 'Unique_Orders', 'Total_Qty_Ordered', 
                                               'Total_Net_Revenue', 'Total_Gross_Profit']
                        daily_summary.to_excel(writer, sheet_name='Daily_Performance', index=False)
                
                # Format the workbook
                workbook = writer.book
                
                # Define formats
                currency_format = workbook.add_format({'num_format': '$#,##0.00'})
                percentage_format = workbook.add_format({'num_format': '0.00%'})
                header_format = workbook.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1
                })
                
                # Format main sheet
                if 'Order_Details' in writer.sheets:
                    worksheet = writer.sheets['Order_Details']
                    
                    # Set column widths and formats
                    worksheet.set_column('A:L', 15)  # Basic info columns
                    worksheet.set_column('M:S', 12, currency_format)  # Revenue/COGS columns
                    worksheet.set_column('T:T', 12, percentage_format)  # Profit margin column
                    worksheet.set_column('U:Z', 12)  # Other columns
                    
                    # Add header formatting
                    for col_num, value in enumerate(df_report.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                
                # Format summary sheet
                if 'Summary' in writer.sheets:
                    worksheet = writer.sheets['Summary']
                    worksheet.set_column('A:A', 25)  # Metric names
                    worksheet.set_column('B:B', 15, currency_format)  # Values
                    
                    # Add header formatting
                    for col_num, value in enumerate(df_summary.columns.values):
                        worksheet.write(0, col_num, value, header_format)
            
            # Log results
            logger.info(f"✅ Excel report generated: {filename}")
            logger.info(f"📈 Report contains {len(report_data)} order items")
            logger.info(f"💰 Total Revenue: ${summary_data.get('Total_Net_Revenue', 0):,.2f}")
            logger.info(f"💸 Total COGS: ${summary_data.get('Total_COGS', 0):,.2f}")
            logger.info(f"📊 Total Profit: ${summary_data.get('Total_Gross_Profit', 0):,.2f}")
            logger.info(f"📋 Order Fill Rate: {summary_data.get('Order_Fill_Rate', 0):.2f}%")
            
            return filename
            
        except Exception as e:
            logger.error(f"❌ Error generating report: {e}")
            import traceback
            traceback.print_exc()
            return None

def main():
    try:
        generator = OrderReportGenerator()

        # Set date range - Fixed to use 2023 as mentioned in original comment
        start_date = datetime(2025, 7, 15, 0, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(2025, 7, 15, 23, 59, 59, tzinfo=timezone.utc)

        logger.info(f"🚀 Starting report generation for {start_date.date()} to {end_date.date()}")

        # Generate report
        filename = generator.generate_excel_report(start_date, end_date)

        if filename:
            logger.info(f"✅ Report saved to: {os.path.abspath(filename)}")
            return filename
        else:
            logger.error("❌ Failed to generate report")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()
        return None

# Additional utility functions
def generate_date_range_report(start_date: str, end_date: str, marketplace: str = None):
    """Generate report for a custom date range"""
    try:
        generator = OrderReportGenerator()
        
        # Parse date strings
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        
        return generator.generate_excel_report(start_dt, end_dt, marketplace)
        
    except Exception as e:
        logger.error(f"❌ Error generating date range report: {e}")
        return None

def generate_monthly_report(year: int, month: int, marketplace: str = None):
    """Generate report for a specific month"""
    try:
        generator = OrderReportGenerator()
        
        # Calculate month start and end dates
        start_date = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
        
        # Calculate last day of month
        if month == 12:
            next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        
        end_date = next_month - timedelta(seconds=1)
        
        return generator.generate_excel_report(start_date, end_date, marketplace)
        
    except Exception as e:
        logger.error(f"❌ Error generating monthly report: {e}")
        return None

def validate_database_connection():
    """Validate database connection and models"""
    try:
        generator = OrderReportGenerator()
        
        # Test basic queries
        order_count = Order.objects.count()
        marketplace_count = Marketplace.objects.count()
        product_count = Product.objects.count()
        
        logger.info(f"📊 Database validation:")
        logger.info(f"  Orders: {order_count}")
        logger.info(f"  Marketplaces: {marketplace_count}")
        logger.info(f"  Products: {product_count}")
        orders = Order.objects()
        print("🧾 All Orders:")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database validation failed: {e}")
        return False

if __name__ == "__main__":
    # You can also run validation first
    if validate_database_connection():
        main()
    else:
        logger.error("❌ Database validation failed. Please check your connection and models.")
        
    # Example usage for different scenarios:
    # generate_date_range_report('2023-07-01', '2023-07-31', 'Amazon')
    # generate_monthly_report(2023, 7, 'Amazon')