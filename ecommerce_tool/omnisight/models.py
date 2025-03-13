from mongoengine import Document, StringField, FloatField, IntField, BooleanField, DictField, ListField, EmbeddedDocument, EmbeddedDocumentField,ReferenceField, DateTimeField
from mongoengine.errors import ValidationError
from datetime import datetime
import re


class Marketplace(Document):
    name = StringField()  # Marketplace name
    url = StringField()  # Marketplace URL
    image_url = StringField()  # Marketplace logo URL
    created_at = StringField()  # Timestamp when the marketplace was added
    updated_at = StringField()  # Timestamp when the marketplace was last updated




class Category(Document):
    name = StringField(required=True, unique=True)  # Category name
    parent_category_id = ReferenceField('self', null=True)  # Parent category (if applicable)
    marketplace_id = ReferenceField(Marketplace)  # Reference to the marketplace
    breadcrumb_path = ListField(StringField())  # Hierarchical category path
    level = IntField()  # Category level
    created_at = StringField()  # Timestamp when the category was added
    updated_at = StringField()  # Timestamp when the category was last updated
    end_level = BooleanField(default=False)  # Whether the category is the last level in the hierarchy


class Brand(Document):
    name = StringField()  # Brand name
    description = StringField()  # Brand description
    website = StringField()  # Brand website
    marketplace_id = ReferenceField(Marketplace)  # Reference to the marketplace



class Manufacturer(Document):
    name = StringField()  # Manufacturer name
    description = StringField()  # Manufacturer description
    website = StringField()  # Manufacturer website
    marketplace_id = ReferenceField(Marketplace)  # Reference to the marketplace




class Product(Document):
    # General Product Details
    product_title = StringField()
    product_description = StringField()
    product_id = StringField()  # Can store ASIN, UPC, GTIN, WPID
    product_id_type = StringField()
    sku = StringField()
    price = FloatField()
    currency = StringField()
    quantity = IntField(default=0)
    quantity_unit = StringField()
    item_condition = StringField()
    item_note = StringField()  # Additional notes about item condition

    # Platform-Specific Identifiers
    listing_id = StringField()  # Amazon Listing ID
    upc = StringField()  # Universal Product Code
    gtin = StringField()  # Global Trade Item Number
    asin = StringField()  # Amazon Standard Identification Number
    model_number = StringField()
    
    # Image & Category
    image_url = StringField() # Main image URL
    image_urls = ListField(StringField())  # Additional image URLs
    zshop_category = StringField()
    zshop_browse_path = StringField()
    
    # Shipping Details
    delivery_partner = StringField()
    merchant_shipping_group = StringField()
    will_ship_internationally = BooleanField(default=False)
    expedited_shipping = BooleanField(default=False)
    zshop_shipping_fee = StringField()

    # Listing & Availability
    open_date = DateTimeField()
    availability = StringField()
    lifecycle_status = StringField()  # e.g., "Active", "Inactive", "Discontinued"
    published_status = StringField()
    unpublished_reasons = StringField()  # If unpublished, list reasons
    
    # Variants & Grouping
    variant_group_id = StringField()
    variant_group_info = DictField()
    
    # Platform-Specific Extras
    zshop_storefront_feature = BooleanField(default=False)
    zshop_boldface = BooleanField(default=False)
    bid_for_featured_placement = BooleanField(default=False)
    
    # Other Metadata
    add_delete = StringField()
    pending_quantity = IntField(default=0)
    is_duplicate = BooleanField(default=False)
    shelf_path = StringField()  # Walmart shelf location
    product_type = StringField()  # e.g., Electronics, Clothing

    category = StringField()  # e.g., "Electronics > Computers > Laptops"
    attributes = DictField()  # Additional attributes (e.g., color, size, weight)
    features = ListField(StringField())  # List of product features
    brand_name = StringField()  # Brand name
    brand_id = ReferenceField(Brand)  # Reference to the brand

    manufacturer_name = StringField()  # Manufacturer name
    manufacturer_id = ReferenceField(Manufacturer)  # Reference to the manufacturer
    marketplace_id = ReferenceField(Marketplace)  # Reference to the marketplace
    created_at = DateTimeField(default=datetime.now())  # Timestamp when the product was added
    updated_at = DateTimeField(default=datetime.now())  # Timestamp when the product was last updated


class ignore_api_functions(Document):
    name = StringField()



class mail_template(Document):
    code = StringField()
    subject = StringField()
    default_template = StringField()
    cutomize_template = StringField()


class role(Document):
    name = StringField()
    description = StringField()
    priority = IntField()


class user(Document):
    first_name = StringField()
    last_name = StringField()
    username = StringField()
    email = StringField(required=True)
    password = StringField()
    age = IntField()
    date_of_birth = StringField()
    mobile_number = StringField()
    active = BooleanField(default=True)
    profile_image = StringField()
    role_id = ReferenceField(role)
    otp = IntField()
    is_verified = BooleanField(default=False)
    otp_generated_time = DateTimeField(default=datetime.now())
    last_login = DateTimeField(default=datetime.now())
    creation_date = DateTimeField(default=datetime.now())
    credentilas = ListField(DictField())


class access_token(Document):
    user_id = ReferenceField(user)
    access_token_str = StringField()
    creation_time = DateTimeField(default=datetime.now())
    updation_time = DateTimeField(default=datetime.now())
    marketplace_id = ReferenceField(Marketplace)



class Order(Document):
    # Tracking IDs
    purchase_order_id = StringField()  # ID generated after a customer orders a product
    customer_order_id = StringField()  # ID from the customer's perspective for tracking
    seller_order_id = StringField()  # ID used by the seller for internal purposes

    # Customer details
    customer_email_id = StringField()  # Email of the customer

    # Order timing
    order_date = DateTimeField()  # Date when the order was placed
    earliest_ship_date = DateTimeField()  # Minimum date for product delivery
    latest_ship_date = DateTimeField()  # Maximum date for product delivery
    last_update_date = DateTimeField()  # Most recent order update date

    # Shipping information
    shipping_information = DictField()  # Contains nested address/details for customer delivery
    ship_service_level = StringField()  # Shipping speed or service
    shipment_service_level_category = StringField()  # High-level service category (e.g., express, standard)
    automated_shipping_settings = DictField()  # Amazon settings for automatically selecting shipping methods

    # Order details and status
    order_details = ListField(DictField())  # List of order lines with details for each ordered item
    order_status = StringField()  # Tracking order lifecycle
    number_of_items_shipped = IntField()  # Number of items that have been shipped
    number_of_items_unshipped = IntField()  # Number of items pending shipment

    # Fulfillment and sales
    fulfillment_channel = StringField()  # Who is fulfilling the order
    sales_channel = StringField()  # The channel through which the order was placed
    order_type = StringField()  # Order type based on purchase basis
    is_premium_order = BooleanField()  # Indicates whether premium cost applies for non-prime members
    is_prime = BooleanField()  # True if the order is placed by a prime account
    has_regulated_items = BooleanField()  # True if order includes items requiring special handling
    is_replacement_order = BooleanField()  # True if the order is a replacement due to defects, lost items, etc.
    is_sold_by_ab = BooleanField()  # True if the product is sold directly by Amazon
    is_ispu = BooleanField()  # True if the order is meant for in-store pickup
    is_access_point_order = BooleanField()  # True if the order uses an access point location (e.g., lockers)
    is_business_order = BooleanField()  # True if this is a business (B2B) order

    # Marketplace and payment
    marketplace = StringField()  # Marketplace from which the order originated
    marketplace_id = ReferenceField(Marketplace)  # Identifier for the marketplace
    payment_method = StringField()  # Type of payment used
    payment_method_details = StringField()  # Detailed information about the payment method
    order_total = FloatField()  # Total order cost including products, shipping, and taxes
    currency = StringField()  # Currency used for the order
    is_global_express_enabled = BooleanField()  # True if fast shipping is available for international orders