from mongoengine import Document, StringField, FloatField, IntField, BooleanField, DictField, ListField, EmbeddedDocument, EmbeddedDocumentField,ReferenceField, DateTimeField
from mongoengine.errors import ValidationError
from datetime import datetime
import re


class Marketplace(Document):
    name = StringField(required=True, unique=True)  # Marketplace name
    url = StringField(required=True)  # Marketplace URL
    created_at = StringField()  # Timestamp when the marketplace was added
    updated_at = StringField()  # Timestamp when the marketplace was last updated




class Category(Document):
    name = StringField(required=True, unique=True)  # Category name
    parent_category = StringField()  # Parent category (if applicable)
    marketplace_id = ReferenceField(Marketplace)  # Reference to the marketplace
    breadcrumb_path = ListField(StringField())  # Hierarchical category path
    level = IntField()  # Category level
    created_at = StringField()  # Timestamp when the category was added
    updated_at = StringField()  # Timestamp when the category was last updated



class Price(EmbeddedDocument):
    currency = StringField(required=True)  # e.g., 'USD'
    amount = FloatField(required=True)  # e.g., 38.19

class Product(Document):
    marketplace_id = ReferenceField(Marketplace)  # Reference to the marketplace
    sku = StringField(required=True, unique=True)  # Stock Keeping Unit
    wpid = StringField()  # Walmart Product ID
    asin = StringField()  # Amazon Standard Identification Number
    upc = StringField()  # Universal Product Code
    gtin = StringField()  # Global Trade Item Number
    product_name = StringField(required=True)  # Name of the product
    category = StringField()  # General category
    shelf_path = ListField(StringField())  # Breadcrumb path (for Walmart)
    product_type = StringField()  # Specific product classification
    brand = StringField()  # Brand of the product
    manufacturer = StringField()  # Manufacturer Name
    condition = StringField(choices=['New', 'Used', 'Refurbished'], default='New')  # Product condition
    availability = StringField(choices=['In_stock', 'Out_of_stock'], default='In_stock')  # Availability status
    price = EmbeddedDocumentField(Price)  # Nested price object
    published_status = StringField()  # e.g., SYSTEM_PROBLEM
    unpublished_reasons = DictField()  # Reasons if product is unpublished
    lifecycle_status = StringField(choices=['ACTIVE', 'INACTIVE'], default='ACTIVE')  # Whether product is still being sold
    is_duplicate = BooleanField(default=False)  # Whether it's a duplicate listing
    created_at = StringField()  # Timestamp when the product was added
    updated_at = StringField()  # Timestamp when the product was last updated



class ignore_api_functions(Document):
    name = StringField()



class mail_template(Document):
    code = StringField()
    subject = StringField()
    default_template = StringField()
    cutomize_template = StringField()


def validateEmail(value):
    mail_re = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if(not re.fullmatch(mail_re, value)):
        raise ValidationError("Email format is invalid")

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

