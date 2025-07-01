import os
from pathlib import Path
from dotenv import load_dotenv
from corsheaders.defaults import default_headers

# Load environment variables from .env
load_dotenv()

# Build base directory
BASE_DIR = Path(__file__).resolve().parent.parent

# SECURITY WARNING: keep secret key in .env
SECRET_KEY = os.getenv('SECRET_KEY')
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

ALLOWED_HOSTS = [
    "34.195.154.218",         # EC2 IP
    "localhost",
    "127.0.0.1",
    "b2bop.netlify.app",
]

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'corsheaders',
    'rest_framework_simplejwt',
    'django_celery_beat',
    'omnisight',
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'ecommerce_tool.custom_mideleware.customMiddleware',  # custom middleware
]

ROOT_URLCONF = 'ecommerce_tool.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'ecommerce_tool.wsgi.application'

# MongoDB connection
from mongoengine import connect
connect(
    db=os.getenv('DATABASE_NAME'),
    host=os.getenv('DATABASE_HOST'),
)

# REST Framework config
REST_FRAMEWORK = {
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'rest_framework.authentication.SessionAuthentication',
        'rest_framework.authentication.TokenAuthentication',
        'rest_framework_simplejwt.authentication.JWTAuthentication',
    ),
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
}

# CORS config
CORS_ALLOW_ALL_ORIGINS = False
CORS_ALLOW_CREDENTIALS = True
CORS_ALLOWED_ORIGINS = [
    "http://34.195.154.218",
    "http://localhost:3000",
    "http://192.168.30.191:4200",
    "https://b2bop.netlify.app",
]
CORS_ALLOW_HEADERS = list(default_headers) + ['content-type']
CSRF_TRUSTED_ORIGINS = [
    "http://34.195.154.218",
    "http://localhost:3000",
    "http://192.168.30.191:4200",
    "https://b2bop.netlify.app",
]

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Localization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_TZ = True

# Static files
STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# Security settings (enable in production)
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG
SECURE_SSL_REDIRECT = not DEBUG
SECURE_HSTS_SECONDS = 31536000 if not DEBUG else 0
SECURE_HSTS_INCLUDE_SUBDOMAINS = not DEBUG
SECURE_HSTS_PRELOAD = not DEBUG
X_FRAME_OPTIONS = 'DENY'

# Celery
CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'

# API Keys
WALMART_API_KEY = os.getenv('WALMART_API_KEY')
WALMART_SECRET_KEY = os.getenv('WALMART_SECRET_KEY')
AMAZON_API_KEY = os.getenv('AMAZON_API_KEY')
AMAZON_SECRET_KEY = os.getenv('AMAZON_SECRET_KEY')
REFRESH_TOKEN = os.getenv('AMAZON_REFRESH_TOKEN')
MARKETPLACE_ID = os.getenv('MARKETPLACE_ID')
SELLER_ID = os.getenv('SELLER_ID')
Role_ARN = os.getenv('Role_ARN')
Acccess_Key = os.getenv('Acccess_Key')
Secret_Access_Key = os.getenv('Secret_Access_Key')
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SELLERCLOUD_USERNAME = os.getenv('SELLERCLOUD_USERNAME')
SELLERCLOUD_PASSWORD = os.getenv('SELLERCLOUD_PASSWORD')
SELLERCLOUD_COMPANY_ID = os.getenv('SELLERCLOUD_COMPANY_ID')
SELLERCLOUD_SERVER_ID = os.getenv('SELLERCLOUD_SERVER_ID')
