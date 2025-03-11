import requests
import base64
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from ecommerce_tool.settings import AMAZON_API_KEY, AMAZON_SECRET_KEY, REFRESH_TOKEN
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import access_token
from datetime import datetime, timedelta
from bson import ObjectId



def get_access_token():
    TOKEN_URL = "https://api.amazon.com/auth/o2/token"
    """Retrieve access token using refresh token."""
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": AMAZON_API_KEY,
        "client_secret": AMAZON_SECRET_KEY
    }
    
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(TOKEN_URL, data=payload, headers=headers)
    
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print("Error getting access token:", response.text)
        return None
    

def getAccesstoken(user_id):
    exist_access_token_obj = DatabaseModel.get_document(access_token.objects,{"user_id" : user_id})
    if exist_access_token_obj != None:
        # Get the current time
        current_time = datetime.now()

        # Get the creation time of the access token
        creation_time = exist_access_token_obj.updation_time

        # Check if the current time is greater than the creation time plus 14 minutes
        if current_time < creation_time + timedelta(minutes=14):
            access_token_str = exist_access_token_obj.access_token_str
        else:
            access_token_str = get_access_token()
            if access_token_str != None:
                DatabaseModel.update_documents(access_token.objects,{"id" : exist_access_token_obj.id},{"access_token_str" : access_token_str,"updation_time" : datetime.now()})
    else:
        access_token_str = get_access_token()
        DatabaseModel.save_documents(access_token,{"user_id" : ObjectId(user_id),"access_token_str" : access_token_str})
    return access_token_str