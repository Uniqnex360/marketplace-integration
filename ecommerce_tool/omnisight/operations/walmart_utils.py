import requests
import base64
import uuid
import xml.etree.ElementTree as ET  # Import XML parser
from ecommerce_tool.settings import WALMART_API_KEY, WALMART_SECRET_KEY
from ecommerce_tool.crud import DatabaseModel
from omnisight.models import access_token, Marketplace
from datetime import datetime, timedelta
from bson import ObjectId
import json



def oauthFunction():
    accesstoken = None
    # Walmart Authentication URL
    AUTH_URL = "https://marketplace.walmartapis.com/v3/token"

    # Encode credentials in Base64
    credentials = f"{WALMART_API_KEY}:{WALMART_SECRET_KEY}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    # Headers for the request
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}",
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),  # Unique request ID
        "WM_SVC.NAME": "Walmart Marketplace"  # Service name
    }

    # Request body
    data = {
        "grant_type": "client_credentials"
    }

    # Send authentication request
    response = requests.post(AUTH_URL, headers=headers, data=data)

    # # Debugging: Print Response Status and Content
    # print("Response Status Code:", response.status_code)
    # print("Response Headers:", response.headers)
    # print("Response Text:", response.text)

    # Check response status
    if response.status_code == 200:
        try:
            # Parse the XML response
            root = ET.fromstring(response.text)
            accesstoken = root.find("accessToken").text
            # print(products(access_token))

            print("✅ Walmart API credentials are valid!")
            # print("Access Token:", access_token)
            # print("Expires In:", expires_in, "seconds")
        except Exception as e:
            print("❌ Error parsing Walmart API XML response:", str(e))
    else:
        print("❌ Authentication failed. Please check your Client ID and Secret.")

    return accesstoken


def getAccesstoken(user_id):
    marketplace_id = DatabaseModel.get_document(Marketplace.objects,{"name" : "Walmart"},['id']).id
    exist_access_token_obj = DatabaseModel.get_document(access_token.objects,{"user_id" : user_id,"marketplace_id" : marketplace_id},['access_token_str','updation_time'])
    if exist_access_token_obj != None:
        # Get the current time
        current_time = datetime.now()

        # Get the creation time of the access token
        creation_time = exist_access_token_obj.updation_time

        # Check if the current time is greater than the creation time plus 14 minutes
        if current_time < creation_time + timedelta(minutes=14):
            access_token_str = exist_access_token_obj.access_token_str
        else:
            access_token_str = oauthFunction()
            if access_token_str != None:
                DatabaseModel.update_documents(access_token.objects,{"id" : exist_access_token_obj.id},{"access_token_str" : access_token_str,"updation_time" : datetime.now()})
    else:
        access_token_str = oauthFunction()
        DatabaseModel.save_documents(access_token,{"user_id" : ObjectId(user_id),"access_token_str" : access_token_str,"marketplace_id" : marketplace_id})
    return access_token_str