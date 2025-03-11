"""
URL configuration for data_extraction_ai project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from omnisight.operations.common_operations import checkEmailExistOrNot, signupUser,loginUser, forgotPassword, changePassword
from omnisight.operations.walmart_operations import fetchAllProducts,fetchProductDetails, fetchAllorders, fetchOrderDetails, fetchBrand
from omnisight.operations.general_functions import getMarketplaceList, getProductList, getProductCategoryList, getBrandList

from omnisight.operations.amazon_operations import updateAmazonProductsBasedonAsins

urlpatterns = [
    
    #General Urls
    path('checkEmailExistOrNot/', checkEmailExistOrNot, name='checkEmailExistOrNot'),
    path('signupUser/', signupUser, name='signupUser'),
    path('loginUser/', loginUser, name='loginUser'),
    path('forgotPassword/',forgotPassword,name="forgotPassword"),
    path('changePassword/',changePassword,name="changePassword"),
    path('fetchAllProducts/',fetchAllProducts,name="fetchAllProducts"),
    path('fetchProductDetails/',fetchProductDetails,name='fetchProductDetails'),
    path('fetchAllorders/',fetchAllorders,name='fetchAllorders'),
    path('fetchOrderDetails/',fetchOrderDetails,name='fetchOrderDetails'),

    path('fetchBrand/',fetchBrand,name='fetch Brand'),


    #GENERAL URLS
    path('getMarketplaceList/',getMarketplaceList,name="getMarketplaceList"),
    path('getProductList/',getProductList,name="getProductList"),
    path('getProductCategoryList/',getProductCategoryList,name="getProductCategoryList"),
    path('getBrandList/',getBrandList,name="getBrandList"),


    #AMAZON URLS
    path('updateAmazonProductsBasedonAsins/',updateAmazonProductsBasedonAsins,name="updateAmazonProductsBasedonAsins")
    


]


