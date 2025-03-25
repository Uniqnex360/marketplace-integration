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
from omnisight.operations.walmart_operations import updateOrdersItemsDetails, fetchAllorders1, syncRecentWalmartOrders
from omnisight.operations.general_functions import getMarketplaceList, getProductList, getProductCategoryList, getBrandList, fetchProductDetails,fetchAllorders, fetchOrderDetails, ordersCountForDashboard,totalSalesAmount, getOrdersBasedOnProduct, createManualOrder, getProductListForOrdercreation,listManualOrders, fetchManualOrderDetails, getSalesTrendPercentage, fetchSalesSummary, salesAnalytics, mostSellingProducts, fetchTopSellingCategories

from omnisight.operations.amazon_operations import updateAmazonProductsBasedonAsins, updateOrdersItemsDetailsAmazon, syncRecentAmazonOrders

urlpatterns = [
    
    #General Urls
    path('checkEmailExistOrNot/', checkEmailExistOrNot, name='checkEmailExistOrNot'),
    path('signupUser/', signupUser, name='signupUser'),
    path('loginUser/', loginUser, name='loginUser'),
    path('forgotPassword/',forgotPassword,name="forgotPassword"),
    path('changePassword/',changePassword,name="changePassword"),
    path('fetchAllorders1/',fetchAllorders1,name="fetchAllorders1"),

    # path('fetchBrand/',fetchBrand,name='fetch Brand'),


    #GENERAL URLS
    path('getMarketplaceList/',getMarketplaceList,name="getMarketplaceList"),
    path('getProductList/',getProductList,name="getProductList"),
    path('getProductCategoryList/',getProductCategoryList,name="getProductCategoryList"),
    path('getBrandList/',getBrandList,name="getBrandList"),
    path('fetchProductDetails/',fetchProductDetails,name="fetchProductDetails"),
    path('fetchAllorders/',fetchAllorders,name="fetchAllorders"),
    path('fetchOrderDetails/',fetchOrderDetails,name='fetchOrderDetails'),
    path('getOrdersBasedOnProduct/',getOrdersBasedOnProduct,name='getOrdersBasedOnProduct'),


    #Dash Board Functions...........................
    path('ordersCountForDashboard/',ordersCountForDashboard,name='ordersCountForDashboard'),
    path('totalSalesAmount/',totalSalesAmount,name='totalSalesAmount'),
    path("getSalesTrendPercentage/",getSalesTrendPercentage,name="getSalesTrendPercentage"),
    path("fetchSalesSummary/",fetchSalesSummary,name="fetchSalesSummary"),
    path("salesAnalytics/",salesAnalytics,name="salesAnalytics"),
    path("mostSellingProducts/",mostSellingProducts,name='mostSellingProducts'),
    path("fetchTopSellingCategories/",fetchTopSellingCategories,name="fetchTopSellingCategories"),

    #Custom Order
    path("getProductListForOrdercreation/",getProductListForOrdercreation,name="getProductListForOrdercreation"),
    path("createManualOrder/",createManualOrder,name="createManualOrder"),
    path("listManualOrders/",listManualOrders,name="listManualOrders"),
    path("fetchManualOrderDetails/",fetchManualOrderDetails,name="fetchManualOrderDetails"),


    #AMAZON URLS
    path('updateAmazonProductsBasedonAsins/',updateAmazonProductsBasedonAsins,name="updateAmazonProductsBasedonAsins"),
    path('updateOrdersItemsDetailsAmazon/',updateOrdersItemsDetailsAmazon,name="updateOrdersItemsDetailsAmazon"),
    path("syncRecentAmazonOrders/",syncRecentAmazonOrders,name="syncRecentAmazonOrders"),

    #Walmart
    path('updateOrdersItemsDetails/',updateOrdersItemsDetails,name='updateOrdersItemsDetails'),
    path("syncRecentWalmartOrders/",syncRecentWalmartOrders,name="syncRecentWalmartOrders")


]










