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
from omnisight.operations.general_functions import getMarketplaceList, getProductList, getProductCategoryList, getBrandList, fetchProductDetails,fetchAllorders, fetchOrderDetails, ordersCountForDashboard,totalSalesAmount, getOrdersBasedOnProduct, createManualOrder, getProductListForOrdercreation,listManualOrders, fetchManualOrderDetails, getSalesTrendPercentage, fetchSalesSummary, salesAnalytics, mostSellingProducts, fetchTopSellingCategories, updateManualOrder, fetchInventryList, exportOrderReport, createUser, updateUser, listUsers,fetchUserDetails, fetchRoles

from omnisight.operations.amazon_operations import updateAmazonProductsBasedonAsins, updateOrdersItemsDetailsAmazon, syncRecentAmazonOrders

from omnisight.operations.helium_dashboard import get_latest_orders,get_metrics_by_date_range,compare_revenue_with_past, LatestOrdersTodayAPIView, RevenueWidgetAPIView, get_top_products, getPeriodWiseData, getPeriodWiseDataCustom

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
    path('updateManualOrder/',updateManualOrder,name="updateManualOrder"),


    #AMAZON URLS
    path('updateAmazonProductsBasedonAsins/',updateAmazonProductsBasedonAsins,name="updateAmazonProductsBasedonAsins"),
    path('updateOrdersItemsDetailsAmazon/',updateOrdersItemsDetailsAmazon,name="updateOrdersItemsDetailsAmazon"),
    path("syncRecentAmazonOrders/",syncRecentAmazonOrders,name="syncRecentAmazonOrders"),

    #Walmart
    path('updateOrdersItemsDetails/',updateOrdersItemsDetails,name='updateOrdersItemsDetails'),
    path("syncRecentWalmartOrders/",syncRecentWalmartOrders,name="syncRecentWalmartOrders"),

    #Inventry
    path("fetchInventryList/",fetchInventryList,name="fetchInventryList"),
    path('exportOrderReport/',exportOrderReport,name="exportOrderReport"),


    #USER creation and Details

    path("createUser/",createUser,name="createUser"),
    path("updateUser/",updateUser,name="updateUser"),
    path("listUsers/",listUsers,name="listUsers"),
    path("fetchUserDetails/",fetchUserDetails,name="fetchUserDetails"),
    path("fetchRoles/",fetchRoles,name="fetchRoles"),

    #Helium 10 Dashboard
    path("get_latest_orders/",get_latest_orders,name="get_latest_orders"),
    path("get_metrics_by_date_range/",get_metrics_by_date_range,name="get_metrics_by_date_range"),
    path("compare_revenue_with_past/",compare_revenue_with_past,name="compare_revenue_with_past"),
    path("LatestOrdersTodayAPIView/",LatestOrdersTodayAPIView,name="LatestOrdersTodayAPIView"),
    path("RevenueWidgetAPIView/",RevenueWidgetAPIView,name="RevenueWidgetAPIView"),
    path("get_top_products/",get_top_products,name="get_top_products"),
    path("getPeriodWiseData/",getPeriodWiseData,name="getPeriodWiseData"),
    path("getPeriodWiseDataCustom/",getPeriodWiseDataCustom,name="getPeriodWiseDataCustom"),
]










