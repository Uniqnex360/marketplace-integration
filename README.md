 MarketLink

**Description:**

MarketLink is a comprehensive data integration platform designed to unify sales and product information from multiple e-commerce marketplaces, including Amazon, Walmart, TikTok Shop, and eBay. By connecting to these platforms via their respective APIs, MarketLink provides users with a centralized dashboard to monitor performance, analyze trends, and streamline their multi-channel e-commerce operations.

**Key Features:**

*   **Multi-Marketplace Integration:** Seamlessly connects to Amazon, Walmart, TikTok Shop, and eBay Seller APIs.
*   **Centralized Dashboard:** Provides a unified view of sales, product listings, and key performance indicators (KPIs).
*   **Automated Data Synchronization:** Automatically pulls and synchronizes data from all connected marketplaces.
*   **Customizable Reporting:** Generates customizable reports on sales trends, product performance, and other metrics.
*   **Real-time Monitoring:** Offers real-time monitoring of sales and inventory levels across all marketplaces.
*   **Secure Authentication:** Implements secure authentication and authorization protocols for accessing marketplace APIs.
*   **Extensible Architecture:** Designed with a modular architecture for easy integration of additional marketplaces and features.

**Use Cases:**

*   E-commerce businesses selling on multiple platforms
*   Brands seeking to consolidate their online sales data
*   Agencies managing multiple e-commerce accounts


**Project structure**
    ├── ecommerce_tool
│   ├── asgi.py
│   ├── celery.py
│   ├── crud.py
│   ├── custom_mideleware.py
│   ├── __init__.py
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── manage.py
├── omnisight
│   ├── admin.py
│   ├── apps.py
│   ├── __init__.py
│   ├── migrations
│   │   ├── __init__.py
│   │   └── __pycache__
│   ├── models.py
│   ├── operations
│   │   ├── amazon_operations.py
│   │   ├── amazon_utils.py
│   │   ├── common_operations.py
│   │   ├── common_utils.py
│   │   ├── general_functions.py
│   │   ├── helium_dashboard.py
│   │   ├── helium_utils.py
│   │   ├── __init__.py
│   │   ├── __pycache__
│   │   ├── walmart_operations.py
│   │   └── walmart_utils.py
│   ├── tasks.py
│   ├── tests.py
│   ├── urls.py
│   └── views.py
└── project_structure.txt



**Project Setup & Prerequisites**
    **Prerequisites**
        Before running the project, ensure the following are installed on your system:

            * Python: 3.8.10

            * Django: 4.2.19

            * MongoDB: Make sure MongoDB is installed and running

           * Operating System: Ubuntu 20.04.6 LTS (or compatible)

    **Required Python Packages**
        All required packages are listed in the requirements.txt file.
            To install them, run:
            pip install -r requirements.txt


    **How to Run the Django Project**
        1) Clone the repository
            git clone https://github.com/KM-Digicommerce/Marketlink
            cd ecommerce_tool

        2) Create a virtual environment (recommended)
            python3 -m venv venv
            source venv/bin/activate

        3) Install dependencies
            pip install -r requirements.txt

        4) Run the Django server
            python manage.py runserver

        5) Visit http://127.0.0.1:8000/ in your browser to access the app.


**Deployment Instructions (AWS - Ubuntu Server)**
    The main branch is deployed on an AWS EC2 instance running Ubuntu. Follow the steps below to deploy the Django project:
    **Server Setup (One-time)**
        1) Connect to your AWS instance
            ssh ubuntu@<your-ec2-public-ip>

        2) Install system dependencies
            sudo apt update
            sudo apt install python3-pip python3-venv nginx git -y

        3) Install and start MongoDB
            (if MongoDB is hosted locally; skip if using Atlas/cloud-hosted MongoDB)
            sudo apt install -y mongodb
            sudo systemctl enable mongodb
            sudo systemctl start mongodb

        4) Clone the project repository
            git clone https://github.com/KM-Digicommerce/Marketlink
            cd ecommerce_tool

        5) Set up a virtual environment
            python3 -m venv venv
            source venv/bin/activate

        6) Install dependencies
            pip install -r requirements.txt

        7) Run the Django server
            python manage.py runserver 0.0.0.0:8000

        8) Access the application
            Open your browser and go to:
            http://<your-ec2-public-ip>:8000/
    
    **Important Notes**

        Make sure the following ports are open in your EC2 security group:

        * Port 8000 → For Django development server
            (Inbound Rule: Custom TCP | Port 8000 | Source: 0.0.0.0/0)

        * Port 27017 → For MongoDB access (if accessed remotely)
            (Inbound Rule: Custom TCP | Port 27017 | Source: [your IP or 0.0.0.0/0 for all])

**Environment Configuration (.env)**
    This project uses a .env file to securely manage sensitive environment variables such as API credentials, database URIs, and secret keys.
    1) You can find the template for the .env file at:
        MarketLink/ecommerce_tool/ecommerce_tool/templateenv.txt

    2) To set up your local environment:
        Copy the template file and rename it to .env:
            cp ecommerce_tool/templateenv.txt ecommerce_tool/.env

    3) Fill in the required values using the credentials provided for your environment (e.g., development or production).

    4) The actual .env file (with values) should be placed at:
            MarketLink/ecommerce_tool/ecommerce_tool/.env

    **Important**
        Do not commit or push the .env file (with actual values) to the Git repository under any circumstances. This file is intentionally ignored using .gitignore for security reasons.
