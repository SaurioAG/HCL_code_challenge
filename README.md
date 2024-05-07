# HCL_code_challenge
Challenge to demonstrate ETL skills

This Repository contains the following:

1. Data Engineer Challenge.ipynb: A Interactive Python Notebook which sumarizes all the instructions, answers and source code that solves every single challenge proposed bu HCL
2. code_challenge_sql_and_etl.py: Python script that handles the SQL and ETL challenges
3. code_challenge_airflow.py: Python script that handles the airflow challenge
4. code_challenge_api.py: Python script that handles API endpoint request for data
5. test_code_challenge_api.py: Extra Python script that makes unit testing for the functions in code_challenge_api.py

## Technical Questions

1. Data Modeling and Warehousing
   
Question: How do you design a schema for a new data-intensive application?

    1. First of all it is important to identify where is data comming from and on which format type
    2. Then a check up on the data is needed to understand it and get as much familiar as possible to it
    3. once checked, it is important to visualize if theres duplicated data/relations that can be normalized as mcuh as possible (depending on project nature/requirements)
    4. At this point after normalization, Fact table(s) and Dimension table(s) are now identified
    5. It is now time to generate an Entity-Relationship Diagram (ERD) with proper one-one or one-many relations
    6. Once ERD is completed, Schema can be generation based on the ERD. Paying attention on column data types and Keys
    7. When Schema is finalized it is now time to ingest/dump data into tables (tables population)

    NOTE: Make sure the source data is suitable to comply with schema requirements (data types, Nulls, etc). Also a good practice
    is to clean data as much as possible (removing unexpected characters, blank spaces, duplicated values, etc)

3. ETL/ELT Design and Implementation
   
Question: Describe a complex ETL pipeline you've designed. What were some of the challenges, and how did you address them?
    
    Electric Transmission Diagrams Migration Project:
    
    The requirement was to gather all required information from source data (provided by customer), then use all that extracted information and transform it to comply with some
    previously defined conversion rules. This conversion rules had to be designed due to the migration was being done between systems of different suppliers. Once data were transformed
    it was used to generate XML files, that will later be loaded into the new system which consumed those XML to populate its operational database.
    All this process was divided on different stages as follows:

    1. Identify data sources: Two data sources were identified, a set of 8k+ SVG files (Containing electric diagram data) and a MySQL database (containing electric network SCADA model data)
    2. Old vs New system mapping: In order to identify common functionalities/atributes between systems and differences.
    3. Conversion rules: from the mapping, the differences between systems were spotted, having to generate migration rules to transform/modify the differences to make them fit on the new system
    4. Once having all needed data and insights to generate a correct migration process, a Python ELT tool was developed with the following stages:
        - Extract data: This stage looped over all the 8k+ SVG files, parsed them and extracted all attribute,values from every tag, generating a list containing a dictionary per object, which
        were later dumped into a CSV file
        - Load data: On this stage, all the CSV files generated on previous stage were dumped into a MySQL DB in order to generate a warehouse on premises
        - Transform data: This stage used the data on the warehouse and transformed it based on the conversion rules. Also this transformation phase
        considered the SCADA model database, transforming the combination of this two sources in order to generate suitable data for the new system that was stored in new tables.
        The data in this new tables was used to generate an XML version of the source SVG files, which will later be used to populate the new system operational data base.

    NOTE: The procedure documentation can be found on my GitHub: https://github.com/SaurioAG/Electric_Transmission_Diagrams_Migration Code is not shared
    due to No Disclosure Agreement (NDA) with customer.

5. Data Infrastructure and Orchestration
   
Question: What experience do you have with managing data infrastructure on-premises or in the cloud?

    Most of my experience and projects have been on-premises enviroments.
    Databases and systems allocated on company servers, which were deployed by system integrators.
    I personaly do not have experience deploying physical infrastructure, just managing/using resources of an already deployed enviroment.
    Deploying and using ETLs, populating  relational data bases and querying data from DBs.
    Unfortunately I haven't had commercial experience with cloud enviroments.
    However I'm willing and open to learn about cloud services, AWS and GCP are on my scope.

7. Programming and Software Engineering Practices
   
Question: What programming languages are you most comfortable with, and what libraries do you frequently use in data engineering projects?

    Most of my experience at programming is with Python 3.10. Also I have used a variety of libraries across all the different projects I've worked on like:
    - For Data Wrangling/Analysis: pandas, numpy, PySpark, re
    - For Data Orchestration: airflow
    - For DB connections: sqlite3, psycopg2, mysql
    - For OS interaction: os, sys, glob
    - For Web scraping: requests
    - For HTML/XML/SVG parsing: bs4, xml

    Also I'm proficient at querying with SQL

9. Data Security and Compliance
    
Question: How do you implement security measures in your data engineering projects?

    On DB side:
        Encrypting table sensitive information.
        Managing user privileges/authorities/access
    On Code:
        Avoid harcoding information that can compromise servers/data
        Use enviroment variables to retrieve data like users, passwords, mac addresses, ip addresses
    On server OS:
        User Authentication
        Audit Logging and Monitoring

Please explain how you ussually follow CI/CD pipelines

    Some of the practices I consider to preserve a good CI/CD pipelines health are:
    1. Using version control: To have an historic of bug fixes, enhancements, new features, customer change requirements, etc.
    2. Code Testing: wether unit testing, integrtation testing, smoke testing, end to end testing.
    3. Iterative improvement: Code is never finished, as it can be continuously improved due to previous projects lessons learned, customer feedback, customer change requirements.

## Code Challenges Details

### code_challenge_sql_and_etl.py

Objective:
You are tasked with creating a Python application that interfaces with a MySQL database. The application will manage a dataset representing sales data for a tech company that sells various products across multiple countries. Your goal is to establish a database connection, create a table, and populate this table with sample data.

Tasks:

Create a Database Connection:
Implement a Python function to establish a connection to a MySQL database using provided credentials (host, username, password, and database name).

Define and Create a Table:
Write SQL commands within your Python script to create a table named sales. This table should have columns for id, country, category, price, quantity, and final_sales, with appropriate data types.

Insert Data:
Prepare a series of SQL INSERT statements to populate the sales table with the provided sample data. Ensure each record accurately reflects the sales data format.

Execute Queries:
Write functions to execute SQL queries to create the table and insert data into the table. Include error handling to manage potential SQL execution errors.

Installation:
This script requires to install the following libraries:

```
pip install mysql pandas
```

Script will ask for your MySQL database information: host, database name, user and password

Once given, It will generate random data and create DB,Schema, tables and populate them with the randomly generated data. You will end up with a DB structure like this

![db_structure](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/301fa86c-eea3-4793-a8b9-347f23f50f57)

And the tables will look like this

![sales_table](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/855444b4-1ffe-4015-aa97-f46340c46e53)

![product_table](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/c640e727-c1c3-4cc0-9393-e4874935c535)

![top_sell_country_category](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/03d06d05-2b79-4095-ab87-bb22bb47f13f)

![transformed_sales](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/693907f3-cda3-49c3-a384-8b3f795ff19b)

================================================================================

### code_challenge_airflow.py

Objective:
The goal of this challenge is to develop a fully functional Airflow Directed Acyclic Graph (DAG) that orchestrates an ETL (Extract, Transform, Load) process. This process involves extracting data from a source, transforming this data, and loading it into a destination system.

Background:
Automating ETL tasks is crucial for ensuring data accuracy and availability in real-time or near-real-time for analysis and decision-making. Airflow is a platform used to programmatically author, schedule, and monitor workflows.

Tasks:

Complete the Python Functions:
Extract Function: Implement logic to extract data from a predefined data source. This could be a database, a file, an API, or any simulated data source.
Transform Function: Apply necessary data transformations which could include cleaning, aggregating, or any other form of data manipulation.
Load Function: Implement the logic to load the transformed data into a specified target, which could be a database or a data warehouse.
Integrate Functions with Airflow:
Use the provided Airflow DAG skeleton to integrate your Python functions.
Configure the DAG to ensure that tasks are executed in the correct order, handling dependencies correctly.

Installation:
This script requires to install the following libraries:

```
pip install airflow requests numpy sqlite3 bs4

versions of airflow dependencies as follows:

Package                                  Version
---------------------------------------- -------------
adal                                     1.2.7
aiofiles                                 0.8.0
aiohttp                                  3.8.3
aiosignal                                1.2.0
alembic                                  1.8.1
amqp                                     5.1.1
anyio                                    3.6.2
apache-airflow                           2.4.2
apache-airflow-providers-amazon          6.0.0
apache-airflow-providers-celery          3.0.0
apache-airflow-providers-cncf-kubernetes 4.4.0
apache-airflow-providers-common-sql      1.2.0
apache-airflow-providers-docker          3.2.0
apache-airflow-providers-elasticsearch   4.2.1
apache-airflow-providers-ftp             3.1.0
apache-airflow-providers-google          8.4.0
apache-airflow-providers-grpc            3.0.0
apache-airflow-providers-hashicorp       3.1.0
apache-airflow-providers-http            4.0.0
apache-airflow-providers-imap            3.0.0
apache-airflow-providers-microsoft-azure 4.3.0
apache-airflow-providers-mysql           3.2.1
apache-airflow-providers-odbc            3.1.2
apache-airflow-providers-postgres        5.2.2
apache-airflow-providers-redis           3.0.0
apache-airflow-providers-sendgrid        3.0.0
apache-airflow-providers-sftp            4.1.0
apache-airflow-providers-slack           6.0.0
apache-airflow-providers-sqlite          3.2.1
apache-airflow-providers-ssh             3.2.0

```

The data is being extracted from the following website:

![source_data](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/a330c8ea-23ee-45b4-8b82-9e98f730c3b9)


NOTE: This code was tested and executed on an airflow instance deployed on docker container (with Linux OS). It should work on any Linux distro with correct python libraries.
DAG was placed on the default directory for Linux deployments: opt/airflow/dags
The output files generated by the script are stored at opt/airflow

DAG code:

![etl_process_dag_code](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/be6770d3-4708-4b28-9a5b-4d2644f83135)


The workflow graph of this DAG should look like this:

NOTE: When DAG is submited, you can manually trigger it from the "play" button to test functionality

![etl_process_dag_graph](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/3354fe8d-e656-4aa9-a011-c83be6ceddbe)

Airflow UI DAG list

![etl_process_dag_list](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/df2495cb-1d44-4ed0-a0c1-29717674a308)

Script:

![code_challenge_airflow](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/93afb366-7bcb-4684-a18d-e201a25f1896)

CLI Dags list:

![dags_list_unpaused](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/d9f6620c-214a-4153-9104-27ccca301b5e)

CLI output files:

![generated_files_by_etl](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/de27cc43-6c1f-463b-bd25-b0ff3b09a1bc)

You can open the bank_data.db file with sqlite3. Table will look like this:

![database](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/32737c60-8b80-4007-8077-735949299ed5)


=================================================================================

### code_challenge_api.py

Objective:
Develop a Python application that fetches data from an external API, applies specified transformations, and outputs the processed data. This challenge is designed to test your abilities in API interaction, data manipulation, and the application of basic data processing principles in Python.

Background:
APIs are a crucial data source in many software systems and data pipelines. Effective handling and integration of API data are key skills for developers and data engineers, involving tasks such as data extraction, transformation, and preparation for further analysis or storage.

Tasks:

Extract Data:
Write a Python function to fetch data from a given API endpoint. This function should handle network errors, API rate limits, and other common issues that can occur during API interaction.
Transform Data:
Implement logic to transform the raw data fetched from the API. Assume the data includes various product details; extract and format this data into a structured JSON format that focuses on specific fields like product_id, product_name, category, and price.
Output Data:
Instead of loading the data into a database or storage system, output the transformed data to the console or a file in a clean, readable format. This simulates the final step in an ETL process where data is made available for further use.

Installation:
This script requires to install the following libraries:

```
pip install requests pandas bs4 numpy datetime matplotlib
```

This script extracts data from a Pokemon API endpoint. Transforms it, generates CSV from both raw and transformed data and generates a log file for the process.
At the end it also generates an histogram.

Data is being extracted from the following url:

![source_data](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/92d16343-a53e-4cab-a086-938cdae84092)

The CSV with raw extracted data looks like:

![berry_data](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/66f72014-fe92-4a43-ab5b-6c5f0dc9e933)

Transformed data:

![transformed_data](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/f4b25ef6-0f55-4641-adb4-5a68fcf42250)

The generated histogram from transformed data:

![histogram](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/97167198-be14-47cb-bf01-8c985ef42ff6)

Log file:

![log_file](https://github.com/SaurioAG/HCL_code_challenge/assets/167505635/6b3e97f7-608e-404d-8719-0b9e366deb66)




