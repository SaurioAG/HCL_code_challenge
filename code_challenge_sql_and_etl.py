import mysql.connector as conn
import random
import pandas as pd

def db_conn(host_id: str, user_id: str, psw: str):
    """
    Handles the connection to a MySQL server to "sales" Data Base
    Receives DB connection credentials in order to connect to DB.
    """
    try:
        connection = conn.connect(
            host = host_id,
            # database = db_name,
            user = user_id,
            password = psw)
    except conn.errors.DatabaseError as error:
        if "2003" in  str(error):
            return print(f'An exception has occurred: {error}. Server is not reachable')
        elif "1045" in str(error):
            return print(f'An exception has occurred: {error}. Any of the provided credentials is incorrect')
        elif "1049" in str(error):
            """
            Unknown database 
            """
            pass
        else:
            return print(f'An exception has occurred: {error}.')
    return connection

def db_cursor(db_connection):
    """
    Generates a cursos object in order to handle queries to "sales" Data Base
    """
    cursor = db_connection.cursor()
    return cursor

def create_database(db_connection, cursor, db_name: str):
    """
    Creates sales DB
    """
    query = f'CREATE DATABASE IF NOT EXISTS {db_name}\
        CHARACTER SET=utf8mb4\
        COLLATE=utf8mb4_bin\
        ENCRYPTION="N"'
    cursor.execute(query)
    print(f'Creating Data Base')
    db_connection.commit()

def create_sales_table(db_connection, cursor, db_name: str):
    """
    Creates a table "sales" on the  Data base
    """
    try:
        query = f'CREATE TABLE {db_name}.sales (\
            product_id int NOT NULL ,\
            country varchar(256) NOT NULL,\
            category varchar(128) NOT NULL,\
            price double NOT NULL,\
            quantity int NOT NULL,\
            final_sales double NOT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin'
        
        cursor.execute(query)
        print(f'Creating sales table')
        db_connection.commit()
    except conn.errors.ProgrammingError as error:
        print(f'An exception has ocurred: {error}')
        response = input(f'Do you want to remove existing table and create a new one? (yes/no) ')
        if response.lower() == "yes":
            drop_query = f'DROP TABLE {db_name}.sales'
            cursor.execute(drop_query)
            db_connection.commit()
            cursor.execute(query)
            db_connection.commit()
            return print("New table sales has been created")
        else:
            return print("Keep exisitng sales table")

def generate_sales_data() -> list:
    """
    A function that generates a list of tupples with random sales data and a list with unique product ids
    """
    country = ["Mexico", "Canada", "US"]
    category = ["phone", "tablet", "laptop"]
    ids = [id for id in range(1, 11)]
    list_of_rows = [(random.choice(ids), random.choice(country),
                        random.choice(category), random.randint(3000, 30000),
                        random.randint(1,10), random.randint(3000, 100000))
                        for item in range(0, 100)]
    id_list = list(set([row[0] for row in list_of_rows]))
    return list_of_rows, id_list

def populate_sales_table(records: list, cursor, db_connection, db_name: str):
    """
    Receives a list of tupples that contain each row data for the "sales" table.
    Each tupple element should match the data type deffined on the table schema
    """
    try:
        full_query = f'INSERT INTO {db_name}.sales (product_id, country, category, price, quantity, final_sales) VALUES'
        for row in records:
            values = f'({row[0]}, "{row[1]}", "{row[2]}", {row[3]}, {row[4]}, {row[5]}),'
            if type(row[1]) != str or type(row[2]) != str:
                return print(f'You are trying to use a number on a string field. row: {row}')
            else:
                full_query = f'{full_query} {values}'
        cursor.execute(full_query[:len(full_query)-1])
        print(f'Populating sales table')
        db_connection.commit()
    except conn.errors.ProgrammingError as error:
        if "1064" in str(error):
            return print(f'You are trying to fill a field with an empty value. Fields do not accept NULL values.')
        elif "1054" in str(error):
            return print(f'You are trying to fill a field with incorrect data type.')
        else:
            return print(f'An exception has ocurred: {error}')
    except IndexError:
        return print(f'The row you are trying to populate is missing a value. It should have 6 values on it.')

def top_sell_country_category(db_connection, cursor, db_name: str):
    """
    Determines the top-selling product categories in each country.
    """
    try:
        query = f'CREATE TABLE {db_name}.top_sell_country_category AS (\
                    SELECT product_id, country, category, total\
                        FROM (\
                            SELECT product_id, country, category, sum(final_sales) AS total,\
                                ROW_NUMBER() OVER (PARTITION BY country ORDER BY SUM(final_sales) DESC) AS category_rank\
                                FROM {db_name}.sales\
                                GROUP BY country, category) AS rank_table\
                    WHERE category_rank = 1)'
        cursor.execute(query)
        print(f'Creating top_sell_country_category table')
        db_connection.commit()
        result = cursor.fetchall()
        dataframe = pd.DataFrame(result, columns=["Product_id", "Country", "Category", "Total_sales"])
    except conn.errors.ProgrammingError as error:
        print(f'An exception has ocurred: {error}')
        response = input(f'Do you want to remove existing table and create a new one? (yes/no) ')
        if response.lower() == "yes":
            drop_query = f'DROP TABLE {db_name}.top_sell_country_category'
            cursor.execute(drop_query)
            db_connection.commit()
            cursor.execute(query)
            db_connection.commit()
            cursor.execute(f'SELECT * FROM {db_name}.top_sell_country_category')
            result = cursor.fetchall()
            dataframe = pd.DataFrame(result, columns=["Product_id", "Country", "Category", "Total_sales"])
            print("New table top_sell_country_category has been created")
            print(dataframe)
            return None
        else:
            return print("Keep exisitng top_sell_country_category table")

def create_product_table(db_connection, cursor, db_name: str):
    """
    Creates a table "product" on the  Data base
    """
    try:
        query = f'CREATE TABLE {db_name}.product (\
            id int NOT NULL,\
            category varchar(128) NOT NULL,\
            capacity int NOT NULL,\
            color varchar(128) NOT NULL,\
            screen_size int NOT NULL,\
            memory int NOT NULL,\
            other_specs varchar(128) NOT NULL,\
            PRIMARY KEY (id)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin'
        
        cursor.execute(query)
        print(f'Creating product table')
        db_connection.commit()
    except conn.errors.ProgrammingError as error:
        print(f'An exception has ocurred: {error}')
        response = input(f'Do you want to remove existing table and create a new one? (yes/no) ')
        if response.lower() == "yes":
            drop_query = f'DROP TABLE {db_name}.product'
            cursor.execute(drop_query)
            db_connection.commit()
            cursor.execute(query)
            db_connection.commit()
            return print("New table product has been created")
        else:
            return print("Keep exisitng product table")

def generate_product_data(sales_ids: list) -> list:
    """
    A function that generates a list of tupples with random IT product data. Which receives a list of unique product ids from sales data.
    Te received list ensures to have unique product ids (Keys) on the product table.
    """
    list_of_rows = []
    ids = sales_ids
    category = ["phone", "tablet", "laptop"]
    capacity = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    color = ["white", "black", "red", "blue", "pink"]
    size = [5, 6, 7, 8, 9, 10, 12, 15, 17]
    specs = ["fast_charge", "usb_c", "5G"]
    list_of_rows = [(ids.pop(0), random.choice(category),
                     random.choice(capacity), random.choice(color),
                     random.choice(size), random.choice(capacity),
                     random.choice(specs)) for item in range(0, 10)]
    return list_of_rows

def populate_product_table(records: list, cursor, db_connection, db_name: str):
    """
    Receives a list of tupples that contain each row data for the "product" table.
    Each tupple element should match the data type deffined on the table schema
    """
    try:
        full_query = f'INSERT INTO {db_name}.product (id, category, capacity, color, screen_size, memory, other_specs) VALUES'
        for row in records:
            values = f'({row[0]}, "{row[1]}", {row[2]}, "{row[3]}", {row[4]}, {row[5]}, "{row[6]}"),'
            if type(row[1]) != str or type(row[3]) != str or type(row[6]) != str:
                return print(f'You are trying to use a number on a string field. row: {row}')
            else:
                full_query = f'{full_query} {values}'
        cursor.execute(full_query[:len(full_query)-1])
        print(f'Populating product table')
        db_connection.commit()
    except conn.errors.ProgrammingError as error:
        if "1064" in str(error):
            return print(f'You are trying to fill a field with an empty value. Fields do not accept NULL values.')
        elif "1054" in str(error):
            print(error)
            return print(f'You are trying to fill a field with incorrect data type.')
        else:
            return print(f'An exception has ocurred: {error}')
    except IndexError:
        return print(f'The row you are trying to populate is missing a value. It should have 7 values on it.')

def top_sell_country_category(db_connection, cursor, db_name: str):
    """
    Determines the top-selling product categories in each country.
    """
    try:
        query = f'CREATE TABLE testing.top_sell_country_category AS (\
                    SELECT product_id, country, category, total\
                        FROM (\
                            SELECT product_id, country, category, sum(final_sales) AS total,\
                                ROW_NUMBER() OVER (PARTITION BY country ORDER BY SUM(final_sales) DESC) AS category_rank\
                                FROM {db_name}.sales\
                                GROUP BY country, category) AS rank_table\
                    WHERE category_rank = 1)'
        cursor.execute(query)
        print(f'Creating top_sell_country_category table')
        db_connection.commit()
        result = cursor.fetchall()
        dataframe = pd.DataFrame(result, columns=["Product_id", "Country", "Category", "Total_sales"])
    except conn.errors.ProgrammingError as error:
        print(f'An exception has ocurred: {error}')
        response = input(f'Do you want to remove existing table and create a new one? (yes/no) ')
        if response.lower() == "yes":
            drop_query = f'DROP TABLE {db_name}.top_sell_country_category'
            cursor.execute(drop_query)
            db_connection.commit()
            cursor.execute(query)
            db_connection.commit()
            result = cursor.fetchall()
            dataframe = pd.DataFrame(result, columns=["Product_id", "Country", "Category", "Total_sales"])
            print("New table top_sell_country_category has been created")
            return dataframe
        else:
            return print("Keep exisitng top_sell_country_category table")
    return dataframe

def top_product_specs(cursor, db_name: str):
    """
    Retrieves detailed product specifications for these top-selling products
    """
    query = f'SELECT p.*, tscc.country, s.final_sales\
                FROM {db_name}.product p\
                JOIN\
                    {db_name}.top_sell_country_category tscc ON tscc.product_id = p.id\
                JOIN \
                    {db_name}.sales s ON tscc.product_id = s.product_id '
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns=["id", "Category", "Capacity", "Color", "Screen_size", "Memory", "Other_specs", "Country", "Final_sales"])
    return dataframe

def total_distinct_products_sold(cursor, db_name: str):
    """
    Determines the total number of distinct products sold
    """
    query = f' SELECT \
	                DISTINCT product_id,\
                    sum(quantity) \
                    FROM {db_name}.sales\
	                GROUP BY product_id'
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns=["Product_id", "Total_sold"])
    return dataframe

def max_sales_category(cursor, db_name: str):
    """
    A query that determines the maximum sales recorded for each category
    """
    query = f' SELECT \
	                DISTINCT category,\
                    sum(final_sales) \
                    FROM {db_name}.sales\
	                GROUP BY category'
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns=["Category", "Total_sales"])
    return dataframe

def extract_data_from_db(cursor, db_name: str):
    """
    Retrieve data from the existing sales and product tables. Including id, country, category, product capacity, color, quantity sold, and final sales amount
    """
    query = f'SELECT\
                s.product_id,\
                s.country,\
                p.category,\
                p.capacity,\
                p.color,\
                s.quantity,\
                s.final_sales\
                FROM {db_name}.sales s\
                JOIN {db_name}.product p ON s.product_id = p.id'
    cursor.execute(query)
    result = cursor.fetchall()
    dataframe = pd.DataFrame(result, columns = ["id", "country", "category", "capacity", "color", "quantity", "final_sales"])
    return dataframe
    
def transform_data(extracted):
    """
    This function Implement transformations to calculate the total revenue for each product (defined as quantity * final_sales).
    Categorize each transaction based on sales volume into 'High', 'Medium', or 'Low'.
    Returns two objecst. A list of rows in order to populate the DB table and a Dataframe for visualization
    """
    dataframe = extracted
    dataframe["total_revenue"] = dataframe["quantity"] * dataframe["final_sales"]
    # Determine some values to set a category scale
    max_revenue = max(dataframe["total_revenue"])
    min_revenue = min(dataframe["total_revenue"])
    revenue_distance = max_revenue - min_revenue
    half_revenue = revenue_distance/2
    upper_limit = half_revenue*1.25
    lower_limit = half_revenue*0.75
    categories = []
    for revenue in dataframe["total_revenue"]:
        if upper_limit < revenue <= max_revenue:
            categories.append("High")
        elif upper_limit >= revenue >= lower_limit:
            categories.append("Medium")
        elif lower_limit > revenue >= min_revenue:
            categories.append("Low")
    dataframe["transact_category"] = categories
    list_of_rows = list(zip(*map(dataframe.get, dataframe)))
    return dataframe, list_of_rows

def create_transformed_sales_table(db_connection, cursor, db_name: str):
    """
    Creates a table "transformed_sales" on the  Data base
    """
    try:
        query = f'CREATE TABLE {db_name}.transformed_sales (\
            id int NOT NULL,\
            country varchar(128) NOT NULL,\
            category varchar(128) NOT NULL,\
            capacity int NOT NULL,\
            color varchar(128) NOT NULL,\
            quantity int NOT NULL,\
            final_sales double NOT NULL,\
            total_revenue double NOT NULL,\
            transact_category varchar(128) NOT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin'
        
        cursor.execute(query)
        print(f'Creating transformed_sales table')
        db_connection.commit()
    except conn.errors.ProgrammingError as error:
        print(f'An exception has ocurred: {error}')
        response = input(f'Do you want to remove existing table and create a new one? (yes/no) ')
        if response.lower() == "yes":
            drop_query = f'DROP TABLE {db_name}.transformed_sales'
            cursor.execute(drop_query)
            db_connection.commit()
            cursor.execute(query)
            db_connection.commit()
            return print("New table transformed_sales has been created")
        else:
            return print("Keep exisitng transformed_sales table")
   
def load_data(transformed_data: list, cursor, db_connection, db_name: str):
    """
    Receives a list of tupples that contain each row data for the "transformed_sales" table.
    Each tupple element should match the data type deffined on the table schema
    """
    try:
        full_query = f'INSERT INTO {db_name}.transformed_sales (id, country, category, capacity, color, quantity, final_sales, total_revenue, transact_category) VALUES'
        for row in transformed_data:
            values = f'({row[0]}, "{row[1]}", "{row[2]}", {row[3]}, "{row[4]}", {row[5]}, {row[6]}, {row[7]}, "{row[8]}"),'
            if type(row[1]) != str or type(row[2]) != str or type(row[4]) != str or type(row[8]) != str:
                return print(f'You are trying to use a number on a string field. row: {row}')
            else:
                full_query = f'{full_query} {values}'
        cursor.execute(full_query[:len(full_query)-1])
        print(f'Populating product table')
        db_connection.commit()
    except conn.errors.ProgrammingError as error:
        if "1064" in str(error):
            return print(f'You are trying to fill a field with an empty value. Fields do not accept NULL values.')
        elif "1054" in str(error):
            print(error)
            return print(f'You are trying to fill a field with incorrect data type.')
        else:
            return print(f'An exception has ocurred: {error}')
    except IndexError:
        return print(f'The row you are trying to populate is missing a value. It should have 9 values on it.')   

host_id = input(f'Provide host: ')
db_name = input(f'To which DB you want to connect? ')
user_id= input(f'User: ')
psw = input(f'Pass: ')

connection = db_conn(host_id, user_id, psw)
if connection is not None:
    cursor = db_cursor(connection)

    # Creating DB
    create_database(connection, cursor, db_name)

    # Creating and populating sales table
    create_sales_table(connection, cursor, db_name)
    sales_data = generate_sales_data()
    populate_sales_table(sales_data[0], cursor = cursor, db_connection = connection, db_name = db_name)

    # Creating and populating product table
    create_product_table(connection, cursor, db_name)
    product_data = generate_product_data(sales_data[1])
    populate_product_table(product_data, cursor = cursor, db_connection = connection, db_name = db_name)

    # Calculating top-selling product categories in each country
    print("top-selling product categories in each country")
    print(top_sell_country_category(connection, cursor, db_name))

    # Retrieve detailed product specifications for these top-selling products
    print("product specifications for the top-selling products")
    print(top_product_specs(cursor, db_name))

    # Provide additional insights like the total number of distinct products sold
    print("total number of distinct products sold")
    print(total_distinct_products_sold(cursor, db_name))

    # Maximum sales recorded for each category
    print("maximum sales recorded for each category")
    print(max_sales_category(cursor, db_name))

    # Extracting data from DB
    extracted_data = extract_data_from_db(cursor, db_name)

    # Transofrming data
    transformed_data = transform_data(extracted_data)

    # Creating and populating transformed_sales table
    create_transformed_sales_table(connection, cursor, db_name)
    load_data(transformed_data[1], cursor = cursor, db_connection = connection, db_name = db_name)
    print("Transformed sales data")
    print(transformed_data[0])

    # Closing DB connection
    connection.close()






