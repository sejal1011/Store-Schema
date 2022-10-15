# import etl_scripts
from etl_scripts import *

# enter database details
user = 'root'  # please write your user name
password = 'sejal1011'  # please write your password
host = 'localhost'  # please write your host address
port = 3306

database = 'store_schema'
if __name__ == '__main__':

    # specifying the zip file name and zip file extract path
    zip_name = 'store_project/Data.zip'
    
    extract_path = 'store_project/'

    # Extract the data from zip file
    extract_zip(zip_name, extract_path)

    # Establish connection with SQL
    engine = establish_connection(user, password, host, database)
    # store_schema
    sql_table = ["products", "shippers", "customers", "order_statuses",
                 "orders", "order_items"]

    for table in sql_table:
        # path, where extracted data from zip is located
        path = 'store_project/Data/'
        data = transform_table(table, path, engine)
        print(data.shape)

        # insert data to sql
        insert_data_sql(data, table, engine)
