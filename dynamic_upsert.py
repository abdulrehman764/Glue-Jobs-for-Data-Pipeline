import psycopg2
import json
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import sys


relational_columns = {
        'customers': ['CustomerID', 'FirstName', 'LastName', 'Email', 'Address', 'City', 'State', 'ZipCode'],
        'products': ['ProductID', 'ProductName', 'Category', 'Description', 'Price'],
        'stores': ['StoreID', 'StoreName', 'Address', 'City', 'State', 'ZipCode'],
        'orders': ['OrderID', 'CustomerID', 'StoreID', 'OrderDate'],
        'orderdetails': ['OrderID', 'ProductID', 'Quantity']
    }
dim_columns = {
    'customers': ['CustomerKey INT IDENTITY(1,1) NOT NULL', 'CustomerID INT NOT NULL', 'FirstName VARCHAR(50)', 'LastName VARCHAR(50)', 'Email VARCHAR(50)', 'Address VARCHAR(50)', 'City VARCHAR(50)', 'State VARCHAR(50)', 'ZipCode VARCHAR(10)', 'StartDate DATE NOT NULL', 'EndDate DATE'],
    'products': ['ProductKey INT IDENTITY(1,1) NOT NULL', 'ProductID INT NOT NULL', 'ProductName VARCHAR(50)', 'Category VARCHAR(50)', 'Description VARCHAR(50)', 'Price DECIMAL(8,2)', 'StartDate DATE NOT NULL', 'EndDate DATE'],
    'stores': ['StoreKey INT IDENTITY(1,1) NOT NULL', 'StoreID INT NOT NULL', 'StoreName VARCHAR(50)', 'Address VARCHAR(50)', 'City VARCHAR(50)', 'State VARCHAR(50)', 'ZipCode VARCHAR(10)', 'StartDate DATE NOT NULL', 'EndDate DATE']
}

staging_columns = {
    'customers': ['CustomerID INT', 'FirstName VARCHAR(50)', 'LastName VARCHAR(50)', 'Email VARCHAR(50)', 'Address VARCHAR(50)', 'City VARCHAR(50)', 'State VARCHAR(50)', 'ZipCode VARCHAR(10)', 'LoadDate DATE DEFAULT current_date'],
    'products': ['ProductID INT NOT NULL', 'ProductName VARCHAR(50)', 'Category VARCHAR(50)', 'Description VARCHAR(50)', 'Price DECIMAL(8,2)', 'LoadDate DATE DEFAULT current_date'],
    'stores': ['StoreID INT', 'StoreName VARCHAR(50)', 'Address VARCHAR(50)', 'City VARCHAR(50)', 'State VARCHAR(50)', 'ZipCode VARCHAR(10)', 'LoadDate DATE DEFAULT current_date']
}

args = getResolvedOptions(sys.argv, ['SecretName', 'SecretRegionName', 'SecretManagerService'])

# Access the custom parameters
_secret_name = args['SecretName']
_region_name = args['SecretRegionName']
_service_name = args['SecretManagerService']

def get_workflow_params():
    """
    Retrieves the workflow parameters from AWS Glue.

    Returns:
        dict: The workflow parameters.
    """
    print("Into get_workflow_params function")
    try:
        print("Into get_workflow_params Try")
        glue_client = boto3.client("glue")
        print("Glue Client Created")
        args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
        workflow_name = args['WORKFLOW_NAME']
        workflow_run_id = args['WORKFLOW_RUN_ID']

        workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
        print("Workflow properties")
        print(f"{workflow_name}, {workflow_params}")
        return workflow_params
    except Exception as exception:
        print("Failed to get workflow parameters")
        raise Exception(f"Failed to get workflow parameters")
        raise


def get_secret():
    secret_name = _secret_name
    region_name = _region_name

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name=_service_name,
        region_name=_region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise Exception("Secret Manager get_secret_value failed")

    # Decrypt the secret value
    secret = get_secret_value_response['SecretString']
    return secret

# Get the secret value
secret = get_secret()
credentials = json.loads(secret)
host = credentials['host']
port = credentials['port']
user = credentials['user']
password = credentials['password']
database = credentials['database']
params = get_workflow_params()
table_name = params['table_name'].lower()
# Establish a connection to Redshift

if table_name!="orders" and table_name!="orderdetails":
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Begin transaction
    cursor.execute("BEGIN;")
    
    sql_script = f"""
            -- Create the staging table
            CREATE TABLE dim_{table_name}_staging (
              {', '.join(staging_columns[table_name])}
            );
    
            -- Print debugging information
            SELECT 'Staging table created.' AS debug_info;
    
            -- Insert new records into the staging table
            INSERT INTO dim_{table_name}_staging ({', '.join([column.split(' ')[0] for column in staging_columns[table_name][:-1]])})
            SELECT DISTINCT {', '.join([column.split(' ')[0] for column in staging_columns[table_name][:-1]])}
            FROM {table_name};
    
            -- Print debugging information
            SELECT 'New records inserted into the staging table.' AS debug_info;
    
            -- Update end dates for existing records
            UPDATE dim_{table_name}
            SET EndDate = current_date - INTERVAL '1 day'
            WHERE {dim_columns[table_name][1].split(' ')[0]} IN (SELECT {dim_columns[table_name][1].split(' ')[0]} FROM {table_name})
              AND EndDate = '9999-12-31';
    
            -- Print debugging information
            SELECT 'Existing records updated.' AS debug_info;
    
            -- Insert new records from the staging table into the dimension table
            INSERT INTO dim_{table_name} ({', '.join([column.split(' ')[0] for column in dim_columns[table_name][1:]])})
            SELECT {', '.join([column.split(' ')[0] for column in staging_columns[table_name]])}, '9999-12-31'
            FROM dim_{table_name}_staging;
    
    
            -- Print debugging information
            SELECT 'New records inserted into the dimension table.' AS debug_info;
    
            -- Truncate the staging table
            DROP TABLE dim_{table_name}_staging;
    
            -- Print debugging information
            SELECT 'Staging table dropped.' AS debug_info;
    
            COMMIT;
            """
    print(f"SQL: {sql_script}")
    
    try:
        # Execute the SQL script
        cursor.execute(sql_script)
        print("Transaction executed successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error executing transaction:", error)
        raise Exception("Transaction failed")
    
    # Close the cursor and connection
    finally:
        cursor.close()
        conn.close()
else:
    print("Upsert Not required for Orders and Order Details")
