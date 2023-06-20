import psycopg2
import json
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import sys

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

if  table_name=="orderdetails":
    
    # Establish a connection to Redshift
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # SQL statement
    sql_statement = """
    
    BEGIN;
    
    -- Create the staging table
    CREATE TABLE staging_fact_orders (
      OrderID INT NOT NULL,
      CustomerID INT NOT NULL,
      StoreID INT NOT NULL,
      ProductID INT NOT NULL,
      Quantity INT NOT NULL,
      UnitPrice DECIMAL(8,2) NOT NULL,
      TotalPrice DECIMAL(8,2) NOT NULL,
      OrderDate DATE NOT NULL
    );
    
    -- Print debugging information
    SELECT 'Staging table created.' AS debug_info;
    
    -- Insert records into the staging table
    INSERT INTO staging_fact_orders (OrderID, CustomerID, StoreID, ProductID, Quantity, UnitPrice, TotalPrice, OrderDate)
    SELECT o.OrderID, o.CustomerID, o.StoreID, od.ProductID, od.Quantity, od.Price, od.Price*od.Quantity, o.OrderDate
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID;
    -- Print debugging information
    SELECT 'Records inserted into staging table.' AS debug_info;
    
    -- Insert records into the fact_orders table
    INSERT INTO fact_orders (OrderID, CustomerID, StoreID, ProductID, Quantity, UnitPrice, TotalPrice, OrderDateID)
    SELECT o.OrderID, dc.CustomerKey, ds.StoreKey, dp.ProductKey, o.Quantity, o.UnitPrice, o.TotalPrice, dd.DateKey
    FROM staging_fact_orders o
    JOIN dim_customers dc ON o.CustomerID = dc.CustomerID
    JOIN dim_stores ds ON o.StoreID = ds.StoreID
    JOIN dim_products dp ON o.ProductID = dp.ProductID
    JOIN dim_dates dd ON o.OrderDate = dd.Date
    WHERE dp.EndDate = '9999-12-31' AND dc.EndDate = '9999-12-31' AND ds.EndDate = '9999-12-31';
    
    -- Print debugging information
    SELECT 'Records inserted into fact_orders table.' AS debug_info;
    
    -- Drop the staging table
    DROP TABLE staging_fact_orders;
    
    -- Print debugging information
    SELECT 'Staging table dropped.' AS debug_info;
    
    COMMIT;
    """
    
    try:
        # Execute the SQL statement
        cursor.execute(sql_statement)
        conn.commit()
        print("INSERT statement executed successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        print("Error executing INSERT statement:", error)
        raise Exception("Populating Fact Table Failed")
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
else:
    print("Fact Table population required only for orders and orders details")
