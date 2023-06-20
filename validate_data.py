import psycopg2
import json
import boto3
import sys
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions


def get_secret(secret_name, region_name, service_name):
    """
    Retrieves the secret value from AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret.
        region_name (str): The name of the region.
        service_name (str): The name of the service.

    Returns:
        str: The secret value.
    """
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name=service_name, region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise Exception("Secret Manager get_secret_value Failed")

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    print("Secret is", secret)
    return secret


def validate_data(redshift_conn, table_name):
    """
    Validates the data in the specified table.

    Args:
        redshift_conn: The connection to the Redshift database.
        table_name (str): The name of the table.

    Returns:
        bool: True if the data is valid, False otherwise.
    """
    print(f"In validate_data function with connection {redshift_conn} and table {table_name}")
    
    table_columns = {
        'Customers': ['CustomerID', 'FirstName', 'LastName', 'Email', 'Address', 'City', 'State', 'ZipCode'],
        'Products': ['ProductID', 'ProductName', 'Category', 'Description', 'Price'],
        'Stores': ['StoreID', 'StoreName', 'Address', 'City', 'State', 'ZipCode'],
        'Orders': ['OrderID', 'CustomerID', 'StoreID', 'OrderDate'],
        'OrderDetails': ['OrderID', 'ProductID', 'Quantity']
    }

    if table_name not in table_columns:
        print("Invalid table name")
        raise Exception("Table Not Found")
        return False

    not_null_columns = table_columns[table_name]
    unique_key_columns = [table_columns[table_name][0]]

    with redshift_conn.cursor() as cur:
        print("Inside the cursor")
        
        # Check for NOT NULL constraints
        for i, column in enumerate(not_null_columns):
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {column} IS NULL;")
            count = cur.fetchone()[0]
            if count > 0:
                print(f"Data violation: {count} rows with NULL value in column {column} of table {table_name}")
                raise Exception(f"Not Null constraints violation in Table: {table_name}")
                return False
            print(f"Query {i} executed")
        
        # Check for unique primary key constraint
        unique_key = ", ".join(unique_key_columns)
        cur.execute(f"SELECT {unique_key}, COUNT(*) FROM {table_name} GROUP BY {unique_key} HAVING COUNT(*) > 1;")
        duplicate_rows = cur.fetchall()
        print(f"Query for duplicate rows executed. Duplicate rows are {duplicate_rows}")
        if duplicate_rows:
            print(f"Data violation: Duplicate rows found for the unique key {unique_key} in table {table_name}")
            raise Exception(f"Unique Key constraints violation in Table: {table_name}")
            return False
    return True


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


def copy_data_to_redshift(bucket, key, table_name):
    """
    Copies data from an S3 bucket to a Redshift table.

    Args:
        bucket (str): The name of the S3 bucket.
        key (str): The key of the file in the S3 bucket.
        table_name (str): The name of the Redshift table.

    Returns:
        dict: The result of the data copy operation.
    """
    print("Into copy_data_to_redshift function")
    credentials = json.loads(get_secret(_secret_name, _region_name, _service_name))
    
    _host = credentials["host"]
    _port = credentials["port"]
    _password = credentials["password"]
    _database = credentials["database"]

    redshift_conn = psycopg2.connect(host=_host, port=int(_port), user=credentials["user"], password=_password, database=_database)
    
    redshift_copy_command = f"""
    TRUNCATE TABLE {table_name}; 
    COPY {table_name} 
    FROM 's3://{bucket}/{key}' 
    IAM_ROLE 'arn:aws:iam::414432075221:role/service-role/AmazonRedshift-CommandsAccessRole-20230302T155618' 
    FORMAT AS CSV 
    DELIMITER ',' 
    IGNOREHEADER 1;
    """
    
    with redshift_conn.cursor() as cur:
        try:
            cur.execute(redshift_copy_command)
            redshift_conn.commit()

            # Validate the data
            data_valid = validate_data(redshift_conn, table_name)

            if data_valid:
                print("Data validation and ingestion completed successfully")
                return {
                    'statusCode': 200,
                    'body': json.dumps('Data validation and ingestion completed successfully')
                }
            else:
                print("Data validation failed")
                raise Exception("Data validation failed")
                # return {
                #     'statusCode': 400,
                #     'body': json.dumps('Data validation failed')
                # }
        except psycopg2.Error as e:
            print(f"Error loading data into table {table_name}: {str(e)}")
            raise Exception("Data loading failed")
            # return {
            #     'statusCode': 500,
            #     'body': json.dumps(f"Error loading data into table {table_name}")
            # }
        finally:
            redshift_conn.rollback()
            redshift_conn.close()


# Main code
# if __name__ == "__main__":
args = getResolvedOptions(sys.argv, ['SecretName', 'SecretRegionName', 'SecretManagerService'])
_secret_name = args['SecretName']
_region_name = args['SecretRegionName']
_service_name = args['SecretManagerService']

params = get_workflow_params()
bucket = params['bucket']
key = params['key']
table_name = params['table_name']
print(f"Bucket: {bucket}\nKey: {key}\nTable: {table_name}")
secret_value = get_secret(_secret_name, _region_name, _service_name)
print(f"Secret value: {secret_value}")
copy_data_to_redshift(bucket, key, table_name)
