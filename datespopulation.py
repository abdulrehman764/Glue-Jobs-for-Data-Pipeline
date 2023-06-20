import psycopg2

# Redshift connection details
conn = psycopg2.connect(
    host='abdul-redshift-cluster.cmex6sgqlpju.us-east-1.redshift.amazonaws.com',
    port=5439,
    user='awsuser',
    password='12345678Aa',
    database='dev'
)

# Create a cursor object
cursor = conn.cursor()

# SQL statements
sql_statements = [
    """
    -- Generate a series of dates from 2023-01-01 to 2024-12-31
    CREATE TEMPORARY TABLE temp_dates AS
    SELECT 
      '2023-01-01'::DATE + ROW_NUMBER() OVER (ORDER BY 1) - 1 AS date
    FROM 
      (SELECT 1 FROM stl_scan LIMIT 731) s; -- 731 days for 2023-2024
    """,
    """
    -- Insert the dates into the dim_dates table
    INSERT INTO dim_dates (Date, Year, Quarter, Month, Day, Weekday, Week)
    SELECT
      date,
      EXTRACT(YEAR FROM date) AS Year,
      EXTRACT(QUARTER FROM date) AS Quarter,
      EXTRACT(MONTH FROM date) AS Month,
      EXTRACT(DAY FROM date) AS Day,
      EXTRACT(DOW FROM date) AS Weekday,
      EXTRACT(WEEK FROM date) AS Week
    FROM 
      temp_dates;
    """,
    """
    -- Drop the temporary table
    DROP TABLE temp_dates;
    """
]

try:
    # Execute the SQL statements
    for sql_statement in sql_statements:
        cursor.execute(sql_statement)
    conn.commit()
    print("SQL statements executed successfully!")
except (Exception, psycopg2.DatabaseError) as error:
    conn.rollback()
    print("Error executing SQL statements:", error)

# Close the cursor and connection
cursor.close()
conn.close()
