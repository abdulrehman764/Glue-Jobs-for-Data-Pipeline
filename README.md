# Redshift Data Processing Repository

This repository contains Python scripts for data processing in Amazon Redshift. The scripts cover tasks such as data validation, transformation, and loading into Redshift tables.

## Files

### `datespopulation.py`

This script connects to an Amazon Redshift cluster and generates a series of dates from 2023-01-01 to 2024-12-31. It inserts these dates into the `dim_dates` table for use in date-based queries. The script is parameterized to work with different Redshift clusters and tables.

### `dynamic_upsert.py`

This script performs dynamic upsert operations in Amazon Redshift. It's designed to handle upsert operations for multiple tables such as `customers`, `products`, and `stores`. The script determines whether an upsert operation is required based on the table name and then performs the necessary operations.

### `populate_fact.py`

This script populates a fact table in Amazon Redshift. It's specifically designed for the `fact_orders` table and works with the `orders` and `orderdetails` tables to populate the fact table with relevant data. The script ensures that data is transformed and loaded efficiently.

### `validate_data.py`

This script validates data in Amazon Redshift tables. It checks for constraints such as NOT NULL and unique primary keys for tables like `Customers`, `Products`, `Stores`, `Orders`, and `OrderDetails`. The script is parameterized to work with different Redshift clusters and tables.

## Configuration

Before running these scripts, make sure to configure the necessary parameters for your Redshift cluster and AWS services. You can set configuration values such as AWS Secrets Manager secret names, region names, and service names as required.

## Usage

Each script can be executed individually based on your data processing needs. Ensure that you have the necessary AWS and Redshift credentials and permissions to run these scripts successfully.

## Contributing

If you find issues or have improvements to suggest, feel free to open an issue or submit a pull request to this repository.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
