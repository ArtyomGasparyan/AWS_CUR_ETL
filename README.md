# Amazon CUR Data Transformation

This project is designed to download, process, and transform Amazon Cost and Usage Reports (CUR) data from an S3 bucket, and then load the transformed data into a MySQL database. The script handles data extraction, transformation, and loading (ETL) operations, and it is intended to run daily.

## Prerequisites

Ensure you have the following installed:

- Python 3.x
- pandas
- boto3
- SQLAlchemy
- MySQL database
- AWS credentials with access to the S3 bucket


## Configuration

### AWS S3 Configuration

- Configure your AWS credentials. 

- Set the AWS S3 bucket and base path in `amazon_data_transformation_daily.py`:
    ```python
    bucket_name = ''
    base_path = 'name/data/BILLING_PERIOD='
    ```

### MySQL Configuration

- Set the MySQL connection details in `amazon_data_transformation_daily.py`:
    ```python
    db_username = 'your_username'
    db_password = 'your_password'
    db_host = 'your_hostname'
    db_name = 'your_database_name'
    ```

## Notes

- The script is designed to be run daily, and it processes the data from the previous day.
- Modify the paths and configuration variables as per your environment setup.