import pandas as pd
import json
import boto3
import gzip
from io import BytesIO
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import re
import os

# AWS S3 configuration
bucket_name = ''
base_path = 'bucket_name/data/BILLING_PERIOD='

# Set AWS credentials directly (not recommended for production)
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')

# Calculate the path for the current billing period
today = datetime.today()
billing_period = (today - timedelta(days=1)).strftime('%Y-%m')
s3_path = f'{base_path}{billing_period}/cur-report-00001.csv.gz'

# AWS credentials (use environment variables or AWS credentials file for production)
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Download the file from S3
try:
    obj = s3.get_object(Bucket=bucket_name, Key=s3_path)
    print("File downloaded from S3 successfully.")
except Exception as e:
    print(f"Error downloading file from S3: {e}")

# Read the gzip file
try:
    with gzip.open(BytesIO(obj['Body'].read()), 'rt') as f:
        df = pd.read_csv(f, dtype={'line_item_blended_rate': 'float', 'line_item_unblended_rate': 'float'}, low_memory=False)
    print("CSV file loaded successfully.")
except Exception as e:
    print(f"Error loading CSV file: {e}")

# Extract date from identity_time_interval
def extract_date(time_interval):
    try:
        return time_interval.split('/')[0]
    except AttributeError:
        return None

df['identity_date'] = df['identity_time_interval'].apply(extract_date)

# Convert identity_date to datetime and ensure it's in the correct format
df['identity_date'] = pd.to_datetime(df['identity_date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')


# Define the directory where the text file is located
SCRIPT_DIR = "/home/ubuntu/amazon_cur"

# Read values from the text file using the absolute path
with open(os.path.join(SCRIPT_DIR, 'resource_id_values.txt'), 'r') as file:
    resource_id_values = [line.strip() for line in file]

# Function to apply regex and update resource_tags
def update_resource_tags(row):
    if row['resource_tags'] == '{}' or row['resource_tags'] == '':
        for value in resource_id_values:
            if re.search(value, str(row['line_item_resource_id'])):  # Ensure the value is a string
                return json.dumps({'user_client': value})
    return row['resource_tags']

# Fill missing values in resource_tags with '{}'
df['resource_tags'] = df['resource_tags'].fillna('{}')

# Update resource_tags where it's an empty JSON object
df['resource_tags'] = df.apply(update_resource_tags, axis=1)

# Parse JSON in resource_tags and extract user_client and user_name
def extract_tags(json_str):
    try:
        tags = json.loads(json_str)
        return tags.get('user_client', 'Unknown'), tags.get('user_name', 'Unknown')
    except json.JSONDecodeError:
        return 'Unknown', 'Unknown'

df['user_client'], df['user_name'] = zip(*df['resource_tags'].apply(extract_tags))

# Add default value for account_name
df['account_name'] = ''

# Drop the original resource_tags column
df = df.drop(columns=['resource_tags'])

# Convert datetime columns to MySQL compatible format
datetime_columns = ['bill_billing_period_end_date', 'bill_billing_period_start_date', 'identity_date']
for col in datetime_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

# Define the columns to group by
group_by_columns = [
    'bill_bill_type', 'bill_billing_entity', 'bill_billing_period_end_date', 
    'bill_billing_period_start_date', 'line_item_availability_zone', 
    'line_item_currency_code', 'line_item_legal_entity', 'line_item_line_item_description', 
    'line_item_line_item_type', 'line_item_operation', 'line_item_product_code', 
    'line_item_tax_type', 'line_item_usage_account_id', 'line_item_usage_account_name', 
    'pricing_currency', 'pricing_term', 'pricing_unit', 
    'product_from_location', 'product_from_location_type', 'product_from_region_code', 
    'product_instance_family', 'product_instance_type', 'product_location', 
    'product_location_type', 'product_operation', 'product_product_family', 
    'product_region_code', 'product_servicecode', 'product_to_location', 
    'product_to_location_type', 'product_to_region_code', 
    'user_client', 'user_name', 'account_name', 'identity_date'
]

# Define the aggregation functions for the respective columns
aggregation_functions = {
    'line_item_blended_cost': 'sum',
    'line_item_unblended_cost': 'sum',
    'line_item_usage_amount': 'sum',
    'line_item_blended_rate': 'mean',
    'line_item_unblended_rate': 'mean'
}

# Check if all columns exist in the DataFrame
missing_columns = [col for col in group_by_columns + list(aggregation_functions.keys()) if col not in df.columns]
if missing_columns:
    print(f"Missing columns in the CSV file: {missing_columns}")
else:
    print("All specified columns are present in the CSV file.")

    # Print the shape of the DataFrame before handling missing values
    print(f"DataFrame shape before handling missing values: {df.shape}")

    # Fill missing values in group by columns with 'Unknown'
    df[group_by_columns] = df[group_by_columns].fillna('Unknown')

    # Print the shape of the DataFrame after filling missing values in group by columns
    print(f"DataFrame shape after filling missing values in group by columns: {df.shape}")

    # Fill missing values in aggregation columns with 0
    for col in aggregation_functions.keys():
        if df[col].isnull().any():
            print(f"Filling missing values in column: {col}")
            df[col].fillna(0, inplace=True)

    # Print the shape of the DataFrame after handling missing values
    print(f"DataFrame shape after handling missing values: {df.shape}")

    # Check if there are any rows left to group by
    if df.empty:
        print("No data left after cleaning. The DataFrame is empty.")
    else:
        # Group by the specified columns and aggregate using the defined functions
        try:
            aggregated_df = df.groupby(group_by_columns).agg(aggregation_functions).reset_index()
            print("Data grouped and aggregated successfully.")
        except Exception as e:
            print(f"Error during aggregation: {e}")

        # Print the shape of the aggregated DataFrame
        print(f"Aggregated DataFrame shape: {aggregated_df.shape}")

        # Display the first few rows of the aggregated DataFrame to ensure it processed correctly
        print("Aggregated data preview:")
        print(aggregated_df.head())

        # Save the aggregated DataFrame to a new CSV file (optional)
        output_csv_path = r'aggregated_cur_report.csv'
        try:
            aggregated_df.to_csv(output_csv_path, index=False)
            print(f"Aggregated data saved to {output_csv_path}")
        except Exception as e:
            print(f"Error saving aggregated data: {e}")

        # Database connection variables
        db_username = os.getenv('admin_user')
        db_password = os.getenv('admin_password')
        db_host = os.getenv('db_host_name')
        db_name = 'amazon'

        # Create database connection
        engine = create_engine(f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}/{db_name}')

        # Remove old data
        identity_month = (today - timedelta(days=1)).strftime('%Y-%m')
        delete_query = text("""
            DELETE FROM aws_billing_daily
            WHERE account_name = 'account_name' AND DATE_FORMAT(identity_date, '%Y-%m') = :identity_month
        """)

        try:
            with engine.connect() as connection:
                trans = connection.begin()
                try:
                    result = connection.execute(delete_query, {'identity_month': identity_month})
                    print(f"Old data for {identity_month} deleted successfully. Rows affected: {result.rowcount}")
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    print(f"Error deleting old data: {e}")
        except Exception as e:
            print(f"Error establishing connection: {e}")

        # Insert aggregated data into MySQL table in batches
        batch_size = 1000
        try:
            with engine.begin() as connection:  # Use a transaction
                for start in range(0, len(aggregated_df), batch_size):
                    end = start + batch_size
                    batch_df = aggregated_df.iloc[start:end]
                    batch_df.to_sql(name='aws_billing_daily', con=connection, if_exists='append', index=False)
                print("Aggregated data inserted into MySQL table successfully.")
        except Exception as e:
            print(f"Error inserting data into MySQL table: {e}")
