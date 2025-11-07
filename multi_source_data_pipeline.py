import boto3
import pandas as pd
from sqlalchemy import create_engine

# 1. Extract from RDS
rds_engine = create_engine("postgresql://...")
customers = pd.read_sql('SELECT * FROM customers', rds_engine)

# 2. Extract from DynamoDB
dynamodb = boto3.resource('dynamodb')
products_table = dynamodb.Table('ProductCatalog')
products = pd.DataFrame(products_table.scan()['Items'])

# 3. Load existing data from S3
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='your-bucket', Key='raw-data/data.parquet')
historical_data = pd.read_parquet(obj['Body'])

# 4. Combine and process
print(f"Customers: {len(customers)}")
print(f"Products: {len(products)}")
print(f"Historical: {len(historical_data)}")

# 5. Save combined dataset back to S3 in Parquet
combined = pd.concat([customers, historical_data], ignore_index=True)
combined.to_parquet('combined_data.parquet')
s3.upload_file('combined_data.parquet', 'your-bucket', 'processed-data/combined.parquet')

print("Pipeline completed!")