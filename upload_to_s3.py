import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
bucket_name = 'profitall-data-bucket'  # REPLACE THIS

# Upload each format
files = ['data.csv', 'data.json', 'data.parquet', 
         'data_uncompressed.parquet', 'data_gzip.parquet']

print("\nUploading to S3...")
for filename in files:
    try:
        s3.upload_file(filename, bucket_name, f'formats-comparison/{filename}')
        print(f"✓ Uploaded {filename}")
    except ClientError as e:
        print(f"✗ Error uploading {filename}: {e}")

# Compare sizes in S3
print("\n" + "="*60)
print("FILES IN S3")
print("="*60)

response = s3.list_objects_v2(Bucket=bucket_name, Prefix='formats-comparison/')
for obj in response.get('Contents', []):
    size_mb = obj['Size'] / (1024 * 1024)
    storage_class = s3.head_object(Bucket=bucket_name, Key=obj['Key']).get('StorageClass', 'STANDARD')
    print(f"{obj['Key']:45} | {size_mb:6.2f} MB | {storage_class}")