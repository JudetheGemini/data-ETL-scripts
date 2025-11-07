import pandas as pd
import numpy as np
import time
import os

# Create a sample dataset
def create_sample_data(num_rows=100000):
    """Create a sample dataset with multiple columns"""
    np.random.seed(42)
    
    data = {
        'customer_id': range(1, num_rows + 1),
        'age': np.random.randint(18, 80, num_rows),
        'income': np.random.normal(50000, 20000, num_rows).astype(int),
        'purchase_amount': np.random.exponential(100, num_rows),
        'city': np.random.choice(['New York', 'London', 'Tokyo', 'Paris', 'Sydney'], num_rows),
        'is_premium': np.random.choice([True, False], num_rows, p=[0.3, 0.7]),
        'signup_date': pd.to_datetime('2020-01-01') + pd.to_timedelta(np.random.randint(0, 365*3, num_rows), unit='D'),
        'last_login': pd.to_datetime('2023-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_rows), unit='D'),
    }
    
    return pd.DataFrame(data)

# Create the sample dataset
print("Creating sample dataset...")
df = create_sample_data(1000)  # 100,000 rows
print(f"Dataset created with {len(df)} rows and {len(df.columns)} columns")
print("Columns:", df.columns.tolist())
print("\nFirst 5 rows:")
print(df.head())

# Save as CSV
print("\n" + "="*50)
print("Saving as CSV...")
csv_start = time.time()
csv_filename = 'sample_data.csv'
df.to_csv(csv_filename, index=False)
csv_time = time.time() - csv_start
print(f"CSV saved: {csv_filename}")
print(f"CSV file size: {os.path.getsize(csv_filename) / 1024 / 1024:.2f} MB")
print(f"CSV save time: {csv_time:.4f} seconds")

# Save as Parquet
print("\n" + "="*50)
print("Saving as Parquet...")
parquet_start = time.time()
parquet_filename = 'sample_data.parquet'
df.to_parquet(parquet_filename, index=False)
parquet_time = time.time() - parquet_start
print(f"Parquet saved: {parquet_filename}")
print(f"Parquet file size: {os.path.getsize(parquet_filename) / 1024 / 1024:.2f} MB")
print(f"Parquet save time: {parquet_time:.4f} seconds")

# Compare file sizes
size_reduction = ((os.path.getsize(csv_filename) - os.path.getsize(parquet_filename)) / 
                 os.path.getsize(csv_filename)) * 100
print(f"\nSize reduction with Parquet: {size_reduction:.1f}%")

# Performance comparison: Reading specific columns
print("\n" + "="*50)
print("PERFORMANCE COMPARISON: Reading specific columns")

# Read single column from CSV
print("\nReading single column 'income' from CSV...")
csv_read_start = time.time()
df_csv_single = pd.read_csv(csv_filename, usecols=['income'])
csv_single_time = time.time() - csv_read_start
print(f"CSV single column read time: {csv_single_time:.4f} seconds")
print(f"Data shape: {df_csv_single.shape}")

# Read single column from Parquet
print("\nReading single column 'income' from Parquet...")
parquet_read_start = time.time()
df_parquet_single = pd.read_parquet(parquet_filename, columns=['income'])
parquet_single_time = time.time() - parquet_read_start
print(f"Parquet single column read time: {parquet_single_time:.4f} seconds")
print(f"Data shape: {df_parquet_single.shape}")

# Read multiple columns from CSV
print("\nReading multiple columns from CSV...")
csv_multi_start = time.time()
df_csv_multi = pd.read_csv(csv_filename, usecols=['customer_id', 'age', 'income', 'city'])
csv_multi_time = time.time() - csv_multi_start
print(f"CSV multiple columns read time: {csv_multi_time:.4f} seconds")

# Read multiple columns from Parquet
print("\nReading multiple columns from Parquet...")
parquet_multi_start = time.time()
df_parquet_multi = pd.read_parquet(parquet_filename, columns=['customer_id', 'age', 'income', 'city'])
parquet_multi_time = time.time() - parquet_multi_start
print(f"Parquet multiple columns read time: {parquet_multi_time:.4f} seconds")

# Performance summary
print("\n" + "="*50)
print("PERFORMANCE SUMMARY")
print(f"Single column read - CSV: {csv_single_time:.4f}s, Parquet: {parquet_single_time:.4f}s")
print(f"Speedup: {csv_single_time/parquet_single_time:.1f}x")
print(f"Multiple columns read - CSV: {csv_multi_time:.4f}s, Parquet: {parquet_multi_time:.4f}s")
print(f"Speedup: {csv_multi_time/parquet_multi_time:.1f}x")

# Data validation
print("\n" + "="*50)
print("DATA VALIDATION")
print("Checking if data is preserved correctly...")
# Read full datasets
df_csv_full = pd.read_csv(csv_filename)
df_parquet_full = pd.read_parquet(parquet_filename)

print(f"CSV data shape: {df_csv_full.shape}")
print(f"Parquet data shape: {df_parquet_full.shape}")
print(f"Dataframes are equal: {df_csv_full.equals(df_parquet_full)}")

# Clean up (optional)
print("\n" + "="*50)
cleanup = input("Do you want to delete the created files? (y/n): ")
if cleanup.lower() == 'y':
    os.remove(csv_filename)
    os.remove(parquet_filename)
    print("Files deleted.")
else:
    print(f"Files preserved: {csv_filename}, {parquet_filename}")