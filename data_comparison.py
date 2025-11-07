import pandas as pd
import os
import time

# Create a realistic ML dataset (10,000 rows)
# This simulates customer purchase data - common in ML problems
data = {
    'customer_id': range(1, 10001),
    'age': [25, 34, 45, 29, 52, 38, 41, 27, 55, 33] * 1000,
    'purchase_amount': [100.50, 250.75, 89.99, 450.00, 120.25, 
                        300.00, 175.50, 95.00, 410.25, 220.00] * 1000,
    'product_category': ['Electronics', 'Clothing', 'Food', 'Books', 
                         'Sports', 'Home', 'Beauty', 'Toys', 
                         'Garden', 'Automotive'] * 1000,
    'purchase_date': pd.date_range('2024-01-01', periods=10000, freq='h'),
    'quantity': [1, 2, 3, 1, 4, 2, 1, 3, 2, 1] * 1000,
    'discount_applied': [True, False, True, False, True, 
                         False, True, False, True, False] * 1000
}

df = pd.DataFrame(data)

print(f"Dataset shape: {df.shape}")
print(f"\nFirst few rows:")
print(df.head())
print(f"\nData types:")
print(df.dtypes)

# Save in different formats and measure time
formats = {
    'CSV': lambda: df.to_csv('data.csv', index=False),
    'JSON': lambda: df.to_json('data.json', orient='records'),
    'Parquet (uncompressed)': lambda: df.to_parquet('data_uncompressed.parquet', compression=None),
    'Parquet (snappy)': lambda: df.to_parquet('data.parquet', compression='snappy'),
    'Parquet (gzip)': lambda: df.to_parquet('data_gzip.parquet', compression='gzip')
}

print("\n" + "="*60)
print("WRITE PERFORMANCE & FILE SIZES")
print("="*60)

results = []
for format_name, save_func in formats.items():
    start = time.time()
    save_func()
    write_time = time.time() - start
    
    # Get filename
    filename = format_name.split('(')[0].strip().lower().replace(' ', '_')
    if 'uncompressed' in format_name:
        filename = 'data_uncompressed.parquet'
    elif 'gzip' in format_name:
        filename = 'data_gzip.parquet'
    elif 'snappy' in format_name:
        filename = 'data.parquet'
    else:
        filename = f'data.{filename}'
    
    size_mb = os.path.getsize(filename) / (1024 * 1024)
    results.append({
        'Format': format_name,
        'Write Time (s)': f"{write_time:.3f}",
        'Size (MB)': f"{size_mb:.2f}",
        'File': filename
    })
    print(f"{format_name:25} | Write: {write_time:.3f}s | Size: {size_mb:.2f} MB")

# Read performance test
print("\n" + "="*60)
print("READ PERFORMANCE")
print("="*60)

read_tests = {
    'CSV': lambda: pd.read_csv('data.csv'),
    'JSON': lambda: pd.read_json('data.json'),
    'Parquet (snappy)': lambda: pd.read_parquet('data.parquet')
}

for format_name, read_func in read_tests.items():
    start = time.time()
    df_read = read_func()
    read_time = time.time() - start
    print(f"{format_name:25} | Read: {read_time:.3f}s | Rows: {len(df_read)}")

print("\n" + "="*60)
print("KEY OBSERVATIONS")
print("="*60)
print("1. Parquet is typically smallest (columnar compression)")
print("2. Parquet read times are usually fastest")
print("3. JSON is largest (verbose text format)")
print("4. CSV is human-readable but inefficient")
print("5. Snappy compression balances speed and size")