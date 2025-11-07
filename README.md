# Data ETL Scripts

A collection of Python scripts for Extract, Transform, Load (ETL) operations and data format comparisons.

## Scripts Overview

### 1. CSV to Parquet Conversion (`csv_to_parquet_conversion.py`)
Demonstrates the performance benefits of converting CSV files to Parquet format:
- Creates sample customer data (100K+ rows)
- Compares file sizes and read/write performance
- Shows columnar storage advantages for ML workloads
- Validates data integrity after conversion

### 2. Data Format Comparison (`data_comparison.py`)
Benchmarks multiple data formats for ML datasets:
- Tests CSV, JSON, and Parquet (various compression levels)
- Measures write/read performance and file sizes
- Simulates realistic customer purchase data (10K rows)
- Provides format selection recommendations

### 3. Format Selection Guide (`format-guide.py`)
Generated reference guide for choosing optimal data formats:
- CSV: Human-readable, simple sharing
- JSON: Nested data, API responses
- Parquet: ML training, large datasets, columnar storage
- ORC: Hadoop ecosystems, better compression
- Avro: Schema evolution, streaming data

### 4. Multi-Source Data Pipeline (`multi_source_data_pipeline.py`)
Demonstrates ETL from multiple AWS data sources:
- Extracts from RDS (PostgreSQL)
- Reads from DynamoDB
- Loads existing data from S3
- Combines and processes datasets
- Saves results back to S3 in Parquet format

### 5. S3 Upload Utility (`upload_to_s3.py`)
Uploads data files to S3 for comparison:
- Batch uploads multiple file formats
- Compares storage sizes in S3
- Shows storage class information
- Error handling for failed uploads

## Prerequisites

```bash
pip install pandas numpy boto3 sqlalchemy pyarrow
```

## AWS Configuration
Ensure AWS credentials are configured:
```bash
aws configure
```

## Usage

1. **Format Comparison**: Run `data_comparison.py` to see performance differences
2. **CSV to Parquet**: Use `csv_to_parquet_conversion.py` for conversion workflows
3. **Multi-source ETL**: Adapt `multi_source_data_pipeline.py` for your data sources
4. **S3 Upload**: Modify bucket name in `upload_to_s3.py` and run

## Key Insights

- **Parquet** is optimal for ML training (columnar storage, compression)
- **CSV** for human readability and simple data sharing
- **JSON** for nested/hierarchical data structures
- File size reductions of 50-80% possible with Parquet
- Read performance improvements of 2-10x with Parquet vs CSV
