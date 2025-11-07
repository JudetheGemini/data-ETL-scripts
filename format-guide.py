# Save this as a reference
format_guide = """
FORMAT SELECTION GUIDE FOR ML
==============================

CSV:
✓ When: Human readability needed, simple data sharing
✓ Use case: Initial data exploration, sharing with non-technical teams
✗ Avoid: Large datasets, production ML pipelines
  Reason: Slow to read, no schema enforcement, large file size

JSON:
✓ When: Nested/hierarchical data, API responses
✓ Use case: Configuration files, semi-structured data
✗ Avoid: Tabular data, large datasets
  Reason: Verbose, slow parsing, large file size

Parquet:
✓ When: Training ML models, large datasets, repeated reads
✓ Use case: Feature stores, data lakes, SageMaker training
✓ Why: Columnar storage = fast column reads (ML training typically reads all rows but specific columns)
✓ Compression: Built-in compression (snappy is good default)
  Exam tip: Parquet is almost always the right answer for ML training data

ORC (Apache Optimized Row Columnar):
✓ When: Similar to Parquet, common in Hadoop/Hive ecosystems
✓ Slightly better compression than Parquet
  Exam tip: Know it exists, similar use cases to Parquet

Avro:
✓ When: Schema evolution is important, streaming data
✓ Use case: Kafka messages, data that changes schema over time
✓ Row-based format (unlike Parquet)
  Exam tip: Good for write-heavy workloads, schema versioning

RecordIO (with Protobuf):
✓ When: Using SageMaker built-in algorithms (specific use case)
✓ Some SageMaker algorithms require this format
  Exam tip: Not common, but know SageMaker connection
"""

print(format_guide)

# Save it
with open('format_guide.txt', 'w') as f:
    f.write(format_guide)