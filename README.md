# zbq

A lightweight, enhanced wrapper around Google Cloud BigQuery and Storage with Polars integration. Simplifies querying and data operations with a unified interface, supporting read, write, insert, delete operations on BigQuery tables, and advanced file upload/download with pattern matching, parallel processing, and comprehensive error handling.

## Features

### BigQuery Operations
* Transparent BigQuery client initialization with automatic project and credentials detection
* Use Polars DataFrames seamlessly for input/output  
* Unified methods for CRUD operations with SQL and DataFrame inputs
* Supports table creation, overwrite warnings, and write mode control
* Context manager support for client lifecycle management
* Enhanced error handling with custom exceptions and retry logic

### Storage Operations  
* **Advanced pattern matching** - Multiple include/exclude patterns, regex support, case-insensitive matching
* **Parallel uploads/downloads** - Configurable thread pool for better performance
* **Progress tracking** - Built-in callbacks and detailed operation statistics
* **Dry-run mode** - Preview operations without executing
* **Retry logic** - Automatic retry with exponential backoff for failed operations  
* **Comprehensive logging** - Structured logging with configurable levels
* **Operation results** - Detailed statistics including file counts, bytes transferred, duration, and errors

## Installation

```bash
pip install zbq
```

## Quick Start

### BigQuery Operations

```python
from zbq import zclient

# Simple query
df = zclient.read("SELECT * FROM `project.dataset.table` LIMIT 1000")
print(df)

# Write DataFrame to BigQuery
result = zclient.write(
    df=df,
    full_table_path="project.dataset.new_table", 
    write_type="truncate",  # or "append"
    warning=True
)

# CRUD operations
zclient.insert("INSERT INTO `project.dataset.table` VALUES (...)")
zclient.update("UPDATE `project.dataset.table` SET col = 'value' WHERE id = 1")
zclient.delete("DELETE FROM `project.dataset.table` WHERE id = 1")
```

### Storage Operations

#### Basic Upload/Download
```python
from zbq import zstorage

# Simple upload with pattern
result = zstorage.upload(
    local_dir="./data",
    bucket_name="my-bucket",
    include_patterns="*.xlsx"  # Upload only Excel files
)

print(f"Uploaded {result.uploaded_files}/{result.total_files} files")
print(f"Total size: {result.total_bytes:,} bytes in {result.duration:.2f}s")

# Simple download  
result = zstorage.download(
    bucket_name="my-bucket", 
    local_dir="./downloads",
    include_patterns="*.csv"  # Download only CSV files
)
```

#### Advanced Pattern Matching
```python
# Multiple include patterns
result = zstorage.upload(
    local_dir="./reports",
    bucket_name="my-bucket", 
    include_patterns=["*.xlsx", "*.csv", "*.json"],  # Multiple file types
    exclude_patterns=["temp_*", "*_backup.*"],       # Exclude temporary/backup files
    case_sensitive=False  # Case-insensitive matching
)

# Regex patterns for complex matching
result = zstorage.upload(
    local_dir="./logs",
    bucket_name="my-bucket",
    include_patterns=r"log_\d{4}-\d{2}-\d{2}\.txt",  # Match log_YYYY-MM-DD.txt
    use_regex=True
)
```

#### Parallel Processing & Progress Tracking
```python
def progress_callback(completed, total):
    percentage = (completed / total) * 100
    print(f"Progress: {completed}/{total} files ({percentage:.1f}%)")

# Parallel upload with progress tracking
result = zstorage.upload(
    local_dir="./large-dataset", 
    bucket_name="my-bucket",
    parallel=True,                    # Enable parallel uploads
    progress_callback=progress_callback,
    max_retries=5                     # Retry failed uploads
)

# Handle results
if result.failed_files > 0:
    print(f"⚠️  {result.failed_files} files failed to upload:")
    for error in result.errors:
        print(f"  - {error}")

print(f"✅ Successfully uploaded {result.uploaded_files} files")
print(f"📊 Total: {result.total_bytes:,} bytes in {result.duration:.2f}s")
```

#### Dry Run & Preview
```python
# Preview what would be uploaded without actually uploading
result = zstorage.upload(
    local_dir="./data",
    bucket_name="my-bucket", 
    include_patterns="*.parquet",
    dry_run=True  # Preview only
)

print(f"Would upload {result.total_files} files ({result.total_bytes:,} bytes)")
```

#### Advanced Download with Filtering
```python  
# Download with prefix filtering and patterns
result = zstorage.download(
    bucket_name="my-data-bucket",
    local_dir="./downloaded-reports", 
    prefix="reports/2024/",           # Only files under this prefix
    include_patterns=["*.xlsx", "*.pdf"],
    exclude_patterns="*_draft.*",     # Skip draft files
    parallel=True,
    max_results=500                   # Limit number of files to list
)
```

## Advanced Configuration

### Custom Logging
```python
from zbq import setup_logging, StorageHandler, BigQueryHandler

# Configure logging
logger = setup_logging("DEBUG")  # DEBUG, INFO, WARNING, ERROR

# Create handlers with custom settings
storage = StorageHandler(
    project_id="my-project",
    log_level="INFO", 
    max_workers=8  # More parallel workers
)

bq = BigQueryHandler(
    project_id="my-project",
    default_timeout=600,  # 10 minute timeout
    interactive_mode=False,  # Disable confirmation prompts
    log_level="DEBUG"
)
```

### Error Handling
```python
from zbq import ZbqAuthenticationError, ZbqOperationError, ZbqConfigurationError

try:
    result = zstorage.upload("./data", "my-bucket", include_patterns="*.csv")
except ZbqAuthenticationError:
    print("Authentication failed. Run: gcloud auth application-default login")
except ZbqConfigurationError:
    print("Configuration error. Check your project settings.")
except ZbqOperationError as e:
    print(f"Operation failed: {e}")
```

### Working with Results
```python
from zbq import UploadResult, DownloadResult

# Upload with detailed result handling
result: UploadResult = zstorage.upload(
    local_dir="./data",
    bucket_name="my-bucket",
    include_patterns=["*.json", "*.csv"]
)

# Detailed statistics
print(f"""
Upload Summary:
📁 Total files: {result.total_files}
✅ Uploaded: {result.uploaded_files} 
⏭️  Skipped: {result.skipped_files}
❌ Failed: {result.failed_files}
📊 Total size: {result.total_bytes:,} bytes
⏱️  Duration: {result.duration:.2f} seconds
""")

# Handle errors
if result.errors:
    print("Errors encountered:")
    for error in result.errors[:5]:  # Show first 5 errors
        print(f"  • {error}")
    if len(result.errors) > 5:
        print(f"  ... and {len(result.errors) - 5} more errors")
```

## Pattern Matching Guide

### Glob Patterns (Default)
- `*.xlsx` - All Excel files
- `data_*.csv` - CSV files starting with "data_" 
- `report_????_??.pdf` - Reports with specific naming pattern
- `**/*.json` - All JSON files in subdirectories (recursive)
- `[!.]*.txt` - Text files not starting with dot

### Regex Patterns
```python
# Enable regex with use_regex=True
zstorage.upload(
    local_dir="./logs",
    bucket_name="my-bucket", 
    include_patterns=[
        r"access_log_\d{4}-\d{2}-\d{2}\.log",  # access_log_2024-01-01.log
        r"error_log_\d{8}\.log"                # error_log_20240101.log  
    ],
    use_regex=True
)
```

### Complex Filtering
```python
# Include multiple types, exclude temp files
zstorage.upload(
    local_dir="./workspace",
    bucket_name="my-bucket",
    include_patterns=["*.py", "*.json", "*.md", "*.yml"],
    exclude_patterns=["__pycache__/*", "*.pyc", "temp_*", ".git/*"],
    case_sensitive=False
)
```

## Authentication & Setup

1. **Install Google Cloud SDK**:
   ```bash
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Or set environment variables**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json" 
   export GOOGLE_CLOUD_PROJECT="your-project-id"
   ```

## Requirements

- Python ≥ 3.11
- Google Cloud project with BigQuery and/or Storage APIs enabled
- Appropriate IAM permissions for your operations

## Migration from v0.1.7

The new version maintains backward compatibility. Old method signatures still work:

```python
# Old way (still works)
zstorage.upload("./data", "my-bucket", pattern="*.csv")

# New way (recommended) 
zstorage.upload("./data", "my-bucket", include_patterns="*.csv")
```

## Performance Tips

1. **Use parallel processing** for multiple files: `parallel=True`
2. **Adjust thread count** based on your system: `max_workers=8`
3. **Use dry-run** to preview large operations first
4. **Filter early** with specific patterns to avoid processing unwanted files
5. **Monitor progress** with callback functions for long operations

## Contributing

Issues and pull requests welcome at the project repository.

## License

See LICENSE file for details.