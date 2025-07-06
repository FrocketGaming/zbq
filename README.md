# zbq

A lightweight, wrapper around Google Cloud BigQuery with Polars integration. Simplifies querying and data ingestion with a unified interface, supporting read, write, insert, and delete operations on BigQuery tables.

## Features
    Transparent BigQuery client initialization with automatic project and credentials detection

    Use Polars DataFrames seamlessly for input/output

    Unified .bq() method for CRUD operations with SQL and DataFrame inputs

    Supports table creation, overwrite warnings, and write mode control

    Context manager support for client lifecycle management
