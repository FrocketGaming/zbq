from polars.exceptions import PolarsError
from google.cloud import bigquery, storage
from google.auth.exceptions import DefaultCredentialsError
from google.auth import default as get_google_credentials
import polars as pl
import re
import tempfile
import os
import configparser
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager


class BaseClientManager:
    def __init__(self):
        pass

    def _create_client(self):
        raise NotImplementedError("Subclasses must implement _create_client()")

    @contextmanager
    def _fresh_client(self):
        """Context manager that provides a fresh client for each operation.
        
        This eliminates shared client state issues by creating a new client
        for each operation and ensuring proper cleanup.
        """
        temp_client = None
        try:
            if not self._check_adc():
                raise RuntimeError(
                    "No Google Cloud credentials found. Run:\n"
                    "  gcloud auth application-default login\n"
                    "Or set the GOOGLE_APPLICATION_CREDENTIALS environment variable."
                )
            if not self.project_id:
                raise RuntimeError(
                    "No GCP project found. Set one via:\n"
                    "  gcloud config set project YOUR_PROJECT_ID\n"
                    "Or set manually: zclient.project_id = 'your-project'"
                )
            
            temp_client = self._create_client()
            yield temp_client
            
        finally:
            if temp_client:
                try:
                    temp_client.close()
                except Exception:
                    pass  # Ignore cleanup errors

    def _get_default_project(self):
        config_path = os.path.expanduser(
            "~/.config/gcloud/configurations/config_default"
        )
        if os.name == "nt":  # Windows
            config_path = os.path.expandvars(
                r"%APPDATA%\gcloud\configurations\config_default"
            )

        parser = configparser.ConfigParser()
        try:
            parser.read(config_path)
            project = parser.get("core", "project", fallback="")
            return project.strip()
        except Exception:
            return os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()

        # Fallback to environment
        return os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()

    @property
    def client(self):
        if self._client is None:
            self._init_client()
        return self._client

    def _check_adc(self) -> bool:
        try:
            creds, proj = get_google_credentials()
            return True
        except DefaultCredentialsError:
            return False

    @property
    def project_id(self):
        return self._project_id

    @project_id.setter
    def project_id(self, id: str):
        if not isinstance(id, str):
            raise ValueError("Project ID must be a string")
        if id != self._project_id:
            self._project_id = id


class StorageHandler(BaseClientManager):
    def __init__(self, project_id: str = ""):
        self._project_id = project_id.strip() or self._get_default_project()

    def download(
        self,
        bucket_name: str,
        file_name: str,
        file_extension: str,
        prefix: str,
        local_dir: str,
    ):
        with self._fresh_client() as client:
            bucket = client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix, max_results=100)

            for blob in blobs:
                # Compute relative path
                relative_path = blob.name[len(prefix) :]
                if not relative_path:  # skip "directory marker" blobs
                    continue
                local_path = os.path.join(local_dir, relative_path)

                # Ensure the directory exists
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                blob.download_to_filename(local_path)

    def _create_client(self):
        return storage.Client(project=self.project_id)


class BigQueryHandler(BaseClientManager):
    def __init__(
        self,
        project_id: str = "",
        default_timeout: int = 300,
        interactive_mode: bool = True,
    ):
        self._project_id = project_id.strip() or self._get_default_project()
        self.default_timeout = default_timeout
        self.interactive_mode = interactive_mode

    def _create_client(self):
        return bigquery.Client(project=self.project_id)

    def validate(self):
        """Optional helper: raise if ADC or project_id not set"""
        if not self._check_adc():
            raise RuntimeError(
                "Missing ADC. Run: gcloud auth application-default login"
            )
        if not self.project_id:
            raise RuntimeError("Project ID not set.")

    def read(
        self,
        query: str | None = None,
        timeout: int = None,
    ):
        """
        Handles CRUD-style operations with BigQuery via a unified interface.

        Args:
            action (str): One of {"read", "write", "insert", "delete"}.
            df (pl.DataFrame, optional): Polars DataFrame to write to BigQuery. Required for "write".
            query (str, optional): SQL query string. Required for "read", "insert", and "delete".

        Returns:
            pl.DataFrame or str: A Polars DataFrame for "read", or a job state string for "write".

        Raises:
            ValueError: If required arguments are missing based on the action.
            RuntimeError: If authentication or project configuration is missing.
        """

        if query:
            try:
                return self._query(query, timeout)
            except TimeoutError as e:
                print(f"Read operation timed out: {e}")
                raise
            except Exception as e:
                print(f"Read operation failed: {e}")
                raise
        else:
            raise ValueError("Query is empty.")

    def insert(self, query: str, timeout: int = None):
        return self.read(query, timeout)

    def update(self, query: str, timeout: int = None):
        return self.read(query, timeout)

    def delete(self, query: str, timeout: int = None):
        return self.read(query, timeout)

    def write(
        self,
        df: pl.DataFrame,
        full_table_path: str,
        write_type: str = "append",
        warning: bool = True,
        create_if_needed: bool = True,
        timeout: int = None,
        interactive: bool = None,
    ):
        # Use instance default if not specified
        if interactive is None:
            interactive = self.interactive_mode

        # Temporarily override interactive mode for this operation
        original_interactive = self.interactive_mode
        self.interactive_mode = interactive

        try:
            self._check_requirements(df, full_table_path)
            return self._write(
                df, full_table_path, write_type, warning, create_if_needed, timeout
            )
        finally:
            # Restore original interactive mode
            self.interactive_mode = original_interactive

    def _check_requirements(self, df, full_table_path):
        if df.is_empty() or not full_table_path:
            missing = []
            if df.is_empty():
                missing.append("df")
            if not full_table_path:
                missing.append("full_table_path")
            raise ValueError(f"Missing required argument(s): {', '.join(missing)}")

    def _query(self, query: str, timeout: int = None) -> pl.DataFrame | pl.Series:
        timeout = timeout or self.default_timeout

        try:
            # Use fresh client for each query to eliminate shared state issues
            with self._fresh_client() as client:
                query_job = client.query(query)

                if re.search(r"\b(insert|update|delete)\b", query, re.IGNORECASE):
                    try:
                        query_job.result(timeout=timeout)
                        return pl.DataFrame(
                            {"status": ["OK"], "job_id": [query_job.job_id]}
                        )
                    except Exception as e:
                        if "timeout" in str(e).lower():
                            raise TimeoutError(f"Query timed out after {timeout} seconds")
                        raise

                try:
                    rows = query_job.result(timeout=timeout).to_arrow(
                        progress_bar_type=None
                    )
                    df = pl.from_arrow(rows)
                except Exception as e:
                    if "timeout" in str(e).lower():
                        raise TimeoutError(f"Query timed out after {timeout} seconds")
                    raise

        except PolarsError as e:
            print(f"PanicException: {e}")
            print("Retrying with Pandas DF")
            try:
                with self._fresh_client() as client:
                    query_job = client.query(query)
                    pandas_df = query_job.result(timeout=timeout).to_dataframe(
                        progress_bar_type=None
                    )
                    df = pl.from_pandas(pandas_df)
            except Exception as e:
                if "timeout" in str(e).lower():
                    raise TimeoutError(f"Query timed out after {timeout} seconds")
                raise

        return df

    def _write(
        self,
        df: pl.DataFrame,
        full_table_path: str,
        write_type: str = "append",
        warning: bool = True,
        create_if_needed: bool = True,
        timeout: int = None,
    ):
        timeout = timeout or self.default_timeout
        destination = full_table_path
        temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        temp_file_path = temp_file.name
        temp_file.close()

        try:
            df.write_parquet(temp_file_path)

            if write_type == "truncate" and warning:
                if self.interactive_mode:
                    try:
                        user_warning = input(
                            "You are about to overwrite a table. Continue? (y/n): "
                        )
                        if user_warning.lower() != "y":
                            return "CANCELLED"
                    except (EOFError, KeyboardInterrupt):
                        print("\nOperation cancelled by user")
                        return "CANCELLED"
                else:
                    print("Warning: Truncating table (interactive mode disabled)")

            write_disp = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if write_type == "truncate"
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            create_disp = (
                bigquery.CreateDisposition.CREATE_IF_NEEDED
                if create_if_needed
                else bigquery.CreateDisposition.CREATE_NEVER
            )

            # Use fresh client for write operation to eliminate shared state issues
            with self._fresh_client() as client:
                with open(temp_file_path, "rb") as source_file:
                    job = client.load_table_from_file(
                        source_file,
                        destination=destination,
                        project=self.project_id,
                        job_config=bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.PARQUET,
                            write_disposition=write_disp,
                            create_disposition=create_disp,
                        ),
                    )
                    # Add timeout to prevent hanging on job.result()
                    try:
                        result = job.result(timeout=timeout)
                        return result.state
                    except Exception as e:
                        if "timeout" in str(e).lower():
                            raise TimeoutError(
                                f"Write operation timed out after {timeout} seconds"
                            )
                        raise

        finally:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except OSError:
                    pass  # Ignore cleanup errors
