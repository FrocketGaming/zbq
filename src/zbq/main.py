from polars.exceptions import PolarsError
from google.cloud import bigquery, storage
from google.auth.exceptions import DefaultCredentialsError
from google.auth import default as get_google_credentials
import polars as pl
import re
import tempfile
import os
import configparser

class BaseClientManager:
    def __init__(self):
        self._client = None # Lazy init
    
    def _create_client(self):
        raise NotImplementedError("Subclasses must implement _create_client()")

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

    def _init_client(self):
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
        self._client = self._create_client()
    
    @property
    def project_id(self):
        return self._project_id

    @project_id.setter
    def project_id(self, id: str):
        if not isinstance(id, str):
            raise ValueError("Project ID must be a string")
        if id != self._project_id:
            self._project_id = id
            self._close_client()

class StorageHandler(BaseClientManager):
    def __init__(self, project_id: str = ""):
        self._client = None # Lazy init
        self._project_id = project_id.strip() or self._get_default_project()
    
    def download(self, bucket_name: str, file_name: str, file_extension: str, prefix: str, local_dir: str):
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix, max_results=100)

        for blob in blobs:
            # Compute relative path
            relative_path = blob.name[len(prefix):]
            if not relative_path:  # skip "directory marker" blobs
                continue
            local_path = os.path.join(local_dir, relative_path)

            # Ensure the directory exists
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            blob.download_to_filename(local_path)

    @property
    def client(self):
        if self._client is None:
            self._init_client()
        return self._client

    def _create_client(self):
        return storage.Client(project=self.project_id)

class BigQueryHandler(BaseClientManager):
    def __init__(self, project_id: str = ""):
        self._project_id = project_id.strip() or self._get_default_project()
        self._client = None  # Lazy init

    @property
    def client(self) -> bigquery.Client:
        if self._client is None:
            self._init_client()
        return self._client

    def _close_client(self):
        if self._client:
            self._client.close()
            self._client = None
    
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
            return self._query(query)
        else:
            raise ValueError("Query is empty.")
    
    def insert(self, query: str):
        self.read(query)
    
    def update(self, query: str):
        self.read(query)
    
    def delete(self, query: str):
        self.read(query)

    def write(
        self,
        df: pl.DataFrame,
        full_table_path: str,
        write_type: str = "append",
        warning: bool = True,
        create_if_needed: bool = True):
        self._check_requirements(df, full_table_path)
        return self._write(df, full_table_path, write_type, warning, create_if_needed)

    def _check_requirements(self, df, full_table_path):
        if df.is_empty() or not full_table_path:
            missing = []
            if df.is_empty():
                missing.append("df")
            if not full_table_path:
                missing.append("full_table_path")
            raise ValueError(f"Missing required argument(s): {', '.join(missing)}")

    def _query(self, query: str) -> pl.DataFrame | pl.Series:
        try:
            query_job = self.client.query(query)

            if re.search(r"\b(insert|update|delete)\b", query, re.IGNORECASE):
                query_job.result()
                return pl.DataFrame(
                {"status": ["OK"], "job_id": [query_job.job_id]}
            )
            rows = query_job.result().to_arrow(progress_bar_type="tqdm")
            df = pl.from_arrow(rows)
        except PolarsError as e:
            print(f"PanicException: {e}")
            print("Retrying with Pandas DF")
            query_job = self.client.query(query)
            df = query_job.result().to_dataframe(progress_bar_type="tqdm")
            df = pl.from_pandas(df)

        return df

    def _write(
        self,
        df: pl.DataFrame,
        full_table_path: str,
        write_type: str = "append",
        warning: bool = True,
        create_if_needed: bool = True,
    ):
        destination = full_table_path
        temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        temp_file_path = temp_file.name
        temp_file.close()

        try:
            df.write_parquet(temp_file_path)

            if write_type == "truncate" and warning:
                user_warning = input(
                    "You are about to overwrite a table. Continue? (y/n): "
                )
                if user_warning.lower() != "y":
                    return

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

            with open(temp_file_path, "rb") as source_file:
                job = self.client.load_table_from_file(
                    source_file,
                    destination=destination,
                    project=self.project_id,
                    job_config=bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.PARQUET,
                        write_disposition=write_disp,
                        create_disposition=create_disp,
                    ),
                )
                return job.result().state

        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_client()
