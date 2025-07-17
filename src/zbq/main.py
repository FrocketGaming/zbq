from polars.exceptions import PolarsError
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.auth import default as get_google_credentials
import polars as pl
import tempfile
import os
import subprocess


class BigQueryHandler:
    def __init__(self, project_id: str = ""):
        self._project_id = project_id.strip() or self._get_default_project()
        self._client = None  # Lazy init

    def _get_default_project(self) -> str:
        try:
            result = subprocess.run(
                ["gcloud", "config", "get", "project"],
                capture_output=True,
                text=True,
                check=True,
            )
            project = result.stdout.strip()
            if project:
                return project
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass

        # Fallback to environment
        return os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()

    def _check_adc(self) -> bool:
        try:
            get_google_credentials()
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
        self._client = bigquery.Client(project=self.project_id)

    @property
    def client(self):
        if self._client is None:
            self._init_client()
        return self._client

    def _close_client(self):
        if self._client:
            self._client.close()
            self._client = None

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

    def validate(self):
        """Optional helper: raise if ADC or project_id not set"""
        if not self._check_adc():
            raise RuntimeError(
                "Missing ADC. Run: gcloud auth application-default login"
            )
        if not self.project_id:
            raise RuntimeError("Project ID not set.")

    def bq(
        self,
        action: str,
        df: pl.DataFrame | None = None,
        query: str | None = None,
        full_table_path: str | None = None,
        write_type: str = "WRITE_APPEND",
        warning: bool = True,
        create_if_needed: bool = True,
    ):
        """
        Handles CRUD-style operations with BigQuery via a unified interface.

        Args:
            action (str): One of {"read", "write", "insert", "delete"}.
            df (pl.DataFrame, optional): Polars DataFrame to write to BigQuery. Required for "write".
            full_table_path (str, optional): Fully-qualified table path. Required for "write".
            query (str, optional): SQL query string. Required for "read", "insert", and "delete".
            write_type (str, optional): "WRITE_APPEND" or "WRITE_TRUNCATE". Default is "WRITE_APPEND".
            warning (bool, optional): Whether to prompt before truncating a table. Default is True.
            create_if_needed (bool, optional): Whether to create the table if it doesn't exist. Default is True.

        Returns:
            pl.DataFrame or str: A Polars DataFrame for "read", or a job state string for "write".

        Raises:
            ValueError: If required arguments are missing based on the action.
            RuntimeError: If authentication or project configuration is missing.
        """

        match action.lower():
            case "read" | "insert" | "delete":
                if query:
                    return self._query(query)
                else:
                    raise ValueError("Query is empty.")
            case "write":
                self._check_requirements(df, full_table_path)
                return self._write(
                    df, full_table_path, write_type, warning, create_if_needed
                )

    def _check_requirements(self, **kwargs):
        missing = [k for k, v in kwargs.items() if not v]
        if missing:
            raise ValueError(f"Missing required argument(s): {', '.join(missing)}")

    def _query(self, query: str) -> pl.DataFrame:
        try:
            query_job = self.client.query(query)
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
