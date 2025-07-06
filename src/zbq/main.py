from polars.exceptions import PolarsError
from google.cloud import bigquery
import polars as pl
import tempfile
import os
import subprocess


class BigQueryHandler:
    def __init__(self):
        self._project_id = self._get_default_project()
        if not self.project_id:
            print(
                "No default GCP project found. Please either:\n"
                " - configure a default project using `gcloud config set project YOUR_PROJECT_ID`\n"
                " - or manually set it in code via `zclient.project_id = 'YOUR_PROJECT_ID'`"
            )
            exit()
        self.client = self._init_client()

    def _get_default_project(self):
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

        env_project = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
        return env_project if env_project else ""

    def _init_client(self):
        self.client = bigquery.Client(project=self.project_id)

    def test_me(self):
        print("This class is working.")

    @property
    def project_id(self):
        return self._project_id

    @project_id.setter
    def project_id(self, id: str) -> None:
        if not isinstance(id, str):
            raise ValueError("Project ID must be a string")
        if id != self._project_id:
            # self.client.close() doesn't exist; skip or replace with _close()
            self._close()
            self._project_id = id
            self._init_client()

    def _close(self):
        if hasattr(self, "client") and self.client is not None:
            self.client.close()
            self.client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()

    def _full_table_path(self, dataset: str, table: str) -> str:
        if not isinstance(dataset, str) or not isinstance(table, str):
            raise ValueError("Dataset and table must be strings")
        return f"{self.project_id}.{dataset}.{table}"

    def query(self, query: str) -> pl.DataFrame:
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

    def execute(
        self,
        df: pl.DataFrame,
        dataset: str,
        table: str,
        write_type: str = "WRITE_APPEND",
        warning: bool = True,
        create_if_needed: bool = True
    ):
        destination = self._full_table_path(dataset, table)

        temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        temp_file_path = temp_file.name
        temp_file.close()  # Close it so Polars and BigQuery can both access it

        try:
            df.write_parquet(temp_file_path)

            if write_type == "WRITE_TRUNCATE" and warning:
                user_warning = input(
                    "You are about to overwrite a table. Continue? (y/n): "
                )
                if user_warning.lower() != "y":
                    return

            write_type = (
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if write_type == "WRITE_TRUNCATE"
                else bigquery.WriteDisposition.WRITE_APPEND
            )

            create_type = (
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
                        write_disposition=write_type,
                        create_disposition=create_type
                    ),
                )
                result = job.result()
                return result.state
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
