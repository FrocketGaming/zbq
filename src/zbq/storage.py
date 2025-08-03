from typing import List, Dict, Optional, Union, Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from polars.exceptions import PolarsError
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from src.zbq.base import ZbqOperationError, BaseClientManager
from dataclasses import dataclass
from pathlib import Path
from tqdm import tqdm
import os
import fnmatch
import re
import time

@dataclass
class UploadResult:
    """Result of upload operations"""

    total_files: int
    uploaded_files: int
    skipped_files: int
    failed_files: int
    total_bytes: int
    duration: float
    errors: List[str]


@dataclass
class DownloadResult:
    """Result of download operations"""

    total_files: int
    downloaded_files: int
    failed_files: int
    total_bytes: int
    duration: float
    errors: List[str]


class ProgressBarWrapper:
    """Wrapper for combining tqdm progress bar with custom callbacks"""
    
    def __init__(self, total: int, show_progress: bool = True, description: str = "Processing", 
                 unit: str = "files", custom_callback: Optional[Callable[[int, int], None]] = None):
        self.total = total
        self.show_progress = show_progress
        self.custom_callback = custom_callback
        self.completed = 0
        
        if self.show_progress and total > 1:  # Only show for multiple files
            self.pbar = tqdm(
                total=total,
                desc=description,
                unit=unit,
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
        else:
            self.pbar = None
    
    def update(self, completed: int, total: int):
        """Update progress bar and call custom callback"""
        self.completed = completed
        
        if self.pbar:
            # Update progress bar to current completion
            self.pbar.n = completed
            self.pbar.refresh()
        
        if self.custom_callback:
            self.custom_callback(completed, total)
    
    def close(self):
        """Close progress bar"""
        if self.pbar:
            self.pbar.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class StorageHandler(BaseClientManager):
    """Enhanced Google Cloud Storage handler with pattern matching and progress tracking"""

    def __init__(
        self, project_id: str = "", log_level: str = "INFO", max_workers: int = 4
    ):
        super().__init__(project_id, log_level)
        self.max_workers = max_workers

    def upload(
        self,
        local_dir: str,
        bucket_path: str,
        include_patterns: Optional[Union[str, List[str]]] = None,
        exclude_patterns: Optional[Union[str, List[str]]] = None,
        case_sensitive: bool = True,
        use_regex: bool = False,
        dry_run: bool = False,
        parallel: bool = True,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        show_progress: Optional[bool] = None,  # Auto-detect if None
        max_retries: int = 3,
    ) -> UploadResult:
        """
        Upload files from local directory to Google Cloud Storage bucket with enhanced pattern matching

        Args:
            local_dir: Local directory path to upload from
            bucket_path: GCS bucket path (e.g., "my-bucket" or "my-bucket/folder/subfolder")
            include_patterns: Pattern(s) to include (e.g., "*.xlsx", ["*.csv", "*.json"])
            exclude_patterns: Pattern(s) to exclude (e.g., "temp_*", ["*.tmp", "*.log"])
            case_sensitive: Whether pattern matching is case sensitive
            use_regex: Use regex patterns instead of glob patterns
            dry_run: Preview operation without actual upload
            parallel: Use parallel uploads for better performance
            progress_callback: Optional callback function for progress updates
            show_progress: Show progress bar (None=auto-detect, True=always, False=never)
            max_retries: Number of retry attempts for failed uploads

        Returns:
            UploadResult with detailed statistics
        """

        # Parse bucket path into bucket name and prefix
        bucket_name, prefix = self.parse_bucket_path(bucket_path)

        start_time = time.time()
        local_path = Path(local_dir)

        if not local_path.exists():
            raise ZbqOperationError(f"Local directory does not exist: {local_dir}")

        self.logger.info(f"Starting upload from {local_dir} to {bucket_path}")

        # Collect files to upload
        files_to_upload = []
        total_bytes = 0

        for root, _, files in os.walk(local_dir):
            for file in files:
                if self.match_patterns(
                    file, include_patterns, exclude_patterns, case_sensitive, use_regex
                ):
                    local_file_path = Path(root) / file
                    relative_path = local_file_path.relative_to(local_path)
                    file_size = local_file_path.stat().st_size

                    # Prepend prefix to blob path if specified
                    blob_path = (
                        f"{prefix}{relative_path}" if prefix else str(relative_path)
                    )

                    files_to_upload.append(
                        {
                            "local_path": local_file_path,
                            "blob_path": blob_path,
                            "size": file_size,
                        }
                    )
                    total_bytes += file_size

        result = UploadResult(
            total_files=len(files_to_upload),
            uploaded_files=0,
            skipped_files=0,
            failed_files=0,
            total_bytes=total_bytes,
            duration=0.0,
            errors=[],
        )

        if dry_run:
            self.logger.info(
                f"DRY RUN: Would upload {len(files_to_upload)} files ({total_bytes:,} bytes)"
            )
            for file_info in files_to_upload:
                self.logger.info(
                    f"  Would upload: {file_info['blob_path']} ({file_info['size']:,} bytes)"
                )
            result.duration = time.time() - start_time
            return result

        if not files_to_upload:
            self.logger.info("No files found matching the specified patterns")
            result.duration = time.time() - start_time
            return result

        # Auto-detect progress bar display
        if show_progress is None:
            show_progress = len(files_to_upload) > 1 and not dry_run

        # Upload files with progress tracking
        with ProgressBarWrapper(
            total=len(files_to_upload),
            show_progress=show_progress,
            description="Uploading",
            custom_callback=progress_callback
        ) as progress:
            if parallel and len(files_to_upload) > 1:
                result = self._upload_parallel(
                    bucket_name, files_to_upload, result, progress.update, max_retries
                )
            else:
                result = self._upload_sequential(
                    bucket_name, files_to_upload, result, progress.update, max_retries
                )

        result.duration = time.time() - start_time
        self.logger.info(
            f"Upload completed: {result.uploaded_files}/{result.total_files} files "
            f"in {result.duration:.2f}s"
        )

        return result


    def match_patterns(
        filename: str,
        include_patterns: Optional[Union[str, List[str]]] = None,
        exclude_patterns: Optional[Union[str, List[str]]] = None,
        case_sensitive: bool = True,
        use_regex: bool = False,
    ) -> bool:
        """
        Check if filename matches include patterns and doesn't match exclude patterns

        Args:
            filename: Name of file to check
            include_patterns: Pattern(s) to include (None means include all)
            exclude_patterns: Pattern(s) to exclude (None means exclude none)
            case_sensitive: Whether pattern matching is case sensitive
            use_regex: Whether to use regex instead of glob patterns

        Returns:
            True if file should be processed, False otherwise
        """
        if not case_sensitive:
            filename = filename.lower()

        # Convert single patterns to lists
        if isinstance(include_patterns, str):
            include_patterns = [include_patterns]
        if isinstance(exclude_patterns, str):
            exclude_patterns = [exclude_patterns]

        # Apply case insensitivity to patterns
        if not case_sensitive:
            if include_patterns:
                include_patterns = [p.lower() for p in include_patterns]
            if exclude_patterns:
                exclude_patterns = [p.lower() for p in exclude_patterns]

        # Check include patterns
        if include_patterns:
            included = False
            for pattern in include_patterns:
                if use_regex:
                    if re.match(pattern, filename):
                        included = True
                        break
                else:
                    if fnmatch.fnmatch(filename, pattern):
                        included = True
                        break
            if not included:
                return False

        # Check exclude patterns
        if exclude_patterns:
            for pattern in exclude_patterns:
                if use_regex:
                    if re.match(pattern, filename):
                        return False
                else:
                    if fnmatch.fnmatch(filename, pattern):
                        return False

        return True

    def retry_operation(
        operation: Callable,
        max_retries: int = 3,
        delay: float = 1.0,
        backoff_factor: float = 2.0,
    ) -> Any:
        """Retry an operation with exponential backoff"""
        for attempt in range(max_retries):
            try:
                return operation()
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(delay * (backoff_factor**attempt))
        return None


    def _upload_sequential(
        self,
        bucket_name: str,
        files_to_upload: List[Dict],
        result: UploadResult,
        progress_callback: Optional[Callable],
        max_retries: int,
    ) -> UploadResult:
        """Upload files sequentially"""
        with self._fresh_client() as client:
            bucket = client.bucket(bucket_name)

            for i, file_info in enumerate(files_to_upload):
                try:

                    def upload_operation():
                        blob = bucket.blob(file_info["blob_path"])
                        blob.upload_from_filename(str(file_info["local_path"]))
                        return True

                    self.retry_operation(upload_operation, max_retries)
                    result.uploaded_files += 1
                    self.logger.debug(f"Uploaded: {file_info['blob_path']}")

                except Exception as e:
                    result.failed_files += 1
                    error_msg = f"Failed to upload {file_info['blob_path']}: {str(e)}"
                    result.errors.append(error_msg)
                    self.logger.error(error_msg)

                if progress_callback:
                    progress_callback(i + 1, len(files_to_upload))

        return result

    def parse_bucket_path(bucket_path: str) -> tuple[str, str]:
        """
        Parse a bucket path into bucket name and prefix

        Args:
            bucket_path: Either "bucket-name" or "bucket-name/path/to/folder"

        Returns:
            Tuple of (bucket_name, prefix)

        Examples:
            "my-bucket" -> ("my-bucket", "")
            "my-bucket/folder" -> ("my-bucket", "folder/")
            "my-bucket/path/to/folder" -> ("my-bucket", "path/to/folder/")
        """
        # Remove gs:// prefix if present
        if bucket_path.startswith("gs://"):
            bucket_path = bucket_path[5:]

        # Split into parts
        parts = bucket_path.split("/", 1)
        bucket_name = parts[0]

        if len(parts) > 1 and parts[1]:
            # Ensure prefix ends with /
            prefix = parts[1]
            if not prefix.endswith("/"):
                prefix += "/"
        else:
            prefix = ""

        return bucket_name, prefix

    def _upload_parallel(
        self,
        bucket_name: str,
        files_to_upload: List[Dict],
        result: UploadResult,
        progress_callback: Optional[Callable],
        max_retries: int,
    ) -> UploadResult:
        """Upload files in parallel using ThreadPoolExecutor"""
        completed_files = 0

        def upload_file(file_info):
            try:
                with self._fresh_client() as client:
                    bucket = client.bucket(bucket_name)

                    def upload_operation():
                        blob = bucket.blob(file_info["blob_path"])
                        blob.upload_from_filename(str(file_info["local_path"]))
                        return True

                    self.retry_operation(upload_operation, max_retries)
                    return {"success": True, "file_info": file_info, "error": None}

            except Exception as e:
                return {"success": False, "file_info": file_info, "error": str(e)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(upload_file, file_info): file_info
                for file_info in files_to_upload
            }

            for future in as_completed(future_to_file):
                upload_result = future.result()
                completed_files += 1

                if upload_result["success"]:
                    result.uploaded_files += 1
                    self.logger.debug(
                        f"Uploaded: {upload_result['file_info']['blob_path']}"
                    )
                else:
                    result.failed_files += 1
                    error_msg = f"Failed to upload {upload_result['file_info']['blob_path']}: {upload_result['error']}"
                    result.errors.append(error_msg)
                    self.logger.error(error_msg)

                if progress_callback:
                    progress_callback(completed_files, len(files_to_upload))

        return result

    def download(
        self,
        bucket_path: str,
        local_dir: str,
        include_patterns: Optional[Union[str, List[str]]] = None,
        exclude_patterns: Optional[Union[str, List[str]]] = None,
        case_sensitive: bool = True,
        use_regex: bool = False,
        dry_run: bool = False,
        parallel: bool = True,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        show_progress: Optional[bool] = None,  # Auto-detect if None
        max_retries: int = 3,
        max_results: int = 1000,
    ) -> DownloadResult:
        """
        Download files from Google Cloud Storage bucket with enhanced pattern matching

        Args:
            bucket_path: GCS bucket path (e.g., "my-bucket" or "my-bucket/folder/subfolder")
            local_dir: Local directory to download files to
            include_patterns: Pattern(s) to include (e.g., "*.xlsx", ["*.csv", "*.json"])
            exclude_patterns: Pattern(s) to exclude (e.g., "temp_*", ["*.tmp", "*.log"])
            case_sensitive: Whether pattern matching is case sensitive
            use_regex: Use regex patterns instead of glob patterns
            dry_run: Preview operation without actual download
            parallel: Use parallel downloads for better performance
            progress_callback: Optional callback function for progress updates
            show_progress: Show progress bar (None=auto-detect, True=always, False=never)
            max_retries: Number of retry attempts for failed downloads
            max_results: Maximum number of blobs to list from bucket

        Returns:
            DownloadResult with detailed statistics
        """
        # Parse bucket path
        bucket_name, combined_prefix = self.parse_bucket_path(bucket_path)

        start_time = time.time()
        local_path = Path(local_dir)
        local_path.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting download from {bucket_path} to {local_dir}")

        # List and filter blobs
        blobs_to_download = []
        total_bytes = 0

        with self._fresh_client() as client:
            bucket = client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=combined_prefix, max_results=max_results)

            for blob in blobs:
                if not blob.name or blob.name.endswith("/"):
                    continue  # Skip directory markers

                blob_filename = Path(blob.name).name
                if self.match_patterns(
                    blob_filename,
                    include_patterns,
                    exclude_patterns,
                    case_sensitive,
                    use_regex,
                ):
                    relative_path = (
                        blob.name[len(combined_prefix) :]
                        if combined_prefix
                        else blob.name
                    )
                    local_file_path = local_path / relative_path

                    blobs_to_download.append(
                        {
                            "blob": blob,
                            "local_path": local_file_path,
                            "size": blob.size or 0,
                        }
                    )
                    total_bytes += blob.size or 0

        result = DownloadResult(
            total_files=len(blobs_to_download),
            downloaded_files=0,
            failed_files=0,
            total_bytes=total_bytes,
            duration=0.0,
            errors=[],
        )

        if dry_run:
            self.logger.info(
                f"DRY RUN: Would download {len(blobs_to_download)} files ({total_bytes:,} bytes)"
            )
            for blob_info in blobs_to_download:
                self.logger.info(
                    f"  Would download: {blob_info['blob'].name} -> {blob_info['local_path']}"
                )
            result.duration = time.time() - start_time
            return result

        if not blobs_to_download:
            self.logger.info("No files found matching the specified patterns")
            result.duration = time.time() - start_time
            return result

        # Auto-detect progress bar display
        if show_progress is None:
            show_progress = len(blobs_to_download) > 1 and not dry_run

        # Download files with progress tracking
        with ProgressBarWrapper(
            total=len(blobs_to_download),
            show_progress=show_progress,
            description="Downloading",
            custom_callback=progress_callback
        ) as progress:
            if parallel and len(blobs_to_download) > 1:
                result = self._download_parallel(
                    blobs_to_download, result, progress.update, max_retries
                )
            else:
                result = self._download_sequential(
                    blobs_to_download, result, progress.update, max_retries
                )

        result.duration = time.time() - start_time
        self.logger.info(
            f"Download completed: {result.downloaded_files}/{result.total_files} files "
            f"in {result.duration:.2f}s"
        )

        return result

    def _download_sequential(
        self,
        blobs_to_download: List[Dict],
        result: DownloadResult,
        progress_callback: Optional[Callable],
        max_retries: int,
    ) -> DownloadResult:
        """Download files sequentially"""
        for i, blob_info in enumerate(blobs_to_download):
            try:
                # Ensure directory exists
                blob_info["local_path"].parent.mkdir(parents=True, exist_ok=True)

                def download_operation():
                    blob_info["blob"].download_to_filename(str(blob_info["local_path"]))
                    return True

                self.retry_operation(download_operation, max_retries)
                result.downloaded_files += 1
                self.logger.debug(f"Downloaded: {blob_info['blob'].name}")

            except Exception as e:
                result.failed_files += 1
                error_msg = f"Failed to download {blob_info['blob'].name}: {str(e)}"
                result.errors.append(error_msg)
                self.logger.error(error_msg)

            if progress_callback:
                progress_callback(i + 1, len(blobs_to_download))

        return result

    def _download_parallel(
        self,
        blobs_to_download: List[Dict],
        result: DownloadResult,
        progress_callback: Optional[Callable],
        max_retries: int,
    ) -> DownloadResult:
        """Download files in parallel using ThreadPoolExecutor"""
        completed_files = 0

        def download_file(blob_info):
            try:
                # Ensure directory exists
                blob_info["local_path"].parent.mkdir(parents=True, exist_ok=True)

                def download_operation():
                    blob_info["blob"].download_to_filename(str(blob_info["local_path"]))
                    return True

                self.retry_operation(download_operation, max_retries)
                return {"success": True, "blob_info": blob_info, "error": None}

            except Exception as e:
                return {"success": False, "blob_info": blob_info, "error": str(e)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_blob = {
                executor.submit(download_file, blob_info): blob_info
                for blob_info in blobs_to_download
            }

            for future in as_completed(future_to_blob):
                download_result = future.result()
                completed_files += 1

                if download_result["success"]:
                    result.downloaded_files += 1
                    self.logger.debug(
                        f"Downloaded: {download_result['blob_info']['blob'].name}"
                    )
                else:
                    result.failed_files += 1
                    error_msg = f"Failed to download {download_result['blob_info']['blob'].name}: {download_result['error']}"
                    result.errors.append(error_msg)
                    self.logger.error(error_msg)

                if progress_callback:
                    progress_callback(completed_files, len(blobs_to_download))

        return result

    def _create_client(self):
        return storage.Client(project=self.project_id)
