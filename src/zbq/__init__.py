from .main import BigQueryHandler
from .storage import StorageHandler, UploadResult, DownloadResult
from .base import (
    ZbqError,
    ZbqAuthenticationError,
    ZbqConfigurationError,
    ZbqOperationError,
)

zclient = BigQueryHandler()
zstorage = StorageHandler()

__all__ = [
    "zclient", 
    "zstorage", 
    "BigQueryHandler", 
    "StorageHandler", 
    "UploadResult", 
    "DownloadResult",
    "ZbqError",
    "ZbqAuthenticationError", 
    "ZbqConfigurationError",
    "ZbqOperationError",
]
