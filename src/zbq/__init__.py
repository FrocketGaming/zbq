from .main import (
    BigQueryHandler, 
    StorageHandler, 
    UploadResult, 
    DownloadResult,
    ZbqError,
    ZbqAuthenticationError,
    ZbqConfigurationError,
    ZbqOperationError,
    setup_logging
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
    "setup_logging"
]
