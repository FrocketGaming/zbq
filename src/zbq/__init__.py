from .main import BigQueryHandler, StorageHandler

zclient = BigQueryHandler()
zstorage = StorageHandler()

__all__ = ["zclient", "zstorage"]
