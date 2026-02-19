from os import environ
from typing import Any, Dict, Hashable, Self, Tuple, TypeGuard

from ap_picker.datasets.moma.items.item import MomaDatasetItemType, MomaDatasetMetadata

from .data_reader import DataReader
from .relational_db_reader import RelationalDbReader


class DataReaderFactory:
    """Factory for creating data readers based on dataset item"""

    @staticmethod
    def create(record: Dict[Hashable, Any]) -> Tuple[DataReader, MomaDatasetItemType]:
        """
        Create a data reader based on the type specified in a Palimpzest DataRecord.
        Args:
            record: A dictionary containing the dataset item information, including 'type' and 'metadata'.
        Returns:
            A tuple of (DataReader instance, MomaDatasetItemType)

        """
        type_raw = record.get("type")
        metadata = record.get("metadata")
        assert DataReaderFactory.__is_metadata(metadata), "Invalid metadata"

        match type_raw:
            case MomaDatasetItemType.RELATIONAL_DB.value:
                # Get the connection details from the environment variables and metadata
                host = environ.get("POSTGRES_HOST")
                port = environ.get("POSTGRES_PORT")
                user = environ.get("POSTGRES_USER")
                password = environ.get("POSTGRES_PASSWORD")
                assert host and port and user and password, "Database connection details are not fully set in environment variables"
                db_name = metadata.get("names", [None])[0]
                assert db_name is not None, "Expected database name in metadata"

                qs = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
                return RelationalDbReader(qs), MomaDatasetItemType.RELATIONAL_DB
            case _:
                raise ValueError(f"Unsupported data reader type: {type_raw}")

    @staticmethod
    def __is_metadata(value: Any) -> TypeGuard[MomaDatasetMetadata]:
        if not isinstance(value, dict):
            return False

        return "names" in value and isinstance(value["names"], list)
