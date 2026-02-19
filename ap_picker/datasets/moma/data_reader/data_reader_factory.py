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
                db_name = metadata.get("names", [None])[0]
                assert db_name is not None, "Expected database name in metadata"
                return RelationalDbReader(db_name), MomaDatasetItemType.RELATIONAL_DB
            case _:
                raise ValueError(f"Unsupported data reader type: {type_raw}")

    @staticmethod
    def __is_metadata(value: Any) -> TypeGuard[MomaDatasetMetadata]:
        if not isinstance(value, dict):
            return False

        return "names" in value and isinstance(value["names"], list)
