from typing import Any, Dict

from ..data_reader import DataReader, RelationalDbReader
from .item import MomaDatasetItem, MomaDatasetItemType


class MomaDatasetItemRelationalDb(MomaDatasetItem):

    def __init__(self, *, id: str, description: str, properties: Dict[str, Any]):
        super().__init__(
            id=id,
            description=description,
            type=MomaDatasetItemType.RELATIONAL_DB,
            properties=properties
        )

    def get_reader(self) -> DataReader:
        """
        Get a reader for streaming data from this database.

        Returns:
            DataReader: Reader instance for streaming access
        """
        qs = "postgresql://provdemo:provdemo@postgres:5432/mathe"
        return RelationalDbReader(qs)

    @property
    def metadata(self) -> Dict[str, Any]:
        return {
            "names": [self.properties.get("name", "")]
        }

    @property
    def content(self) -> Dict[str, Any]:
        return {
            "tables": self.properties.get("tables", [])
        }
