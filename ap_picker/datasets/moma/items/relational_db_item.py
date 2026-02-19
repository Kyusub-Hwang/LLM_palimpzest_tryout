from typing import Any, Dict

from ..data_reader import DataReader, RelationalDbReader
from .item import MomaDatasetItem, MomaDatasetItemType, MomaDatasetMetadata


class MomaDatasetItemRelationalDb(MomaDatasetItem):

    def __init__(self, *, id: str, description: str, properties: Dict[str, Any]):
        super().__init__(
            id=id,
            description=description,
            type=MomaDatasetItemType.RELATIONAL_DB,
            properties=properties
        )

    @property
    def metadata(self) -> MomaDatasetMetadata:
        return {
            "names": [self.properties.get("name", "")]
        }

    @property
    def content(self) -> Dict[str, Any]:
        return self.properties
