from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict

from pydantic import BaseModel


class MomaDatasetItemType(Enum):
    RELATIONAL_DB = "relational_db"
    FILE_DATASET = "file_dataset"


class MomaDatasetItem(BaseModel, ABC):
    id: str
    description: str
    type: MomaDatasetItemType
    # TODO : Hard type this
    properties: Dict[str, Any] = {}

    @property
    @abstractmethod
    def content(self) -> Dict[str, Any]:
        pass

    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        pass
