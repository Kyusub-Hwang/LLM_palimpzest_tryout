from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Dict

from pydantic import BaseModel


class MomaDatasetItemType(Enum):
    RELATIONAL_DB = auto(),
    FILE_DATASET = auto(),


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
