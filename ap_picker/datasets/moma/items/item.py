from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, TypedDict

from pydantic import BaseModel


class MomaDatasetItemType(Enum):
    """
    Represent the type of the underlying dataset item, 
    which determines how to read and query the data.
    """

    RELATIONAL_DB = "relational_db"
    FILE_DATASET = "file_dataset"


class MomaDatasetMetadata(TypedDict):
    """Structured metadata for a MoMa dataset item."""
    names: List[str]


class MomaDatasetItem(BaseModel, ABC):
    """A dataset node in MoMa"""

    # Moma id for the dataset
    id: str
    # Natural language description of the dataset
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
    def metadata(self) -> MomaDatasetMetadata:
        """Extracted dataset metadata fro the MomaNode"""
        pass
