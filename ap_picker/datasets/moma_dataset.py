from abc import ABC, abstractmethod
from json import load
from pathlib import Path
from typing import Any, Dict, List, Optional, Self

from palimpzest import IterDataset
from pydantic import BaseModel, PrivateAttr

moma_dataset_schema = [
    {"name": "id", "type": str, "desc": "Dataset uuidv4"},
    {"name": "description", "type": str, "desc": "A description of the dataset"},
    {"name": "content", "type": Dict[str, Any],
        "desc": "Content of the dataset, which may include tables, charts, text, etc."},
]


class MomaDatasetItem(BaseModel, ABC):
    id: str
    description: str

    @property
    @abstractmethod
    def content(self) -> Dict[str, Any]:
        pass


class MomaDatasetItemRelationalDb(MomaDatasetItem):
    @property
    def content(self) -> Dict[str, Any]:
        # In a real implementation, this would connect to a relational database and return its schema and sample data
        return {
            "type": "relational_db",
            "schema": {
                "tables": [
                    {
                        "name": "employees",
                        "columns": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                            {"name": "department", "type": "string"},
                        ],
                    },
                    {
                        "name": "departments",
                        "columns": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                        ],
                    },
                ],
            },
            "sample_data": {
                "employees": [
                    {"id": 1, "name": "Alice", "department": "Engineering"},
                    {"id": 2, "name": "Bob", "department": "HR"},
                ],
                "departments": [
                    {"id": 1, "name": "Engineering"},
                    {"id": 2, "name": "HR"},
                ],
            },
        }


class MomaDatasetItemFile(MomaDatasetItem):
    _raw_nodes: list = PrivateAttr()

    def __init__(self, *, id: str, description: str, raw_nodes: list):
        super().__init__(id=id, description=description)
        self._raw_nodes = raw_nodes

    @property
    def content(self) -> Dict[str, Any]:
        return {
            "type": "file_dataset",
            "nodes": self._raw_nodes,
        }


class MomaDataset(IterDataset):
    items: List[MomaDatasetItem]

    def __init__(self, *, path: Optional[str]):
        super().__init__(id="moma", schema=moma_dataset_schema)
        # TODO: add support for initization by URL
        assert path is not None, "Path to dataset JSON file must be provided"
        self.items = MomaDataset._parse_items(Path(path))

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, idx: int) -> dict:
        return self.items[idx].model_dump()

    @classmethod
    def _parse_items(cls, path: Path) -> List[MomaDatasetItem]:
        with open(path, "r") as f:
            payload = load(f)

        items: List[MomaDatasetItem] = []

        for dataset_entry in payload.get("datasets", []):
            nodes = dataset_entry.get("nodes", [])

            # 1. find the sc:Dataset node
            dataset_node = next(
                (n for n in nodes if "sc:Dataset" in n.get("labels", [])),
                None,
            )

            if dataset_node is None:
                continue

            dataset_id = dataset_node["id"]
            props = dataset_node.get("properties", {})
            description = props.get("description", "")

            labels = set(dataset_node.get("labels", []))

            # 2. choose concrete dataset item type
            if "Relational_Database" in labels:
                item = MomaDatasetItemRelationalDb(
                    id=dataset_id,
                    description=description,
                )

            else:
                # fallback / placeholder for now
                item = MomaDatasetItemFile(
                    id=dataset_id,
                    description=description,
                    raw_nodes=nodes,
                )

            items.append(item)

        return items
