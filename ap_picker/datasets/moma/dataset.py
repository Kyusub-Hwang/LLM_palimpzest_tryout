from json import load
from typing import Any, Dict, List, Optional

from palimpzest import IterDataset

from .items import (
    MomaDatasetItem,
    MomaDatasetItemFile,
    MomaDatasetItemRelationalDb,
    MomaDatasetItemType,
)

moma_dataset_schema = [
    {"name": "id", "type": str, "desc": "Dataset uuidv4"},
    {"name": "description", "type": str, "desc": "A description of the dataset"},
    {"name": "type", "type": MomaDatasetItemType,
        "desc": "The type of the dataset, e.g. relational_db, file_dataset, etc."},
    {"name": "content", "type": Dict[str, Any],
        "desc": "Content of the dataset, which may include tables, charts, text, etc."},
    {"name": "metadata", "type": Dict[str, Any],
             "desc": "Metadata of the dataset, which may include information about the source, size, format, etc."},
]


class MomaDataset(IterDataset):
    items: List[MomaDatasetItem]

    def __init__(self, *, path: Optional[str]):
        super().__init__(id="moma", schema=moma_dataset_schema)
        # TODO: add support for initization by URL
        assert path is not None, "Path to dataset JSON file must be provided"
        self.items = MomaDataset._from_file(path)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, idx: int) -> dict:
        item = self.items[idx]
        result = item.model_dump()
        result['content'] = item.content
        result['metadata'] = item.metadata
        return result

    @classmethod
    def _from_file(cls, path: str) -> List[MomaDatasetItem]:
        with open(path, "r") as f:
            payload = load(f)

        return cls._parse_items(payload)

    @classmethod
    def _parse_items(cls, datasets: Any) -> List[MomaDatasetItem]:

        items: List[MomaDatasetItem] = []
        if len(datasets) == 0:
            return items

        for dataset_entry in datasets:
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
                    properties=props,
                )

            else:
                # fallback / placeholder for now
                item = MomaDatasetItemFile(
                    id=dataset_id,
                    description=description,
                    properties=props,
                    raw_nodes=nodes,
                )

            items.append(item)

        return items
