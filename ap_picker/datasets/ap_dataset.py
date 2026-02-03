from json import load
from pathlib import Path
from typing import List

from palimpzest import IterDataset
from pydantic import BaseModel
from requests import get

# NOTE: this schema doesn't seems compatible with the Pydantic Schema
ap_references_schema = [
    {"name": "id", "type": str, "desc": "AP uuidv4"},
    {"name": "description", "type": str,
        "desc": "High level description of what the Analytical Pattern does"},
]


class ApReference(BaseModel):
    """ A reference to a stored Analytical Pattern """
    id: str
    description: str


class ApDataset(IterDataset):

    def __init__(self, apsRefs: List[ApReference]):
        super().__init__(id="ap", schema=ap_references_schema)
        self._apsRef = apsRefs

    def __len__(self) -> int:
        return len(self._apsRef)

    def __getitem__(self, idx: int) -> dict:
        return self._apsRef[idx].model_dump()


class LocalApDataset(ApDataset):

    def __init__(self, folder: Path):
        if not folder.exists() or not folder.is_dir():
            raise ValueError(
                f"Folder does not exist or is not a directory: {folder}")

        apsRefs: List[ApReference] = []

        for path in sorted(folder.glob("*.json")):
            with path.open("r", encoding="utf-8") as f:
                graph = load(f)

            ap_node = graph.get("nodes")[0]
            id = ap_node.get("id")
            description = ap_node.get("properties", {}).get("description", "")
            apsRefs.append(ApReference(id=id, description=description))

        super().__init__(apsRefs)
