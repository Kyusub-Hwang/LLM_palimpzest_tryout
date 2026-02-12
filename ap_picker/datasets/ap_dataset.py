from json import load
from pathlib import Path
from typing import Any, Dict, List

from palimpzest import IterDataset
from pydantic import BaseModel

# NOTE: this schema now matches the dataset schema from the sample_api_response.json file
ap_references_schema = [
    {"name": "id", "type": str, "desc": "Dataset uuidv4"},
    {"name": "name", "type": str, "desc": "Dataset name"},
    {"name": "description", "type": str, "desc": "Dataset description"},
    {"name": "country", "type": str, "desc": "Country code", "optional": True},
    {"name": "inLanguage", "type": list, "desc": "Languages", "optional": True},
    {"name": "type", "type": str, "desc": "Type", "optional": True},
    {"name": "version", "type": str, "desc": "Version", "optional": True},
    {"name": "url", "type": str, "desc": "URL", "optional": True},
    {"name": "dg:headline", "type": str, "desc": "Headline", "optional": True},
    {"name": "dg:keywords", "type": list, "desc": "Keywords", "optional": True},
    {"name": "datePublished", "type": str,
        "desc": "Date published", "optional": True},
    {"name": "license", "type": str, "desc": "License", "optional": True},
    {"name": "dg:status", "type": str, "desc": "Status", "optional": True},
    {"name": "dg:fieldOfScience", "type": list,
        "desc": "Field of science", "optional": True},
    {"name": "sc:archivedAt", "type": str,
        "desc": "Archive location", "optional": True},
    {"name": "conformsTo", "type": str, "desc": "Conforms to", "optional": True},
    {"name": "citeAs", "type": str, "desc": "Citation", "optional": True}
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
    """
    Loads AP references from a sample_api_response.json file (API response format).
    Extracts all dataset nodes with id and description.
    """

    def _is_relational_db(self, dataset: Dict[str, Any]) -> bool:
        nodes = dataset.get("nodes", [])
        return any("Relational_Database" in node.get("labels", []) for node in nodes)

    def _get_db_name(self, dataset: Dict[str, Any]) -> str | None:
        nodes = dataset.get("nodes", [])
        for node in nodes:
            if "Relational_Database" in node.get("labels", []):
                props = node.get("properties", {})
                db_name = props.get("name")
                if isinstance(db_name, str) and db_name:
                    return db_name

                content_url = props.get("contentUrl")
                if isinstance(content_url, str) and content_url:
                    return content_url.rsplit("/", 1)[-1]

        return None

    def __init__(self, json_path: Path):
        if not json_path.exists() or not json_path.is_file():
            raise ValueError(f"File does not exist: {json_path}")

        with json_path.open("r", encoding="utf-8") as f:
            api_response = load(f)

        apsRefs: List[ApReference] = []
        datasets = api_response.get("datasets", [])
        for dataset in datasets:
            nodes = dataset.get("nodes", [])
            for node in nodes:
                # Accept nodes with a label containing 'Dataset'
                labels = node.get("labels", [])
                if any("Dataset" in label for label in labels):
                    props = node.get("properties", {})
                    id = node.get("id")
                    description = props.get("description", "")
                    apsRefs.append(ApReference(id=id, description=description))

        super().__init__(apsRefs)
