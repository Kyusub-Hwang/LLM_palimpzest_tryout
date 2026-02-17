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
    {"name": "id", "type": str, "description": "Dataset uuidv4"},
    {"name": "description", "type": str,
        "description": "A description of the dataset"},
    {"name": "type", "type": str,  # Use str instead of enum for JSON serialization
        "description": "The type of the dataset, e.g. relational_db, file_dataset, etc."},
    {"name": "content", "type": Dict[str, Any],
        "description": "Content of the dataset, which may include tables, charts, text, etc."},
    {"name": "metadata", "type": Dict[str, Any],
             "description": "Metadata of the dataset, which may include information about the source, size, format, etc."},
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
        # Convert enum to string value for JSON serialization
        result['type'] = result['type'].value if isinstance(
            result['type'], MomaDatasetItemType) else result['type']
        result['content'] = item.content
        result['metadata'] = item.metadata
        return result

    def expand(self):
        """
        Expand MomaDataset meta-records into individual records from each dataset.
        Wraps each record in an envelope schema with source information.

        Returns:
            IterDataset with envelope schema: {
                source_dataset_id: str,
                source_dataset_desc: str,
                source_dataset_type: str,
                record_data: dict
            }
        """
        # Define the envelope schema (no leading underscores - Pydantic requirement)
        envelope_schema = [
            {"name": "source_dataset_id", "type": str,
                "description": "ID of the source dataset"},
            {"name": "source_dataset_desc", "type": str,
                "description": "Description of the source dataset"},
            {"name": "source_dataset_type", "type": str,
                "description": "Type of the source dataset"},
            {"name": "record_data",
                "type": Dict[str, Any], "description": "The actual record data"},
        ]

        # Use flat_map to expand each dataset into records
        return self.flat_map(
            udf=self._expand_dataset_to_records,
            cols=envelope_schema
        )

    @staticmethod
    def _expand_dataset_to_records(meta_record: dict) -> List[dict]:
        """
        UDF for flat_map: Expand a single dataset meta-record into multiple records.

        Args:
            meta_record: Dictionary with keys: id, description, type, content, metadata

        Returns:
            List of envelope-wrapped records from this dataset
        """
        dataset_id = meta_record.get("id", "unknown")
        description = meta_record.get("description", "")
        dataset_type_raw = meta_record.get("type")

        # Convert enum to string if needed
        if isinstance(dataset_type_raw, MomaDatasetItemType):
            dataset_type = dataset_type_raw
        elif isinstance(dataset_type_raw, str):
            # Try to parse string back to enum
            try:
                dataset_type = MomaDatasetItemType(dataset_type_raw)
            except (ValueError, KeyError):
                print(
                    f"Warning: Invalid dataset type string '{dataset_type_raw}' for {dataset_id}, skipping")
                return []
        else:
            print(f"Warning: Invalid dataset type for {dataset_id}, skipping")
            return []

        results = []
        try:
            # Import readers here to avoid circular imports
            from .data_reader import RelationalDbReader

            match dataset_type:
                case MomaDatasetItemType.RELATIONAL_DB:
                    # Get database name from metadata
                    metadata = meta_record.get("metadata", {})
                    db_name = metadata.get("names", [None])[0]
                    if not db_name:
                        print(f"Warning: No database name for {dataset_id}")
                        return []

                    reader = RelationalDbReader(db_name)

                    # Stream records from the database
                    for record in reader.read_stream():
                        # Wrap in envelope
                        envelope = {
                            "source_dataset_id": dataset_id,
                            "source_dataset_desc": description,
                            "source_dataset_type": str(dataset_type),
                            "record_data": record,
                        }
                        results.append(envelope)

                case MomaDatasetItemType.FILE_DATASET:
                    # For file datasets, we can't easily reconstruct the reader
                    # without the full node information
                    # Skip for now - this would need access to the original item
                    print(
                        f"Warning: File dataset expansion not yet fully implemented for {dataset_id}")
                    return []

                case _:
                    print(
                        f"Warning: Unsupported dataset type {dataset_type} for {dataset_id}")
                    return []

        except Exception as e:
            # Log error but don't fail completely
            print(
                f"Warning: Failed to read records from dataset {dataset_id}: {e}")

        return results

    def expand_and_extract(self, fields: List[str]):
        """
        Expand datasets to records and extract common fields via sem_map.
        This creates a unified schema across heterogeneous datasets.

        Args:
            fields: List of field names to extract (e.g., ["question", "answer"])

        Returns:
            IterDataset with schema containing source info + extracted fields
        """
        # First expand to envelope schema
        expanded = self.expand()

        # Build output schema with source info + extracted fields
        output_schema = [
            {"name": "source_dataset_id", "type": str,
                "description": "ID of the source dataset"},
            {"name": "source_dataset_desc", "type": str,
                "description": "Description of the source dataset"},
        ]

        for field in fields:
            output_schema.append({
                "name": field,
                "type": str,
                "description": f"Extracted field: {field}"
            })

        # Use sem_map to extract fields from record_data
        return expanded.sem_map(
            cols=output_schema,
            desc=f"Extract fields: {', '.join(fields)}"
        )

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
