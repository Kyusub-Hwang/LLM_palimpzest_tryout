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
    # Filter conditions to be applied during expand() (combined with AND)
    _lazy_filters: List[str]

    def __init__(self, *, path: Optional[str] = None, items: Optional[List[MomaDatasetItem]] = None,
                 lazy_filters: Optional[List[str]] = None):
        """
        Initialize MomaDataset.

        Args:
            path: Path to dataset JSON file (for loading from file)
            items: Pre-loaded dataset items (for creating filtered instances)
            lazy_filters: List of filter conditions to apply during expand() (combined with AND)
        """
        super().__init__(id="moma", schema=moma_dataset_schema)

        if path is not None:
            self.items = MomaDataset._from_file(path)
        elif items is not None:
            self.items = items
        else:
            raise ValueError("Either 'path' or 'items' must be provided")

        self._lazy_filters = lazy_filters if lazy_filters is not None else []

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

    def find(self, filter_condition: str):
        return super().sem_filter(filter_condition, depends_on=["name", "description"])

    def sem_filter2(self, filter_condition: str, filter_rows_in_datasets=True):
        """
        Lazily filter dataset CONTENT. The filter is applied during expand().
        This is NOT executed immediately - it returns a new MomaDataset with the filter stored.

        Multiple sem_filter() calls can be chained - filters are combined with AND logic:
            moma.sem_filter("algebra").sem_filter("basic") 
            -> filters for records matching BOTH "algebra" AND "basic"

        For relational DBs: Applies SQL WHERE clause during expansion
        For file datasets: Could filter records during read (not yet implemented)

        Args:
            filter_condition: Natural language filter for data content
                            (e.g., "questions about algebra")

        Returns:
            New MomaDataset instance with lazy filter added to filter list

        Example:
            >>> moma.sem_filter("questions about algebra").expand().run()
            >>> # Filter applied when expand() is called

            >>> moma.sem_filter("algebra").sem_filter("basic level").expand().run()
            >>> # Both filters combined with AND
        """
        # Combine existing filters with new one
        new_filters = self._lazy_filters + [filter_condition]

        # Return new instance with updated filter list
        return MomaDataset(
            items=self.items,
            lazy_filters=new_filters
        )

    def expand(self):
        """
        Expand MomaDataset meta-records into individual records from each dataset.
        Wraps each record in an envelope schema with source information.

        Applies any lazy filters stored via sem_filter() during expansion:
        - Multiple filters are combined with AND logic
        - For relational DBs: Applies SQL WHERE clause for efficient filtering
        - For file datasets: Could filter during read (not yet implemented)

        Implementation:
        - Uses DatasetExpandConvert operator (a proper Palimpzest ConvertOp)
        - Provides cost estimates for query optimization
        - Integrates lazy filters into the expansion UDF

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

        # Import here to avoid circular dependency
        from palimpzest.core.lib.schemas import create_schema_from_fields

        from ap_picker.operators.dataset_expand import DatasetExpandConvert

        # Convert envelope_schema list to Pydantic model
        envelope_pydantic_schema = create_schema_from_fields(envelope_schema)

        # Create the expand operator with lazy filters
        # Note: We don't pass input_schema since it's set by Dataset operations
        expand_op = DatasetExpandConvert(
            output_schema=envelope_pydantic_schema,
            input_schema=self.schema,
            lazy_filters=self._lazy_filters
        )

        # Use flat_map with the expand operator's UDF
        # This integrates with Palimpzest's pipeline and optimization
        return self.flat_map(
            udf=expand_op.udf,
            cols=envelope_schema
        )

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
