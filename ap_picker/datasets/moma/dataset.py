from json import load
from typing import Any, Dict, List, Optional

from palimpzest import Dataset, IterDataset

from .items import (
    MomaDatasetItem,
    MomaDatasetItemFile,
    MomaDatasetItemRelationalDb,
    MomaDatasetItemType,
)


moma_dataset_schema = [
    {"name": "id",          "type": str,             "description": "Dataset uuidv4"},
    {"name": "description", "type": str,             "description": "A description of the dataset"},
    {"name": "type",        "type": str,             "description": "The type of the dataset, e.g. relational_db, file_dataset, etc."},
    {"name": "content",     "type": Dict[str, Any], "description": "Content of the dataset, which may include tables, charts, text, etc."},
    {"name": "metadata",    "type": Dict[str, Any], "description": "Metadata of the dataset, which may include information about the source, size, format, etc."},
]

_QUERY_ENVELOPE_SCHEMA = [
    {"name": "source_dataset_id",   "type": str,             "description": "ID of the source dataset"},
    {"name": "source_dataset_desc", "type": str,             "description": "Description of the source dataset"},
    {"name": "source_dataset_type", "type": str,             "description": "Type of the source dataset"},
    {"name": "record_data",         "type": Dict[str, Any], "description": "The actual record data"},
]


class MomaDataset(IterDataset):
    """
    Operates in two modes:

    * **Source mode** (``path`` or ``items`` provided): loads MomaDatasetItems and
      behaves as a normal ``IterDataset``.
    * **View mode** (``_wrapped_dataset`` provided): wraps a ``Dataset`` returned by
      a Palimpzest operation (e.g. ``sem_filter``), delegating all attribute access to
      it while still exposing MomaDataset-specific methods such as ``query_data()``.

    Use ``MomaDataset.as_view(dataset)`` to create a view explicitly.
    """

    items: List[MomaDatasetItem]
    # Filter conditions to be applied during expand() (combined with AND)
    _lazy_filters: List[str]

    def __init__(
        self,
        *,
        path: Optional[str] = None,
        items: Optional[List[MomaDatasetItem]] = None,
        lazy_filters: Optional[List[str]] = None,
        _wrapped_dataset: Optional[Dataset] = None,
    ):
        """
        Initialize MomaDataset.

        Args:
            path: Path to dataset JSON file (source mode).
            items: Pre-loaded dataset items (source mode).
            lazy_filters: Filter conditions applied during expand() (AND-combined).
            _wrapped_dataset: Existing Dataset to wrap (view mode). Use
                ``MomaDataset.as_view()`` instead of passing this directly.
        """
        if _wrapped_dataset is not None:
            # View mode — store the wrapped dataset without calling IterDataset.__init__
            object.__setattr__(self, "_wrapped_dataset", _wrapped_dataset)
            return

        # Source mode
        super().__init__(id="moma", schema=moma_dataset_schema)

        if path is not None:
            self.items = MomaDataset._from_file(path)
        elif items is not None:
            self.items = items
        else:
            raise ValueError("Either 'path', 'items', or '_wrapped_dataset' must be provided")

        self._lazy_filters = lazy_filters if lazy_filters is not None else []

    # ------------------------------------------------------------------
    # View-mode helpers
    # ------------------------------------------------------------------

    @classmethod
    def as_view(cls, dataset: Dataset) -> "MomaDataset":
        """Wrap an existing Dataset so it gains MomaDataset query methods."""
        return cls(_wrapped_dataset=dataset)

    def _is_view(self) -> bool:
        try:
            object.__getattribute__(self, "_wrapped_dataset")
            return True
        except AttributeError:
            return False

    def __getattr__(self, name: str):
        # Delegate to the wrapped dataset in view mode.
        try:
            wrapped: Dataset = object.__getattribute__(self, "_wrapped_dataset")
            return getattr(wrapped, name)
        except AttributeError:
            raise AttributeError(name)

    def __setattr__(self, name: str, value):
        if self._is_view():
            wrapped: Dataset = object.__getattribute__(self, "_wrapped_dataset")
            setattr(wrapped, name, value)
        else:
            super().__setattr__(name, value)

    def __repr__(self) -> str:
        if self._is_view():
            wrapped: Dataset = object.__getattribute__(self, "_wrapped_dataset")
            return f"MomaDataset(view={wrapped!r})"
        return f"MomaDataset(items={len(self.items)})"

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

    def find(self, filter_condition: str) -> "MomaDataset":
        """
        Find datasets matching the filter condition, without querying data from them.

        Args:
            filter_condition: A natural language description of the filter condition,
                e.g. "is about math"

        Returns:
            A ``MomaDataset`` in view mode wrapping the filtered ``Dataset``.
        """
        filtered = self.sem_filter(
            filter_condition,
            depends_on=["description"]
        )
        return MomaDataset.as_view(filtered)

    def query_data(self, query: str) -> Dataset:
        return self.sem_map(
            cols=_QUERY_ENVELOPE_SCHEMA,
            desc=query,
            depends_on=["content"]
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

            # Choose concrete dataset item type
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
