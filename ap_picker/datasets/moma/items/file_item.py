from typing import Any, Dict, List

from pydantic import PrivateAttr

from ap_picker.datasets.moma.data_reader import DataReader, FileReader

from .item import MomaDatasetItem, MomaDatasetItemType


class MomaDatasetItemFile(MomaDatasetItem):
    _raw_nodes: list = PrivateAttr()

    def __init__(self, *, id: str, description: str, properties: Dict[str, Any], raw_nodes: list):
        super().__init__(
            id=id,
            description=description,
            type=MomaDatasetItemType.FILE_DATASET,
            properties=properties
        )
        self._raw_nodes = raw_nodes

    def get_reader(self, file_index: int = 0) -> DataReader:
        """
        Get a reader for streaming data from a file in this dataset.

        Args:
            file_index: Index of the file to read (default: 0)

        Returns:
            DataReader: Reader instance for streaming access
        """
        # Extract file objects from raw nodes
        file_objects = []
        for node in self._raw_nodes:
            labels = node.get("labels", [])
            if "cr:FileObject" in labels:
                file_objects.append(node)

        if not file_objects:
            raise ValueError("No file objects found in dataset")

        if file_index >= len(file_objects):
            raise IndexError(f"File index {file_index} out of range")

        file_obj = file_objects[file_index]
        props = file_obj.get("properties", {})

        # Get file path and format
        content_url = props.get("contentUrl", "")
        encoding_format = props.get("encodingFormat", "")

        # Determine file format from encoding
        file_format = "csv"  # default
        if "csv" in encoding_format.lower():
            file_format = "csv"
        elif "json" in encoding_format.lower():
            file_format = "json"

        return FileReader(content_url, file_format)

    def get_all_readers(self) -> List[DataReader]:
        """Get readers for all files in this dataset."""
        file_count = len([
            n for n in self._raw_nodes
            if "cr:FileObject" in n.get("labels", [])
        ])
        return [self.get_reader(i) for i in range(file_count)]

    @property
    def content(self) -> Dict[str, Any]:
        # TODO: IMPLEMENT
        return {
            "type": "file_dataset",
            "nodes": self._raw_nodes,
        }

    @property
    def metadata(self) -> Dict[str, Any]:
        # Extract file names from nodes with 'cr:FileObject' label
        file_names = []
        for node in self._raw_nodes:
            labels = node.get("labels", [])
            if "cr:FileObject" in labels:
                props = node.get("properties", {})
                file_name = props.get("name", "")
                if file_name:
                    file_names.append(file_name)

        return {
            "names": file_names
        }
