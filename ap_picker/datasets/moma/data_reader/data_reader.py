from typing import Any, Dict, Iterator, Optional, Protocol, runtime_checkable


@runtime_checkable
class DataReader(Protocol):
    """Interface to access Data for various sources (e.g. relational databases, file datasets, etc.) in a streaming manner."""

    def read_stream(self, query: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Yields rows/records as dictionaries in a streaming manner.

        Args:
            query: Filter query depending on the data type

        Returns:
            Iterator[Dict[str, Any]]: Stream of data rows
        """
        ...

    def get_schema(self) -> Dict[str, Any]:
        """
        Returns schema information about the data source.

        Returns:
            Dict[str, Any]: Schema metadata (columns, types, etc.)
        """
        ...
