from typing import Any, Dict, Iterator, Protocol, runtime_checkable


@runtime_checkable
class DataReader(Protocol):
    """Protocol for streaming data access from various sources."""

    def read_stream(self) -> Iterator[Dict[str, Any]]:
        """
        Yields rows/records as dictionaries in a streaming manner.

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




