from typing import Any, Dict, Iterator


class FileReader:
    """Reader for streaming data from file sources (CSV, JSON, etc.)."""

    def __init__(self, file_path: str, file_format: str = "csv"):
        self.file_path = file_path
        self.file_format = file_format.lower()

    def read_stream(self) -> Iterator[Dict[str, Any]]:
        """Stream rows from file."""
        if self.file_format == "csv":
            yield from self._read_csv_stream()
        elif self.file_format == "json":
            yield from self._read_json_stream()
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")

    def _read_csv_stream(self) -> Iterator[Dict[str, Any]]:
        """Stream CSV file row by row."""
        import csv

        with open(self.file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row

    def _read_json_stream(self) -> Iterator[Dict[str, Any]]:
        """Stream JSON file (assumes newline-delimited JSON or JSON array)."""
        import json

        with open(self.file_path, 'r', encoding='utf-8') as f:
            # Try to load as JSON array first
            try:
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        yield item
                else:
                    yield data
            except json.JSONDecodeError:
                # Try newline-delimited JSON
                f.seek(0)
                for line in f:
                    if line.strip():
                        yield json.loads(line)

    def get_schema(self) -> Dict[str, Any]:
        """Infer schema from file."""
        # Read first row to infer schema
        first_row = next(self.read_stream(), None)

        if first_row is None:
            return {"columns": []}

        columns = [
            {"name": key, "type": type(value).__name__}
            for key, value in first_row.items()
        ]

        return {
            "file_path": self.file_path,
            "format": self.file_format,
            "columns": columns
        }
