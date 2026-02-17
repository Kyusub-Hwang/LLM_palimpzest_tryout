from anyio import Path

from ap_picker.datasets.moma.dataset import MomaDataset
from ap_picker.datasets.moma.items import (
    MomaDatasetItemFile,
    MomaDatasetItemRelationalDb,
)


def test_dataset_creation(asset_path: Path):
    MomaDataset(path=str(asset_path / "moma_datasets" / "mixed_items.json"))


def test_dataset_item_file(asset_path: Path):
    ds = MomaDataset(path=str(asset_path / "moma_datasets" / "file_item.json"))
    assert len(ds) == 1
    item = ds.items[0]
    assert isinstance(item, MomaDatasetItemFile)
    names = item.metadata.get("names", [])
    assert len(names) > 0
    assert names[0] == "weather_data_fr.csv"


def test_dataset_item_relational_db(asset_path: Path):
    file_path = asset_path / "moma_datasets" / "relational_db_item.json"
    ds = MomaDataset(path=str(file_path))
    assert len(ds) == 1
    item = ds.items[0]
    assert isinstance(item, MomaDatasetItemRelationalDb)
    names = item.metadata.get("names", [])
    assert len(names) > 0
    assert names[0] == "mathe"
