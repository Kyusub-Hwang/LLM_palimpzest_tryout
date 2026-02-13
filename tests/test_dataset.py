from typing import Dict

import pytest
from anyio import Path
from palimpzest import Model, QueryProcessorConfig, TextFileDataset, Validator

from ap_picker.datasets.moma_dataset import MomaDataset


def test_dataset_creation(asset_path: Path):
    MomaDataset(path=str(asset_path / "moma_datasets/sample_api_response.json"))


def test_basic_filtering(asset_path: Path, additional_models: Dict[str, str]):
    moma_ds = MomaDataset(
        path=str(asset_path / "moma_datasets/sample_api_response.json")
    )
    moma_ds.sem_filter("About mathe", depends_on=["description"])
    ollama = Model[additional_models["ollama/llama3.1"]]
    output = moma_ds.run(
        max_quality=True,
        config=QueryProcessorConfig(
            available_models=[ollama, Model.NOMIC_EMBED_TEXT]
        )
    )
    print(output.to_df())
