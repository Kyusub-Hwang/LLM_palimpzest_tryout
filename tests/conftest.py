import logging
from pathlib import Path
from typing import TypedDict

import pytest
from dotenv import load_dotenv
from palimpzest import Model

from ap_picker.custom_models import CUSTOM_MODELS_CARDS
from ap_picker.datasets.moma.dataset import MomaDataset
from ap_picker.monkey_patching import add_model_support, use_custom_optimizer
from ap_picker.optimizer.ap_optimizer import ApOptimizer

load_dotenv()
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class TestModels(TypedDict):
    nomic: Model
    llama: Model
    qwen: Model


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def asset_path(project_root: Path) -> Path:
    return project_root / "assets"


@pytest.fixture(scope="session", autouse=True)
def models() -> TestModels:
    models: TestModels = {}  # type: ignore

    for model_name, model_card in CUSTOM_MODELS_CARDS.items():
        alias = add_model_support(model_name, model_card)
        match model_name:
            case "ollama/llama3.1":
                models["llama"] = Model[alias]  # type: ignore
            case "ollama/qwen3":
                models["qwen"] = Model[alias]  # type: ignore
            case _:
                logger.warning(
                    f"Model {model_name} was added with alias {alias}, but no matching case was found to add it to the models fixture."
                )

    models["nomic"] = Model.NOMIC_EMBED_TEXT
    return models


@pytest.fixture(scope="session", autouse=True)
def custom_optimizer():
    use_custom_optimizer(ApOptimizer)


@pytest.fixture(scope="session")
def sample_dataset(asset_path: Path) -> MomaDataset:
    return MomaDataset(path=str(asset_path / "moma_datasets" / "mixed_items.json"))
