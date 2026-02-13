from pathlib import Path
from typing import Dict

import pytest

from ap_picker.custom_models import CUSTOM_MODELS_CARDS
from ap_picker.monkey_patching import add_model_support, use_custom_optimizer


@pytest.fixture
def project_root() -> Path:
    return Path(__file__).parent.parent


@pytest.fixture
def asset_path(project_root: Path) -> Path:
    return project_root / "assets"


@pytest.fixture(scope="session")
def additional_models() -> Dict[str, str]:
    added_models = {}
    for model_name, model_card in CUSTOM_MODELS_CARDS.items():
        alias = add_model_support(model_name, model_card)
        added_models[model_name] = alias

    return added_models
