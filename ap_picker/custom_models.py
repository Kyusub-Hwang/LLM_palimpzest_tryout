from enum import Enum
from typing import Dict

from monkey_patching import ModelCardLike


class CustomModels(str, Enum):
    OLLAMA_LLAMA3_1 = "ollama/llama3.1"


CUSTOM_MODELS_CARDS: Dict[str, ModelCardLike] = {
    "ollama/llama3.1": {
        "usd_per_input_token": 0.18 / 1e6,
        "usd_per_output_token": 0.18 / 1e6,
        "seconds_per_output_token": 0.0050,
        "overall": 44.25,
    },
}
