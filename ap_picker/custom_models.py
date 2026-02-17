from typing import Dict

from ap_picker.monkey_patching import ModelCardLike

CUSTOM_MODELS_CARDS: Dict[str, ModelCardLike] = {
    "ollama/llama3.1": {
        "usd_per_input_token": 0.18 / 1e6,
        "usd_per_output_token": 0.18 / 1e6,
        "seconds_per_output_token": 0.0050,
        "overall": 44.25,
    },
    "ollama/qwen3": {
        "usd_per_input_token": 0.18 / 1e6,
        "usd_per_output_token": 0.18 / 1e6,
        "seconds_per_output_token": 0.1000,
        "overall": 66.25,
    },
    "ollama/nomic-embed-text": {
        ##### Cost in USD #####
        "usd_per_input_token": 0.02 / 1e6,
        "usd_per_output_token": None,
        ##### Time #####
        # NOTE: just copying GPT_4o_MINI_MODEL_CARD for now
        "seconds_per_output_token": 0.0098,
        ##### Agg. Benchmark #####
        "overall": 63.09,  # NOTE: just copying GPT_4o_MINI_MODEL_CARD for now
    }
}
