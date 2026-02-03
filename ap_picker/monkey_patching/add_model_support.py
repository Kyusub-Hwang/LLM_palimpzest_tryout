from typing import TypedDict

import palimpzest.constants as pz_constants
from palimpzest import Model

from .internal.helpers import _model_str_to_enum_name
from .internal.model_enum_proxy import ModelEnumProxy


class ModelCardLike(TypedDict):
    """
    A Model card is a collection of statistics about a model.
    On Palimpzest, they are used for cost and speed estimation.
    """
    # Cost per million input token (I guess ?)
    usd_per_input_token: float
    # Cost per million output token (I guess ?)
    usd_per_output_token: float
    # Time taken to generate one output token
    seconds_per_output_token: float
    # Overall intelligence score (higher is better)
    overall: float


def add_model_support(model_name: str, model_card: ModelCardLike) -> str:
    """
    Adds support for a new liteLLM-compatible model in palimpzest at runtime.

    BEWARE : This is a monkey-patch solution, and should not be used outside of testing

    Args:
        model_name (str): The name of the model in liteLLM format (e.g., "ollama/llama3.1").
        model_card (dict): A dictionary containing the model card information.
    Returns:
        str: The enum name of the newly added model (e.g., "OLLAMA_LLAMA3_1").
    """

    enum_name = _model_str_to_enum_name(model_name)

    # In Python, enum are frozen, it's not possible to edit them at runtime
    # But a string enum is a fancy way of saying "a string with custom methods"
    # So we can create a proxy class that behaves like a string but has the necessary methods
    extra = ModelEnumProxy(model_name)

    # And force add it to the Model enum
    setattr(Model, enum_name, extra)

    # Add to the internal enum maps
    # The typing is ignored because we are modifying private members
    # fmt:off
    Model._member_map_[enum_name] = extra # pyright: ignore[reportArgumentType] 
    Model._value2member_map_[model_name] = extra # pyright: ignore[reportArgumentType]
    # fmt: on

    # Finally, add the model card to the constants
    # This is just a dictionary, so we can edit it at runtime
    pz_constants.MODEL_CARDS[model_name] = model_card

    return enum_name
