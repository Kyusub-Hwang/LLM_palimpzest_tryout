from re import sub


def _model_str_to_enum_name(model_str: str) -> str:
    """
    Converts a model name in liteLLM format to a valid enum name.
    """
    # Replace '/' with '_'
    name = model_str.replace("/", "_")
    # Remove dots or other invalid characters
    name = sub(r"[^A-Za-z0-9_]", "_", name)
    # Collapse multiple underscores
    name = sub(r"_+", "_", name)
    # Uppercase
    name = name.upper()
    return name
