from .helpers import _model_str_to_enum_name


class ModelEnumProxy(str):
    """
    Proxy class to mimic a Model enum member for a dynamically added model.
    The deinition is taken from palimpzest.constants.Model
    """

    def __init__(self, model_name: str):
        self.model_name = model_name

    def is_llama_model(self):
        return "llama" in self.model_name.lower()

    def is_clip_model(self):
        return False

    def is_together_model(self):
        return False

    def is_text_embedding_model(self):
        return "embed" in self.model_name.lower()

    def is_o_model(self):
        return False

    def is_gpt_5_model(self):
        return False

    def is_openai_model(self):
        return False

    def is_anthropic_model(self):
        return False

    def is_vertex_model(self):
        return False

    def is_google_ai_studio_model(self):
        return False

    def is_vllm_model(self):
        return False

    def is_reasoning_model(self):
        return False

    def is_text_model(self):
        return "embed" not in self.model_name.lower()

    def is_vision_model(self):
        return False

    def is_audio_model(self):
        return False

    def is_text_image_multimodal_model(self):
        return False

    def is_text_audio_multimodal_model(self):
        return False

    def is_embedding_model(self):
        # NOTE: Simplified assumption based on model name
        return "embed" in self.model_name.lower()

    @property
    def value(self):
        return str(self)

    def __repr__(self):
        return _model_str_to_enum_name(self.model_name)
