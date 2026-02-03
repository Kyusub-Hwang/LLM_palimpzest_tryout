import pytest
from litellm import completion


def test_int_ollama_deployment():
    response = completion(
        model="ollama/llama3.1",
        messages=[
            {"content": "respond in 20 words. who are you?", "role": "user"}],
        api_base="http://host.docker.internal:11434"
    )
    print(response)
