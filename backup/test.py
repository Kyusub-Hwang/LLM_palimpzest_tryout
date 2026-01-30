from litellm import completion

response = completion(
    model="ollama/qwen3",
    messages=[{"content": "respond in 20 words. who are you?", "role": "user"}],
    api_base="http://host.docker.internal:11434"
)
print(response)
