import os

import palimpzest as pz
from palimpzest import Model

os.environ["OLLAMA_API_BASE"] = "http://host.docker.internal:11434"


# class Model2():
#     qwen3 = "OLLAMA_LAMA3_1_8B"


# load the emails into a dataset
emails = pz.TextFileDataset(id="enron-emails", path="emails/")

# filter for emails matching natural language criteria
emails = emails.sem_filter(
    'The email refers to one of the following business transactions: "Raptor", "Deathstar", "Chewco", and/or "Fat Boy")',
)
emails = emails.sem_filter(
    "The email contains a first-hand discussion of the business transaction",
)

# extract structured fields for each email
emails = emails.sem_map([
    {"name": "subject", "type": str, "desc": "the subject of the email"},
    {"name": "sender", "type": str, "desc": "the email address of the sender"},
    {"name": "summary", "type": str, "desc": "a brief summary of the email"},
])

# execute the program and print the output
output = emails.run(max_quality=True, config=pz.QueryProcessorConfig(
    available_models=[Model.OLLAMA_GPT_5_MINI_LOCAL]), execution_strategy="parallel")

print(output.to_df(cols=["filename", "sender", "subject", "summary"]))
