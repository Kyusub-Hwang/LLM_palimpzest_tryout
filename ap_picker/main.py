from pathlib import Path

from dotenv import load_dotenv
from palimpzest import Model, QueryProcessorConfig, TextFileDataset

from ap_picker.custom_models import CUSTOM_MODELS_CARDS
from ap_picker.datasets.ap_dataset import LocalApDataset
from ap_picker.monkey_patching import add_model_support, use_custom_optimizer
from ap_picker.optimizer.ap_optimizer import ApOptimizer

# NOTE: Uncomment to see Palimpzest debug logs, including optimization steps
# logging.basicConfig(level=logging.DEBUG)

load_dotenv()


def main():

    # Load all custom models
    # To use them in Palimpzest, we need to use their alias
    added_models = {}
    for model_name, model_card in CUSTOM_MODELS_CARDS.items():
        alias = add_model_support(model_name, model_card)
        added_models[model_name] = alias
    llama = added_models["ollama/llama3.1"]

    use_custom_optimizer(ApOptimizer)

    output = (
        LocalApDataset(Path("assets/aps"))
        .sem_filter(
            "The Analytical Pattern to get data about cities in Switzerland",
            depends_on=["description"],
        ).run(
            max_quality=True,
            config=QueryProcessorConfig(available_models=[Model[llama]]),
            execution_strategy="parallel"
        )

    )
    print(output.to_df())


if __name__ == "__main__":
    main()


def email_sample(model: Model):

    emails = TextFileDataset(
        id="enron-emails", path="/workspaces/LLM_palimpzest_tryout/assets/emails")

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
    output = emails.run(max_quality=True, config=QueryProcessorConfig(
        available_models=[model]), execution_strategy="parallel")

    print(output.to_df(cols=["filename", "sender", "subject", "summary"]))
