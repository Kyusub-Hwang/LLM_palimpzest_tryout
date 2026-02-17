import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from palimpzest import Model, QueryProcessorConfig, TextFileDataset, Validator

from ap_picker.custom_models import CUSTOM_MODELS_CARDS
from ap_picker.monkey_patching import add_model_support, use_custom_optimizer
from ap_picker.optimizer.ap_optimizer import ApOptimizer

# NOTE: Uncomment to see Palimpzest debug logs, including optimization steps
logging.basicConfig(level=logging.DEBUG)

load_dotenv()


def email_sample(model: Model):

    emails = TextFileDataset(
        id="enron-emails", path="/workspaces/test/assets/emails")

    # filter for emails matching natural language criteria
    emails = emails.sem_filter(
        'The email refers to one of the following business transactions: "Robotrader")',
    )
    # emails = emails.sem_filter(
    #     "The email contains a first-hand discussion of the business transaction",
    # )

    # extract structured fields for each email
    emails = emails.sem_map([
        {"name": "subject", "type": str, "desc": "the subject of the email"},
        {"name": "sender", "type": str, "desc": "the email address of the sender"},
        {"name": "summary", "type": str, "desc": "a brief summary of the email"},
    ])
    valid = Validator(model=model)
    # execute the program and print the output
    # output = emails.optimize_and_run(max_quality=True, config=QueryProcessorConfig(
    #     available_models=[model]), execution_strategy="parallel", validator=valid, output_schema={"filename": str, "sender": str, "subject": str, "summary": str})

    output = emails.run(max_quality=True, config=QueryProcessorConfig(
        # available_models=[model, Model.NOMIC_EMBED_TEXT]), execution_strategy="parallel", output_schema={"filename": str, "sender": str, "subject": str, "summary": str})
        available_models=[model, Model.NOMIC_EMBED_TEXT]))

    print(output.to_df(cols=["filename", "sender", "subject", "summary"]))


def main():

    # Load all custom models
    # To use them in Palimpzest, we need to use their alias
    added_models = {}
    for model_name, model_card in CUSTOM_MODELS_CARDS.items():
        alias = add_model_support(model_name, model_card)
        added_models[model_name] = alias

    llama = Model[added_models["ollama/llama3.1"]]
    qwen = Model[added_models["ollama/qwen3"]]
    nomic_embedding = Model[added_models["ollama/nomic-embed-text"]]

    use_custom_optimizer(ApOptimizer)

    valid = Validator(model=qwen)

    # output = (
    #     LocalApDataset(Path("assets/aps"))
    #     .sem_filter("The AP is related to switzeland cities")
    #     .optimize_and_run(
    #         config=QueryProcessorConfig(
    #             available_models=[llama, nomic_embedding],
    #             optimizer_strategy="Greedy",
    #         ),
    #         validator=valid,
    #     )
    # )
    # print(output.to_df())

    email_sample(model=llama)


if __name__ == "__main__":
    main()
