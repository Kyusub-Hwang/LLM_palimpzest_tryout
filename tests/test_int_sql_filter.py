import logging

from conftest import TestModels
from palimpzest import QueryProcessorConfig

from ap_picker.datasets.moma.dataset import MomaDataset

logger = logging.getLogger(__name__)


DATASETS_ID_ABOUT_MATH = {
    "1928a6b4-9e92-40f9-b8e3-d28553694d6d", "dfe9310a-87e3-4e42-bcf4-b65bf0c80406", "bfca980d-e6a4-4ea5-a7d5-eb8b08fb02bb"
}


def test_int_find(math_db, sample_dataset: MomaDataset, models: TestModels):
    """Test dataset-level filtering with SQL on relational databases using find()."""
    avail_models = [models["llama"], models["nomic"]]
    output = (
        sample_dataset
        .find("Dataset about math")
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )
    logger.info(f"Output: {output.to_df()}")
    assert output is not None
    assert len(output) == len(DATASETS_ID_ABOUT_MATH)
    assert set(output.to_df()["id"].unique()) == DATASETS_ID_ABOUT_MATH


def test_int_search_data(math_db, sample_dataset: MomaDataset, models: TestModels):
    """Test dataset-level filtering with SQL on relational databases using find()."""
    avail_models = [models["gemma3_27b"], models["nomic"]]
    output = (
        sample_dataset
        .query_data("Find 5 questions with level 'Basic' and topic 'Integration'")
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )
    logger.info(f"Output: {output.to_df(['content', 'record_data'])}")
    assert output is not None
    assert len(output) > 0


def test_int_search_data_map(math_db, sample_dataset: MomaDataset, models: TestModels):
    """Test dataset-level filtering with SQL on relational databases using find()."""
    avail_models = [models["llama"], models["nomic"]]
    output = (
        sample_dataset
        .query_data("Find 5 questions with level basic and topic algebra")
        .sem_map(
            cols=[
                {"name": "girls name", "type": str,
                    "description": "Girls name in data"},
                {"name": "boys name", "type": str,
                    "description": "Boys name in data"},
            ],
            desc="Map to envelope schema with dataset info and record data"
        )
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )
    logger.info(f"Output: {output.to_df()}")
    assert output is not None
    assert len(output) > 0


def test_int_find_search_data(math_db, sample_dataset: MomaDataset, models: TestModels):
    """Test dataset-level filtering with SQL on relational databases using find()."""
    avail_models = [models["gemma3_27b"], models["nomic"]]
    output = (
        sample_dataset
        .find("Dataset about math")
        .query_data("Find 5 questions with level 'Basic' and topic 'Integration'")
        .sem_map([
            {"name": "question_id", "type": str, "description": "Id of the question"},  # noqa
            {"name": "topic", "type": str, "description": "Which field of mathematics the question is about"},  # noqa
            {"name": "level", "type": str, "description": "Can a high schooler solve this question?"},  # noqa
        ])
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )
    logger.info(f"Output: {output.to_df(['question_id', 'topic', 'level'])}")
    assert output is not None

    assert set(output.to_df()["id"].unique()) == DATASETS_ID_ABOUT_MATH
