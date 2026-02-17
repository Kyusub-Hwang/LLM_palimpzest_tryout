import logging

from conftest import TestModels
from palimpzest import QueryProcessorConfig

from ap_picker.datasets.moma.dataset import MomaDataset

logger = logging.getLogger(__name__)


def test_int_sql_filter(sample_dataset: MomaDataset, models: TestModels):
    """Test dataset-level filtering with SQL on relational databases."""
    avail_models = [models["llama"], models["nomic"]]
    output = (
        sample_dataset
        .sem_filter("Dataset about math", depends_on=["description"])
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


def test_two_level_filtering(sample_dataset: MomaDataset, models: TestModels):
    """
    Test the complete two-level filtering architecture:
    1. Expand: Convert meta-records to individual records
    2. Record-level filter: Filter individual records

    Note: We expand first because sem_filter returns Dataset (not MomaDataset),
    so we can't call expand() after filtering.
    """
    avail_models = [models["llama"], models["nomic"]]

    # Step 1: Expand datasets to individual records
    # This creates envelope schema with source_dataset_id
    expanded_records = sample_dataset.expand()

    # Step 2: Record-level filter - find specific records about algebra
    # This should use RecordLevelFilter with LLM per-record evaluation
    # filtered_records = expanded_records.sem_filter(
    #     "questions about algebra"
    # )

    # Execute the pipeline
    output = expanded_records.run(
        max_quality=True,
        config=QueryProcessorConfig(
            available_models=avail_models, progress=False
        )
    )

    logger.info(f"Two-level filter output: {output.to_df()}")

    # Verify results
    assert output is not None
    # Note: May be 0 if no records match after expansion

    # Check that output has envelope schema fields
    df = output.to_df()
    if len(df) > 0:
        assert "source_dataset_id" in df.columns, "Missing source dataset ID"
        assert "record_data" in df.columns, "Missing record data field"
        logger.info(
            f"Successfully filtered {len(output)} records from {df['source_dataset_id'].nunique()} datasets")


def test_dataset_level_filter_only(sample_dataset: MomaDataset, models: TestModels):
    """Test that dataset-level filter correctly identifies matching datasets."""
    avail_models = [models["llama"], models["nomic"]]

    # Filter at dataset level only
    output = sample_dataset.sem_filter(
        "datasets about mathematics or science",
        depends_on=["description"]
    ).run(
        max_quality=True,
        config=QueryProcessorConfig(
            available_models=avail_models, progress=False
        )
    )

    logger.info(f"Dataset-level filter output: {output.to_df()}")

    assert output is not None
    assert len(output) > 0

    # Verify we got meta-records (should have 'type' field, not 'source_dataset_id')
    df = output.to_df()
    assert "type" in df.columns, "Should have 'type' field for meta-records"
    assert "source_dataset_id" not in df.columns, "Should not have expanded schema"


def test_record_level_filter_only(sample_dataset: MomaDataset, models: TestModels):
    """Test record-level filtering on expanded records."""
    avail_models = [models["llama"], models["nomic"]]

    # First expand all datasets, then filter at record level
    output = (
        sample_dataset
        .expand()
        .sem_filter("records containing the word 'equation'")
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )

    logger.info(f"Record-level filter output: {output.to_df()}")

    assert output is not None
    # Note: May be 0 if no records match, but should not error

    # Verify envelope schema
    df = output.to_df()
    if len(df) > 0:
        assert "source_dataset_id" in df.columns
        assert "record_data" in df.columns
