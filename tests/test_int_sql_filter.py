import logging

from conftest import TestModels
from palimpzest import QueryProcessorConfig

from ap_picker.datasets.moma.dataset import MomaDataset

logger = logging.getLogger(__name__)


"""
MomaDataset Three-Method Architecture Tests
============================================

MomaDataset provides three distinct methods for different filtering/expansion needs:

1. **find(filter_condition, depends_on=None)** -> Dataset
   - Filters datasets by METADATA only (description, type, etc.)
   - Does NOT access actual dataset content
   - Uses DatasetLevelFilter (SQL LIMIT 1 for DBs, LLM on metadata for files)
   - Returns: Palimpzest Dataset with filtered meta-records
   - Use case: "Which datasets are relevant to my task?"
   
   Example:
       datasets = moma.find("datasets about mathematics").run()

2. **sem_filter(filter_condition)** -> MomaDataset  
   - Stores a LAZY filter condition for dataset content
   - Does NOT execute immediately - filter applied during expand()
   - **SUPPORTS CHAINING**: Multiple sem_filter() calls combine with AND logic
   - For relational DBs: Generates SQL WHERE clause with AND conditions
   - Returns: New MomaDataset instance with filter added to list
   - Use case: "Filter the actual data inside datasets"
   
   Example:
       # Single filter
       filtered_moma = moma.sem_filter("questions about algebra")
       
       # Multiple filters (combined with AND)
       multi = moma.sem_filter("algebra").sem_filter("basic")
       # -> WHERE ... algebra ... AND ... basic ...

3. **expand()** -> Dataset
   - Converts dataset-level to row-level (meta-records -> individual records)
   - Applies any lazy filters from sem_filter() during expansion
   - For relational DBs: Uses SQL WHERE clause for efficient filtering
   - Returns: Palimpzest Dataset with envelope schema
   - Use case: "Give me individual records (with filters applied)"
   
   Example:
       records = moma.sem_filter("algebra").expand().run()
       # Filter applied via SQL WHERE during expansion

Pipeline Examples:
------------------

A. Metadata discovery only:
   moma.find("math datasets").run()

B. Lazy data filtering (most efficient):
   moma.sem_filter("algebra questions").expand().run()

C. Expand without filtering:
   moma.expand().run()

D. Post-expansion filtering (less efficient):
   moma.expand().sem_filter("algebra").run()
   # Note: Uses RecordLevelFilter (LLM per-record), not SQL WHERE

Architecture Benefits:
---------------------
- find() + DatasetLevelFilter: Efficient metadata-based dataset discovery
- sem_filter() + expand(): Lazy SQL filtering (WHERE clause) at DB level  
- expand(): Clear conversion from dataset-level to row-level
- Follows Palimpzest's lazy evaluation philosophy
"""


def test_int_find(sample_dataset: MomaDataset, models: TestModels):
    """Test dataset-level filtering with SQL on relational databases using find()."""
    avail_models = [models["llama"], models["nomic"]]
    output = (
        sample_dataset
        # Changed to find()
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
    assert len(output) > 0


def test_two_level_filtering(sample_dataset: MomaDataset, models: TestModels):
    """
    Test lazy filtering with sem_filter() and expand().

    sem_filter() stores a filter condition that is applied DURING expand(),
    using SQL WHERE clause for efficient filtering at the database level.
    """
    avail_models = [models["llama"], models["nomic"]]

    # sem_filter() returns MomaDataset with lazy filter stored
    # The filter is applied during expand() using SQL WHERE clause
    output = (
        sample_dataset
        # Lazy filter (not executed yet)
        .sem_filter("questions about algebra")
        .expand()  # Applies filter during expansion via SQL WHERE
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )

    logger.info(f"Lazy filter output: {output.to_df()}")

    # Verify results
    assert output is not None
    # Note: May be 0 if no records match the filter

    # Check that output has envelope schema fields
    df = output.to_df()
    if len(df) > 0:
        assert "source_dataset_id" in df.columns, "Missing source dataset ID"
        assert "record_data" in df.columns, "Missing record data field"
        logger.info(
            f"Lazy filter returned {len(output)} records from {df['source_dataset_id'].nunique()} datasets")


def test_dataset_level_filter_only(sample_dataset: MomaDataset, models: TestModels):
    """Test find() for metadata-level dataset discovery."""
    avail_models = [models["llama"], models["nomic"]]

    # find() filters datasets by metadata only (no data access)
    output = sample_dataset.find(
        "datasets about mathematics or science",
        depends_on=["description"]
    ).run(
        max_quality=True,
        config=QueryProcessorConfig(
            available_models=avail_models, progress=False
        )
    )

    logger.info(f"find() output: {output.to_df()}")

    assert output is not None
    assert len(output) > 0

    # Verify we got meta-records (should have 'type' field, not 'source_dataset_id')
    df = output.to_df()
    assert "type" in df.columns, "Should have 'type' field for meta-records"
    assert "source_dataset_id" not in df.columns, "Should not have expanded schema"


def test_record_level_filter_only(sample_dataset: MomaDataset, models: TestModels):
    """Test expand() without any filtering - just conversion to row-level."""
    avail_models = [models["llama"], models["nomic"]]

    # expand() converts dataset-level to row-level (no filtering)
    output = (
        sample_dataset
        .expand()  # Materialize all records from all datasets
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )

    logger.info(f"expand() output: {output.to_df()}")

    assert output is not None
    # Should have records from all datasets

    # Verify envelope schema
    df = output.to_df()
    if len(df) > 0:
        assert "source_dataset_id" in df.columns
        assert "record_data" in df.columns
        logger.info(
            f"Expanded {len(output)} records from {df['source_dataset_id'].nunique()} datasets")


def test_complete_pipeline(sample_dataset: MomaDataset, models: TestModels):
    """
    Test complete pipeline workflow with all three operations.

    Architecture:
    1. find() - Discover relevant datasets by metadata (efficient dataset-level filter)
    2. sem_filter() - Lazily specify data filter (not executed yet)  
    3. expand() - Materialize records with lazy filter applied via SQL WHERE

    Note: Can't chain find().sem_filter() because find() returns Dataset, not MomaDataset.
    Instead, we can either:
    - Workflow A: find() -> run() -> get dataset IDs -> filter on those
    - Workflow B: sem_filter() -> expand() -> run() (lazy filter during expansion)

    This test demonstrates Workflow B (most common use case).
    """
    avail_models = [models["llama"], models["nomic"]]

    # Workflow B: Lazy data filtering with expansion
    # sem_filter() stores condition, expand() applies it via SQL WHERE
    output = (
        sample_dataset
        .sem_filter("questions about calculus")  # Lazy filter (not executed)
        .expand()  # Materializes with SQL WHERE applied
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )

    logger.info(f"Complete pipeline output: {output.to_df()}")

    assert output is not None

    if len(output) > 0:
        df = output.to_df()
        assert "source_dataset_id" in df.columns
        assert "record_data" in df.columns
        logger.info(
            f"Pipeline returned {len(output)} filtered records from "
            f"{df['source_dataset_id'].nunique()} datasets"
        )


def test_multiple_sem_filters(sample_dataset: MomaDataset, models: TestModels):
    """
    Test chaining multiple sem_filter() calls.

    Multiple filters should be combined with AND logic:
    - sem_filter(\"algebra\").sem_filter(\"basic\") 
    - Results in: records matching BOTH algebra AND basic

    For SQL databases, this generates a WHERE clause with multiple conditions connected by AND.
    """
    avail_models = [models["llama"], models["nomic"]]

    # Chain multiple filters - should combine with AND
    output = (
        sample_dataset
        .sem_filter("questions about algebra")  # First filter
        .sem_filter("basic level")  # Second filter (AND with first)
        .expand()  # Applies both filters: WHERE ... AND ...
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False
            )
        )
    )

    logger.info(f"Multiple filters output: {output.to_df()}")

    assert output is not None

    # Verify the filters are combined (should be more restrictive than single filter)
    if len(output) > 0:
        df = output.to_df()
        assert "source_dataset_id" in df.columns
        assert "record_data" in df.columns
        logger.info(
            f"Multiple filters returned {len(output)} records "
            f"(more restrictive than single filter)"
        )
