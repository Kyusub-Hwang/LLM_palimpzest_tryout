import pytest
from litellm import completion

from ap_picker.planner.get_plan import (
    Operation,
    OperationDefinition,
    ParameterDefinition,
    get_logical_operations,
)


def test_int_ollama_deployment():
    response = completion(
        model="ollama/llama3.1",
        messages=[
            {"content": "respond in 20 words. who are you?", "role": "user"}],
        api_base="http://host.docker.internal:11434"
    )
    print(response)


def test_get_logical_operations_basic():
    """Test get_logical_operations with a simple task."""
    operations_list = [
        OperationDefinition(
            operation="find_dataset",
            description="Search for datasets relevant to the task.",
            parameters={
                "query": ParameterDefinition(
                    description="The search query to find relevant datasets."
                )
            },
        ),
        OperationDefinition(
            operation="filter",
            description="Semantically filter the datasets to retain only relevant information.",
            parameters={
                "criteria": ParameterDefinition(
                    description="The criteria to semantically filter the datasets."
                )
            },
        ),
    ]

    task = "Find temperature datasets for Swiss cities and filter for data from the last week"
    model = "ollama/llama3.1"

    result = get_logical_operations(task, model, operations_list)

    # Validate results
    assert isinstance(result, list), "Result should be a list"
    assert len(result) > 0, "Result should contain at least one operation"

    # Check that all results are Operation objects
    for op in result:
        assert isinstance(
            op, Operation), f"Each item should be an Operation, got {type(op)}"
        assert hasattr(
            op, 'operation'), "Operation should have 'operation' attribute"
        assert hasattr(
            op, 'parameters'), "Operation should have 'parameters' attribute"
        assert isinstance(
            op.parameters, dict), "Parameters should be a dictionary"


def test_get_logical_operations_with_all_operations():
    """Test get_logical_operations with all three operation types."""
    operations_list = [
        OperationDefinition(
            operation="find_dataset",
            description="Search for datasets relevant to the task.",
            parameters={
                "query": ParameterDefinition(
                    description="The search query to find relevant datasets."
                )
            },
        ),
        OperationDefinition(
            operation="filter",
            description="Semantically filter the datasets to retain only relevant information.",
            parameters={
                "criteria": ParameterDefinition(
                    description="The criteria to semantically filter the datasets."
                )
            },
        ),
        OperationDefinition(
            operation="map",
            description="Semantically maps the data schema to another schema.",
            parameters={
                "new_schema": ParameterDefinition(
                    description="The new schema to map the data to."
                )
            },
        ),
    ]

    task = "Find email datasets, filter for urgent messages, and map to a simplified schema with sender and subject"
    model = "ollama/llama3.1"

    result = get_logical_operations(task, model, operations_list)

    # Validate results
    assert isinstance(result, list), "Result should be a list"
    assert len(result) > 0, "Result should contain operations"

    # Check operation names are valid
    valid_operations = {"find_dataset", "filter", "map"}
    for op in result:
        assert op.operation in valid_operations, f"Operation '{op.operation}' not in valid operations"


def test_get_logical_operations_custom_operations():
    """Test get_logical_operations with custom operation definitions."""
    operations_list = [
        OperationDefinition(
            operation="aggregate",
            description="Aggregate data by grouping and computing statistics.",
            parameters={
                "group_by": ParameterDefinition(
                    description="Field to group data by."
                ),
                "aggregation": ParameterDefinition(
                    description="Type of aggregation (sum, avg, count, etc.)."
                )
            },
        ),
        OperationDefinition(
            operation="sort",
            description="Sort data by a specified field.",
            parameters={
                "field": ParameterDefinition(
                    description="Field to sort by."
                ),
                "order": ParameterDefinition(
                    description="Sort order (asc or desc)."
                )
            },
        ),
    ]

    task = "Sort temperature data by date in descending order"
    model = "ollama/llama3.1"

    result = get_logical_operations(task, model, operations_list)

    # Validate results
    assert isinstance(result, list), "Result should be a list"
    for op in result:
        assert isinstance(op, Operation), "Each item should be an Operation"
        assert op.operation in [
            "aggregate", "sort"], f"Operation should be from custom list, got {op.operation}"
