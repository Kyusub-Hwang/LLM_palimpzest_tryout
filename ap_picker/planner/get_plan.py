import json
from typing import Any, Dict, List

from litellm import completion
from pydantic import BaseModel, Field


class ParameterDefinition(BaseModel):
    """Defines a parameter for an operation."""
    description: str = Field(
        description="Description of what the parameter does")


class OperationDefinition(BaseModel):
    """Defines an available operation with its parameters."""
    operation: str = Field(description="The name of the operation")
    description: str = Field(
        description="Description of what the operation does")
    parameters: Dict[str, ParameterDefinition] = Field(
        description="Dictionary of parameter names to their definitions")


class Operation(BaseModel):
    """Represents a single operation with its parameters."""
    operation: str = Field(description="The name of the operation to execute")
    parameters: Dict[str, Any] = Field(
        description="Dictionary of parameter names to their values")


class OperationPlan(BaseModel):
    """Represents a sequence of operations to accomplish a task."""
    operations: List[Operation] = Field(
        description="List of operations to execute in sequence")


def get_plan():
    pass


def get_logical_operations(task: str, model: str, operations_list: List[OperationDefinition]):

    prompt = f"""You are a data processing pipeline planner. Given a task description and a list of available operations, your job is to:
    1. Select the appropriate operations needed to accomplish the task
    2. Determine the correct sequence of operations
    3. Fill in the parameter values for each operation

    Available Operations:
    {operations_list}

    Task: {task}

    Instructions:
    - Analyze the task and determine which operations are needed
    - Consider the logical order of operations (e.g., find data before filtering it)
    - For each selected operation, provide specific parameter values based on the task description
    - You must return a structured response with a list of operations"""

    response = completion(
        model=model,
        messages=[
            {"content": prompt, "role": "user"}
        ],
        api_base="http://host.docker.internal:11434",
        response_format=OperationPlan
    )

    # Parse the structured response
    result = OperationPlan.model_validate_json(
        response.choices[0].message.content)
    return result.operations


def join_operations(operations: list, model: str):
    pass
