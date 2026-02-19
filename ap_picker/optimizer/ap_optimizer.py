import json
from copy import deepcopy

from palimpzest.core.data.dataset import Dataset
from palimpzest.core.lib.schemas import get_schema_field_names
from palimpzest.query.operators.logical import (
    ComputeOperator,
    ConvertScan,
    Distinct,
    FilteredScan,
    JoinOp,
    LimitScan,
    Project,
    SearchOperator,
)
from palimpzest.query.optimizer.optimizer import Optimizer
from palimpzest.query.optimizer.primitives import Group, LogicalExpression
from pydantic.fields import FieldInfo

from ap_picker.operators import IMPLEMENTATION_RULES, TRANSFORMATION_RULES


class ApOptimizer(Optimizer):
    """
    Custom optimizer that includes AP-specific rules.
    Also overrides construct_group_tree to fix a palimpzest bug where a dict
    (unhashable) was placed directly into a set for the 'maps' property.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.implementation_rules.extend(IMPLEMENTATION_RULES)
        self.transformation_rules.extend(TRANSFORMATION_RULES)
