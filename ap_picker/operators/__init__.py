from palimpzest.query.optimizer.rules import ImplementationRule as _ImplementationRule
from palimpzest.query.optimizer.rules import TransformationRule as _TransformationRule

from .sem_filter_by_task import TaskFilterRule

# NOTE : this structures mimics palimpzest's rule registration system
# https://github.com/mitdbg/palimpzest/blob/01a7aaa8d28f220a4eaed006bbc1c87fafe076b6/src/palimpzest/query/optimizer/__init__.py#L87
# This is to allow easier integration of custom rules if a fork of palimpzest is used
ALL_RULES = [
    TaskFilterRule
]

IMPLEMENTATION_RULES = [
    rule
    for rule in ALL_RULES
    if issubclass(rule, _ImplementationRule)
    and rule not in [_ImplementationRule]
]

TRANSFORMATION_RULES = [
    rule for rule in ALL_RULES if issubclass(rule, _TransformationRule) and rule not in [_TransformationRule]
]
