from palimpzest.query.optimizer.optimizer import Optimizer

from ap_picker.operators import IMPLEMENTATION_RULES, TRANSFORMATION_RULES


class ApOptimizer(Optimizer):
    """
    Custom optimizer that includes AP-specific rules 
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.implementation_rules.extend(IMPLEMENTATION_RULES)
        self.transformation_rules.extend(TRANSFORMATION_RULES)
