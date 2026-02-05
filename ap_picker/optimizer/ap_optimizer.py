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
        rules_to_remove = {"RAGRule"}
        self.implementation_rules = [
            rule for rule in self.implementation_rules
            if rule.__name__ not in rules_to_remove
        ]
        print(self.implementation_rules)
