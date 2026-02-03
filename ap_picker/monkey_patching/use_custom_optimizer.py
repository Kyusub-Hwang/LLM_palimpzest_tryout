
from palimpzest.query.optimizer.cost_model import SampleBasedCostModel
from palimpzest.query.optimizer.optimizer import Optimizer
from palimpzest.query.processor.config import QueryProcessorConfig
from palimpzest.query.processor.query_processor_factory import QueryProcessorFactory


def use_custom_optimizer(optimizer_cls: type[Optimizer]):
    """
    Replace the default rules optimizer in Palimpzest with a custom one.

    The default Optimize has a default set of implementation rules that cannot be modified.
    BEWARE : This is a monkey-patch solution, and should not be used outside of testing

    Args:
        optimizer_cls (type[Optimizer]): The custom optimizer class to use.
    """

    def _create_optimizer(config: QueryProcessorConfig) -> Optimizer:
        return optimizer_cls(
            cost_model=SampleBasedCostModel(),  # type: ignore
            **config.to_dict()
        )
    QueryProcessorFactory._create_optimizer = _create_optimizer
