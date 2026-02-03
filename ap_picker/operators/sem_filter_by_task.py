import logging

from palimpzest.constants import (
    MODEL_CARDS,
    NAIVE_EST_FILTER_SELECTIVITY,
    NAIVE_EST_NUM_INPUT_TOKENS,
    Cardinality,
    Model,
    PromptStrategy,
)
from palimpzest.core.elements.records import DataRecord
from palimpzest.core.models import GenerationStats, OperatorCostEstimates
from palimpzest.query.generators.generators import Generator
from palimpzest.query.operators.filter import FilterOp
from palimpzest.query.operators.logical import FilteredScan
from palimpzest.query.optimizer.primitives import LogicalExpression, PhysicalExpression
from palimpzest.query.optimizer.rules import ImplementationRule
from palimpzest.utils.model_helpers import resolve_reasoning_settings
from pydantic.fields import FieldInfo

logger = logging.getLogger(__name__)


class TaskFilter(FilterOp):
    def __init__(
        self,
        model: Model,
        prompt_strategy: PromptStrategy = PromptStrategy.FILTER,
        reasoning_effort: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.model = model
        self.prompt_strategy = prompt_strategy
        self.reasoning_effort = reasoning_effort
        if model is not None:
            self.generator = Generator(model, prompt_strategy, reasoning_effort,
                                       self.api_base, Cardinality.ONE_TO_ONE, self.desc, self.verbose)

    def get_id_params(self):
        id_params = super().get_id_params()
        id_params = {
            "model": None if self.model is None else self.model.value,
            "prompt_strategy": None if self.prompt_strategy is None else self.prompt_strategy.value,
            "reasoning_effort": self.reasoning_effort,
            **id_params,
        }

        return id_params

    def get_op_params(self):
        op_params = super().get_op_params()
        op_params = {
            "model": self.model,
            "prompt_strategy": self.prompt_strategy,
            "reasoning_effort": self.reasoning_effort,
            **op_params,
        }

        return op_params

    def get_model_name(self):
        return None if self.model is None else self.model.value

    def naive_cost_estimates(self, source_op_cost_estimates: OperatorCostEstimates):
        # estimate number of input tokens from source
        est_num_input_tokens = NAIVE_EST_NUM_INPUT_TOKENS
        if self.is_image_op():
            est_num_input_tokens = 765 / 10  # 1024x1024 image is 765 tokens

        # NOTE: the output often generates an entire reasoning sentence, thus the true value may be higher
        # the filter operation's LLM call should only output TRUE or FALSE, thus we expect its
        # number of output tokens to be ~1.25
        est_num_output_tokens = 1.25

        # get est. of conversion time per record from model card;
        model_conversion_time_per_record = (
            MODEL_CARDS[self.model.value]["seconds_per_output_token"] *
            est_num_output_tokens
        )

        # get est. of conversion cost (in USD) per record from model card
        usd_per_input_token = (
            MODEL_CARDS[self.model.value]["usd_per_audio_input_token"]
            if self.is_audio_op()
            else MODEL_CARDS[self.model.value]["usd_per_input_token"]
        )
        model_conversion_usd_per_record = (
            usd_per_input_token * est_num_input_tokens
            + MODEL_CARDS[self.model.value]["usd_per_output_token"] *
            est_num_output_tokens
        )

        # estimate output cardinality using a constant assumption of the filter selectivity
        selectivity = NAIVE_EST_FILTER_SELECTIVITY
        cardinality = selectivity * source_op_cost_estimates.cardinality

        # estimate quality of output based on the strength of the model being used
        quality = (MODEL_CARDS[self.model.value]["overall"] / 100.0)

        return OperatorCostEstimates(
            cardinality=cardinality,
            time_per_record=model_conversion_time_per_record,
            cost_per_record=model_conversion_usd_per_record,
            quality=quality,
        )

    def filter(self, candidate: DataRecord) -> tuple[dict[str, bool], GenerationStats]:
        # get the set of input fields to use for the filter operation
        input_fields = self.get_input_fields()

        # construct kwargs for generation
        gen_kwargs = {"project_cols": input_fields,
                      "filter_condition": self.filter_obj.filter_condition}

        # generate output; NOTE: FieldInfo is used to indicate the output type; thus, the desc is not needed
        fields = {"passed_operator": FieldInfo(
            annotation=bool, description="Whether the record passed the filter operation")}
        field_answers, _, generation_stats, _ = self.generator(
            candidate, fields, **gen_kwargs)

        return field_answers, generation_stats


class TaskFilterRule(ImplementationRule):
    """
    Substitute a logical expression for a FilteredScan with an Task-based filter physical implementation.
    """

    @classmethod
    def matches_pattern(cls, logical_expression: LogicalExpression) -> bool:
        """
        This method checks if this rule can be applied to the execution plan.
        """
        logical_op = logical_expression.operator
        is_match = isinstance(
            logical_op, FilteredScan) and logical_op.filter.filter_fn is None
        logger.debug(
            f"TaskFilterRule matches_pattern: {is_match} for {logical_expression}")
        return is_match

    @classmethod
    # NOTE : The signature is theorically incompatible with the base class, but this seems to be an intentional design of Palimpzest
    def substitute(cls, logical_expression: LogicalExpression, **runtime_kwargs) -> set[PhysicalExpression]:  # pyright: ignore[reportIncompatibleMethodOverride] # noqa: E501
        logger.debug(f"Substituting TaskFilterRule for {logical_expression}")

        # create variable physical operator kwargs for each model which can implement this logical_expression
        models = [model for model in runtime_kwargs["available_models"]
                  if cls._model_matches_input(model, logical_expression)]
        variable_op_kwargs = []
        for model in models:
            use_reasoning_prompt, reasoning_effort = resolve_reasoning_settings(
                model, runtime_kwargs["reasoning_effort"])
            prompt_strategy = PromptStrategy.FILTER if use_reasoning_prompt else PromptStrategy.FILTER_NO_REASONING
            variable_op_kwargs.append(
                {
                    "model": model,
                    "prompt_strategy": prompt_strategy,
                    "reasoning_effort": reasoning_effort,
                }
            )

        return cls._perform_substitution(logical_expression, TaskFilter, runtime_kwargs, variable_op_kwargs)
