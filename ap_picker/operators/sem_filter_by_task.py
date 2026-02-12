from __future__ import annotations

import time

from palimpzest.constants import (
    NAIVE_EST_FILTER_SELECTIVITY,
)
from palimpzest.core.elements.records import DataRecord
from palimpzest.core.models import GenerationStats, OperatorCostEstimates
from palimpzest.query.operators.filter import FilterOp


class NonLLMFilter(FilterOp):

    def naive_cost_estimates(self, source_op_cost_estimates: OperatorCostEstimates):
        # estimate output cardinality using a constant assumption of the filter selectivity
        selectivity = NAIVE_EST_FILTER_SELECTIVITY
        cardinality = selectivity * source_op_cost_estimates.cardinality

        # estimate 1 ms single-threaded execution for filter function
        time_per_record = 0.001

        # assume filter fn has perfect quality
        return OperatorCostEstimates(
            cardinality=cardinality,
            time_per_record=time_per_record,
            cost_per_record=0.0,
            quality=1.0,
        )

    def filter(self, candidate: DataRecord) -> tuple[dict[str, bool], GenerationStats]:
        # apply filter function to input record
        start_time = time.time()
        answer = {}
        try:
            # execute the UDF filter
            passed_operator = self.filter_obj.filter_fn(candidate.to_dict())
            answer = {"passed_operator": passed_operator}

            if self.verbose:
                print(f"{self.filter_obj.get_filter_str()}:\n{passed_operator}")

        except Exception as e:
            print(f"Error invoking user-defined function for filter: {e}")
            raise e

        # create generation stats object containing the time spent executing the UDF function
        generation_stats = GenerationStats(
            fn_call_duration_secs=time.time() - start_time)

        return answer, generation_stats
