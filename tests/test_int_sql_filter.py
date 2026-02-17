import logging
from asyncio.log import logger

from conftest import TestModels
from palimpzest import QueryProcessorConfig

from ap_picker.datasets.moma.dataset import MomaDataset

logger = logging.getLogger(__name__)


def test_int_sql_filter(sample_dataset: MomaDataset, models: TestModels):
    avail_models = [models["llama"], models["nomic"]]
    output = (
        sample_dataset
        .sem_filter("Questions on algebra", depends_on=["description"])
        .run(
            max_quality=True,
            config=QueryProcessorConfig(
                available_models=avail_models, progress=False)
        )
    )
    logger.info(f"Output: {output.to_df()}")
    assert output is not None
    assert len(output) > 0
