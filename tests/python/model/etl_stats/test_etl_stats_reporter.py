import logging
from collections import Counter

import pytest
from src.delphyne.model.etl_stats import EtlStats, EtlTransformation, EtlStatsReporter

from tests.python.model.etl_stats.conftest import get_etltransformation

logger = logging.getLogger('test_logger')
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope='function')
def reporter_summary_output(request, caplog) -> str:
    """Get summary report for the given EtlTransformation."""
    stats = EtlStats()
    transformation: EtlTransformation = request.param
    stats.add_transformation(transformation)
    reporter = EtlStatsReporter(etl_stats=stats)
    with caplog.at_level(logging.INFO):
        reporter.log_summary()
    return caplog.text


@pytest.mark.parametrize('reporter_summary_output',
                         [get_etltransformation(name='empty_successful')],
                         indirect=True)
def test_empty_successful(reporter_summary_output: str):
    assert 'empty_successful' not in reporter_summary_output


@pytest.mark.parametrize('reporter_summary_output',
                         [get_etltransformation(name='empty_unsuccessful', query_success=False)],
                         indirect=True)
def test_empty_unsuccessful(reporter_summary_output: str):
    assert 'empty_unsuccessful' in reporter_summary_output


@pytest.mark.parametrize('reporter_summary_output',
                         [get_etltransformation(name='with_records_successful',
                                                insertion_counts=Counter({'person': 5}))],
                         indirect=True)
def test_with_records_successful(reporter_summary_output: str):
    assert 'with_records_successful' in reporter_summary_output


@pytest.mark.parametrize('reporter_summary_output',
                         [get_etltransformation(name='with_records_unsuccessful',
                                                query_success=False,
                                                insertion_counts=Counter({'observation': 5}))],
                         indirect=True)
def test_with_records_unsuccessful(reporter_summary_output: str):
    assert 'with_records_unsuccessful' in reporter_summary_output
