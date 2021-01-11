from collections import Counter
from datetime import datetime

from src.delphyne.model.etl_stats import EtlTransformation


def get_etltransformation(name: str, **kwargs) -> EtlTransformation:
    default_params = {
        'name': name,
        'start': datetime(year=2025, month=1, day=1, hour=1),
        'end': datetime(year=2025, month=1, day=1, hour=2),
        'query_success': True,
        'insertion_counts': Counter(),
        'update_counts': Counter(),
        'deletion_counts': Counter(),
    }
    # Replace default values with those provided, if any
    final_params = {k: v if k not in kwargs else kwargs[k] for k, v in default_params.items()}
    return EtlTransformation(**final_params)
