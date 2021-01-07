# Copyright 2020 The Hyve
#
# Licensed under the GNU General Public License, version 3,
# or (at your option) any later version (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.gnu.org/licenses/
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

import copy
import datetime
import logging
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional, Union, List, Dict, ClassVar

import pandas as pd
from itertools import chain

from ...database.constraints import VOCAB_TABLES

logger = logging.getLogger()


@dataclass
class _AbstractEtlBase(ABC):
    start: datetime.datetime = field(default_factory=datetime.datetime.now)
    end: Optional[datetime.datetime] = None

    @abstractmethod
    def __str__(self):
        pass

    @property
    def duration(self) -> Optional[datetime.timedelta]:
        if self.start is not None and self.end is not None:
            return self.end - self.start

    def end_now(self):
        """Set current time as the end time."""
        self.end = datetime.datetime.now()

    def to_dict(self) -> Dict:
        d = self.__dict__
        d['duration'] = self.duration
        return d


@dataclass
class EtlSource(_AbstractEtlBase):
    """Metadata storage unit for source tables/files."""
    source_name: str = ''
    n_rows: Optional[int] = None

    df_column_order: ClassVar = ['source_name', 'n_rows', 'duration', 'start', 'end']

    def __str__(self):
        if self.duration is not None:
            return f'{self.source_name}: {self.n_rows} ({self.duration})'
        else:
            return f'{self.source_name}: {self.n_rows}'

    def to_dict(self) -> Dict:
        return super().to_dict()


@dataclass
class EtlTransformation(_AbstractEtlBase):
    """Metadata storage unit for data mutation calls."""
    name: str = ''
    query_success: bool = True
    insertion_counts: Counter = field(default_factory=Counter)
    deletion_counts: Counter = field(default_factory=Counter)
    update_counts: Counter = field(default_factory=Counter)

    df_column_order: ClassVar = ['name', 'query_success', 'insertion_counts', 'update_counts',
                                 'deletion_counts', 'duration', 'start', 'end']

    def __str__(self):
        return f'{self.name} ({self.duration})'

    @property
    def is_empty(self) -> bool:
        return (not self.insertion_counts
                and not self.deletion_counts
                and not self.update_counts)

    @property
    def is_vocab_only(self) -> bool:
        """All mutations are exclusively on vocabulary tables."""
        tables = {table.rsplit('.', 1)[-1] for table in chain(self.insertion_counts.keys(),
                                                              self.update_counts.keys(),
                                                              self.deletion_counts.keys())}
        is_vocab_table = [table in VOCAB_TABLES for table in tables]
        return all(is_vocab_table)

    def to_dict(self) -> Dict:
        """
        Return dict with empty Counters as None, otherwise convert to
        string.
        """
        d = copy.deepcopy(super().to_dict())
        for key, value in d.items():
            if isinstance(value, Counter):
                if not value:
                    d[key] = None
                else:
                    counts: List[str] = [':'.join([table, str(count)])
                                         for table, count in value.items()]
                    d[key] = ', '.join(counts)
        return d


class EtlStats:
    """
    Metadata storage unit for ETL statistics.

    Can contain:
     - list of transformations executed with script name, target table,
       start time, end time, status (exceptions) and number of affected
       rows (**EtlTransformation**).
     - list of source tables with file/tablename and raw input row
       counts (**EtlSource**).
    """
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.transformations: List[EtlTransformation] = []
        self.sources: List[EtlSource] = []

    @property
    def n_queries_executed(self) -> int:
        return len(self.transformations)

    @property
    def successful_transformations(self) -> List[EtlTransformation]:
        return [t for t in self.transformations if t.query_success]

    @property
    def total_insertions(self) -> Counter:
        return sum([t.insertion_counts for t in self.successful_transformations], Counter())

    @property
    def sources_df(self) -> pd.DataFrame:
        sources_df = pd.DataFrame(columns=EtlSource.df_column_order)
        sources_df = sources_df.append([s.to_dict() for s in self.sources])
        return sources_df[EtlSource.df_column_order]

    @property
    def transformations_df(self) -> pd.DataFrame:
        transformations_df = pd.DataFrame(columns=EtlTransformation.df_column_order)
        transformations_df = transformations_df.append([t.to_dict() for t in self.transformations])
        return transformations_df[EtlTransformation.df_column_order]

    def reset(self) -> None:
        """Remove all stored Etl objects from this instance."""
        self.start_time = datetime.datetime.now()
        self.transformations = []
        self.sources = []

    @staticmethod
    def get_total_duration(etl_objects: Union[List[EtlTransformation], List[EtlSource]]
                           ) -> datetime.timedelta:
        durations = (obj.duration for obj in etl_objects if obj.start and obj.end)
        return sum(durations, datetime.timedelta())

    def add_transformation(self, transformation: EtlTransformation) -> None:
        self.transformations.append(transformation)

    def add_source(self, source: EtlSource) -> None:
        self.sources.append(source)


etl_stats = EtlStats()
