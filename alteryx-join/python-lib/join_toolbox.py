# coding: utf-8
import pandas as pd
import numpy as np
import logging

import dataiku
from dataiku.core.sql import SQLExecutor2, HiveExecutor
from dataiku.sql import toSQL
from dataiku.sql import Expression, Column, List, Constant, Interval, SelectQuery, Window, TimeUnit, ColumnType


import sql_generation
from sql_generation import dialectHandler

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,  # avoid getting log from 3rd party module
                    format='alteryx-join plugin %(levelname)s - %(message)s')


class JoinParams:

	def __init__(self, 
				 join_type = ['left']):

		self.join_types = join_type

    @staticmethod
    def deserialize(serialized_params):
        params = AggregationParams()
        params.__dict__ = serialized_params
        params.check()
        return params

    def check(self):
    	return None


class Joiner:

	def __init__(self, join_params):
		self.params = join_params

	def _make_query(self):

		is_hdfs = 'hdfsTableName' in dataset.get_config().get('params').keys()
		keys_expr = [Column(k) for k in self.params.keys]

		query = SelectQuery()
		if is_hdfs():
			query.select_from(left_dataset, alias='left') #TODO update this
		else:
			query.select_from(left_dataset, alias='left')

		query.select(keys_expr)
		join_cond = Expression()
		for key in self.params.keys:
			join_cond = join_cond.and_(Column(key, 'inner')).eq_null_unsafe(Column, agg)
		query.join(right_dataset, JoinTypes.LEFT, join_cond)

	def run(self, left_dataset, right_dataset):

		queries = self._make_query(left_dataset, right_dataset)

		

