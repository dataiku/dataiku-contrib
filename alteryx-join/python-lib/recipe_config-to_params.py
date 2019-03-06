# coding: utf-8
import ast
import re
import logging

from join_toolbox import JoinParams


def get_params(recipe_config):
	def _p(param_name, default=None):
		return recipe_config.get(param_name, default)

	params = JoinParams(_p('join_type'))
	params.keys = _p('join_key')

	return params