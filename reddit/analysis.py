"""
Module for analysis functions. The functions here only take in-memory data, simpler statistics-extraction directly
from the hdf5 files are hosted in the datasets module.
"""

from reddit import datasets
import pandas as pd
import numpy as np
import powerlaw as plw
from itertools import combinations

def get_statistics(df):
	"""
	Calculates statistics of a dataframe with the fields ['num_comments','score','author']
		Args:
			df:

	Returns:

	"""

	statistics = {}
	statistics['n_sub'] = len(df)
	statistics['n_comm'] = df['num_comments'].sum()
	statistics['score_mean'] = df['score'].mean()
	statistics['score_std'] = df['score'].std()
	statistics['unique_authors'] = len(df['author'].unique())
	statistics['comments_mean'] = df['num_comments'].mean()
	statistics['comments_std'] = df['num_comments'].std()
	statistics['R'] = df['num_comments'].mean()

	return statistics

def fit_compare(data, estimate_discrete = True, p_lim = 0.05):

	fit_obj = plw.Fit(data, estimate_discrete=estimate_discrete)

	distributions = ['power_law', 'truncated_power_law', 'exponential','lognormal_positive']

	results = {}

	results['power_law_score'] = 0
	results['truncated_power_law_score'] = 0
	results['exponential_score'] = 0
	results['lognormal_positive_score'] = 0

	#Tests all distribution combinations
	for (a,b) in combinations(distributions,2):
		(likelihood_ratio, p) = fit_obj.distribution_compare(a,b)
		if p < p_lim:
			if likelihood_ratio > 0:
				results[a+'_score'] += 1
			else:
				results[b+'_score'] += 1

	#Selects a best fit if it is better than all the others
	results['best_fit'] = None
	for dist in distributions:
		if results[dist + '_score'] == 3:
			results['best_fit'] = dist

	#Fills data
	results['power_law_alpha'] = fit_obj.power_law.alpha
	try:
		results['power_law_xmin'] = fit_obj.power_law.xmin
		results['power_law_xmax'] = fit_obj.pdf()[0][-1]
		results['power_law_orders'] = np.log10(results['power_law_xmax']) - np.log10(results['power_law_xmin'])
	except:
		results['power_law_xmin'] = fit_obj.power_law.xmin
		results['power_law_xmax'] = fit_obj.power_law.xmin
		results['power_law_orders'] = 0

	results['truncated_power_law_alpha'] = fit_obj.truncated_power_law.parameter1
	results['truncated_power_law_lambda'] = fit_obj.truncated_power_law.parameter2
	try:
		results['truncated_power_law_xmin'] = fit_obj.truncated_power_law.xmin
		results['truncated_power_law_xmax'] = fit_obj.pdf()[0][-1]
		results['truncated_power_law_orders'] = np.log10(results['truncated_power_law_xmax']) - np.log10(results[
			'truncated_power_law_xmin'])
	except:
		results['truncated_power_law_xmin'] = fit_obj.truncated_power_law.xmin
		results['truncated_power_law_xmax'] = fit_obj.truncated_power_law.xmin
		results['truncated_power_law_orders'] = 0

	results['exp_lambda'] = fit_obj.exponential.parameter1

	results['lognormal_positive_mu'] = fit_obj.lognormal_positive.parameter1
	results['lognormal_positive_sigma'] = fit_obj.lognormal_positive.parameter2

	return results