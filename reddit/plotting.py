# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2021-03-02 00:49:46
# @Last Modified by:   Joao Neto
# @Last Modified time: 2021-03-09 15:55:27

import powerlaw as plw
import seaborn as sns
from reddit import pushshift
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def powerlaw(data, ax=None, show_fit=True, xmin=1):
	"""Plots the probability distribution of data with a power-law fit

	Args:
		data (list): list/numpy array of observations
		ax (None, optional): ax to plot the distribution
		show_fit (bool, optional): whether to show the power-law fit
		xmin (int, optional): smallest value to fit
	"""
	if ax is None:
		ax = plt.gca()

	# Varies fitting method based on package recomendation
	if xmin > 6:
		estimate_discrete = True
	else:
		estimate_discrete = False

	# Plots data
	data_nonzero = data[data > 0]
	pl_obj = plw.Fit(data_nonzero, xmin=xmin, estimate_discrete=estimate_discrete)
	str_label = r'N = {:0.0f}, R = {:0.1f}'.format(data_nonzero.size, np.sum(data_nonzero) / data_nonzero.size)
	pl_obj.plot_pdf(ax=ax, original_data=True, **{'label': str_label})

	if show_fit:
		str_label_fit = r'$\alpha$ = {:0.3f}'.format(pl_obj.power_law.alpha)
		pl_obj.power_law.plot_pdf(ax=ax, color='k', linestyle='--', **{'label': str_label_fit})


def powerlaws_df(df, ax):
	# count = np.array(df[field])
	# fit_obj = powerlaw.Fit(count[count>0], xmax=xmax, xmin=xmin)
	# fit_obj.plot_pdf(color=color)
	# fit_obj.power_law.plot_pdf(color=color, linestyle='--')
	# str_alpha_ncomm = r'$\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)

	# Plots n_comments and score distributions
	n_comm = np.array(df['num_comments_updated'])
	score = np.array(df['score_updated'])

	fit_ncomm = plw.Fit(n_comm[n_comm > 0], xmax=1e3, xmin=1)
	str_label = r'comments, $\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)
	fit_ncomm.plot_pdf(ax=ax, color='b', **{'label': str_label})
	fit_ncomm.power_law.plot_pdf(ax=ax, color='b', linestyle='--')
	# str_alpha_ncomm = r'$\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)

	fit_score = plw.Fit(score[score > 0], xmax=1e4, xmin=10)
	str_label = r'score, $\alpha$ = {:0.3f}'.format(fit_score.power_law.alpha)
	fit_score.plot_pdf(ax=ax, color='r', **{'label': str_label})
	fit_score.power_law.plot_pdf(ax=ax, color='r', linestyle='--')
	# str_alpha_score = r'$\alpha$ = {:0.3f}'.format(fit_score.power_law.alpha)

	plt.legend()


def subreddits_live(subreddit_list, date_min, date_max=None):
	"""Downloads and analyzes subreddit data from a list of subreddits, in a certain timeframe.

	Args:
		subreddit_list (list): list of subreddits
		date_min (str): Oldest post to get, in the ISO format ("2020-12-31")
		date_max (str, optional): Youngest post to get, in the ISO format
	"""

	df = pd.DataFrame()
	# Downloads data
	for subreddit in subreddit_list:
		print('Downloading data for r/' + subreddit)
		df = df.append(pushshift.download_posts(subreddit, date_min, date_max, fields=None), sort=True)

	# Updates all using praw
	print('Updating scores and comments')
	df = pushshift.update_praw(df)

	# Plots scatterplots
	print('Plotting results')
	sns.scatterplot(data=df, x='score_updated', y='num_comments_updated', hue='subreddit')
