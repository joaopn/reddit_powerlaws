# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2021-03-02 00:44:06
# @Last Modified by:   joaopn
# @Last Modified time: 2021-03-06 14:48:27

"""
Module for handling the reddit data, both the data dump (available at https://files.pushshift.io/reddit/) and the
processed hdf5 files.
"""

import numpy as np
import pandas as pd
import tables

def load_data(subreddit, data_location, year_range, fields=['num_comments']):
	"""
	Loads data from the hdf5 files for a single subreddit
	Args:
		subreddit:
		data_location
		year_range:
	Returns:

	"""

	YEAR_MIN, YEAR_MAX = year_range

	file_location = submission_filenames(np.arange(YEAR_MIN, YEAR_MAX + 1), path=data_location)

	data_list = list()

	for str_data in file_location:
		try:
			data_list.append(pd.read_hdf(str_data, subreddit, columns=fields))
		except:
			pass

	return pd.concat(data_list, ignore_index=True)

def count_subreddits(file, savefile=None, chunksize=1000):
	"""Reads a monthly submission dump by chunks, returning a pandas Series with the count of each subreddit.
	
	Args:
		file (TYPE): file location
		savefile (None, optional): Saves results to a csv
		chunksize (int, optional): size of the chunk to read
	
	Returns:
		Series: comment count for each subreddit.
	"""

	subreddits = pd.Series(dtype='float64')

	for chunk in pd.read_json(file, lines=True, chunksize=chunksize):
		subreddits = subreddits.add(chunk['subreddit'].value_counts(), fill_value=0)

	if savefile is not None:
		subreddits.to_csv(savefile, header=False)

	return subreddits

def count_subreddits_h5(file, savefile):
	"""
	Returns a dataframe of number of submissions and total comments for all subreddits in the h5 file.
	Args:
		file (str): file location

	Returns:
		DataFrame of [subreddits, submissions, comments
	"""
	load_obj = pd.HDFStore(file, mode='r', complevel=9)
	subreddits = load_obj.keys()
	if '/' in subreddits:
		subreddits.remove('/')
	load_obj.close()

	a = tables.open_file(file,'r')

	data_list = []
	for subreddit in subreddits:
		data = a.root[subreddit].table.read(field='values_block_0')[:,1] #hardcoded position of comments
		data_list.append({'name': subreddit[1:], 'submissions': data.size, 'comments': data.sum()})

	df = pd.DataFrame(data_list)
	df.to_csv(savefile, index=False)

def subreddits_hdf5(file, savefile, chunksize=100000, mem_limit=1000, drop_stickied=True):
	"""Parses a submission dump dataset into an HDF5 file, where each group is a subreddit.
	
	Args:
		file (str): file location
		savefile (str): savefile location
		chunksize (int, optional): Chunksize to read the json file
		mem_limit (int, optional): size of the memory cache before dumping to disk
		drop_stickied (bool, optional): Whether to drop stickied submissions (default True)
	"""

	# Fixed parameters
	fields = ['subreddit', 'author', 'domain', 'created_utc', 'num_comments', 'score', 'id']
	field_dtypes = {'subreddit': str, 'author': str, 'domain': str, 'created_utc': int, 'num_comments': int,
	                'score': int, 'id': str}

	# File object
	save_obj = pd.HDFStore(savefile, mode='a', complevel=9)

	# Stores caching data
	results_list = []
	mem_used = 0

	for df in pd.read_json(file, lines=True, chunksize=chunksize, dtype=field_dtypes):

		# Drops stickied sub
		if drop_stickied and 'stickied' in df.keys():
			df.drop(df[df['stickied']].index, inplace=True)

		# Drops other columns
		df = df[fields]

		# Adds caching data
		mem_used += df.memory_usage().sum() / 1024 / 1024
		results_list.append(df)

		# Dumps data if mem_used > mem_limit
		if mem_used > mem_limit:

			df = pd.concat(results_list)
			grouped_df = df.groupby('subreddit')

			for subreddit in df['subreddit'].unique():
				save_obj.put('/' + subreddit, grouped_df.get_group(subreddit), append=True, format='table',
				             min_itemsize=255)

			results_list = []
			mem_used = 0
			last_saved = True  # ugly hack because python lacks an iterator check
		else:
			last_saved = False

	# Saves last piece of data
	if last_saved is False:
		df = pd.concat(results_list)
		grouped_df = df.groupby('subreddit')
		for subreddit in df['subreddit'].unique():
			save_obj.put('/' + subreddit, grouped_df.get_group(subreddit), append=True, format='table',
			             min_itemsize=255)

	# Closes object
	save_obj.close()


def submission_filenames(years=None, path=None, termination=None, last_data = [2020,4]):
	"""Returns a list of filename paths for datasets up to 06/2005-07/2019.

	Args:
		years (optional): tuple of years to load (None for all)
		path (str, optional): path to dataset
		termination (str, optional): file termination

	Returns:
		TYPE: list of filepaths
	"""

	if years is None:
		years = np.arange(2005, 2020)
	else:
		years = np.arange(years[0], years[1]+1)

	filenames = []

	for year in years:

		if year == 2005:
			month_range = np.arange(6, 13)
		elif year == last_data[0]:
			month_range = np.arange(1, last_data[1]+1)
		else:
			month_range = np.arange(1, 13)

		if year > 2010:
			name_base = 'RS'
		else:
			name_base = 'RS_v2'

		for month in month_range:

			str_file = '{:s}_{:d}-{:02d}'.format(name_base, year, month)
			if path is not None:
				str_file = path + str_file
			if termination is not None:
				str_file = str_file + termination

			filenames.append(str_file)

	return filenames
