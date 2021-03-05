# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2021-03-02 00:44:06
# @Last Modified by:   joaopn
# @Last Modified time: 2021-03-05 22:24:02

'''
Module for handling the reddit data dump available at https://files.pushshift.io/reddit/
'''

import pandas as pd
import h5py

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

	for chunk in pd.read_json(file, lines = True, chunksize=chunksize):
		subreddits = subreddits.add(chunk['subreddit'].value_counts(),fill_value=0)

	if savefile is not None:
		subreddits.to_csv(savefile, header=False)

	return subreddits

def subreddits_hdf5(file, savefile, chunksize=100000, mem_limit = 1000, drop_stickied = True):
	"""Parses a submission dump dataset into an HDF5 file, where each group is a subreddit.
	
	Args:
		file (str): file location
		savefile (str): savefile location
		fields (str): fields to preserve
		chunksize (int, optional): Chunksize to read the json file
		drop_stickied (bool, optional): Whether to drop stickied submissions (default True)
	"""

	#Fixed parameters
	fields = ['subreddit', 'author', 'domain', 'created_utc','num_comments', 'score', 'id']
	field_dtypes = {'subreddit':str, 'author':str, 'domain':str, 'created_utc':int,'num_comments':int, 'score':int, 'id':str}

	#File object
	save_obj = pd.HDFStore(savefile,mode='a', complevel=9)

	#Stores caching data
	results_list = []
	mem_used = 0

	for df in pd.read_json(file, lines = True, chunksize=chunksize, dtype=field_dtypes):

		#Drops stickied sub
		if drop_stickied and 'stickied' in df.keys():
			df.drop(df[df['stickied']].index, inplace=True)

		#Drops other columns
		df = df[fields]

		#Adds caching data
		mem_used += df.memory_usage().sum()/1024/1024
		results_list.append(df)

		#Dumps data if mem_used > mem_limit
		if mem_used > mem_limit:

			df = pd.concat(results_list)
			grouped_df = df.groupby('subreddit')

			for subreddit in df['subreddit'].unique():
				save_obj.put('/' + subreddit, grouped_df.get_group(subreddit), append=True, format='table', min_itemsize=255)

			results_list = []
			mem_used = 0
			last_saved = True #ugly hack because python lacks an iterator check
		else:
			last_saved = False

	#Saves last piece of data
	if last_saved is False:
		df = pd.concat(results_list)
		grouped_df = df.groupby('subreddit')
		for subreddit in df['subreddit'].unique():
			save_obj.put('/' + subreddit, grouped_df.get_group(subreddit), append=True, format='table', min_itemsize=255)

	#Closes object
	save_obj.close()

