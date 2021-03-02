# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2021-03-02 00:44:06
# @Last Modified by:   joaopn
# @Last Modified time: 2021-03-02 13:44:10

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

def split_subreddits(file, subreddit_list, save_path, fields, chunksize=1000):
	"""Processes a submission dump, splitting the data into a CSV file for each subreddit.
	
	Args:
		file (TYPE): Description
		subreddit_list (TYPE): Description
		save_path (TYPE): Description
		chunksize (int, optional): Description
	"""

	pass

def subreddits_hdf5(file, savefile, chunksize=10000, drop_stickied = True):
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

	for df in pd.read_json(file, lines = True, chunksize=chunksize, dtype=field_dtypes):

		#Drops stickied sub
		if drop_stickied and 'stickied' in df.keys():
			df.drop(df[df['stickied']].index, inplace=True)

		#Drops other columns
		df = df[fields]

		#Adds data to df
		for subreddit in df['subreddit'].unique():
			df[df['subreddit'] == subreddit].to_hdf(savefile, '/' + subreddit, complevel=9, append=True, min_itemsize=255)


