"""
Module with functions to interface with and maintain the dataset MongoDB database.
"""

from pymongo import MongoClient
import numpy as np

def add_data(db_from, db_to, collection_from, collection_to, fields_selection, db_address=
'127.0.0.1:27017'):

	"""
	Copies selected fields from a database collection to another.
	Args:
		db_from (str): origin database
		db_to (str): target database
		collection_from (str): origin collection
		collection_to (str): target collection
		fields_selection (str): Set of fields to copy. Valid options: 'comments','submissions'

	Returns:
		None
	"""

	global fields
	if fields_selection == 'comments':
		fields = ['author', 'created_utc', 'id', 'subreddit', 'score', 'link_id', 'parent_id']
	elif fields_selection == 'submissions':
		fields = ['author', 'created_utc', 'id', 'subreddit', 'score', 'domain', 'num_comments']
	else:
		raise ValueError('field_selection must be "comments" or "submissions"')

	client = MongoClient('mongodb://' + db_address)

	project = {field:1 for field in fields}
	project['_id'] = 0

	pipeline = [
    {'$project': project},
	{'$merge':{'into':{'db': db_to, 'coll': collection_to}}}
	]

	client[db_from][collection_from].aggregate(pipeline, **{'allowDiskUse': True})

def add_collection(collection, db, db_address=
'127.0.0.1:27017'):
	"""
	Ensures a collection is created with the proper configuration options.
	Args:
		collection (str): collection name
		db (str): DB name
		db_address (str): IP address of the DB

	Returns:
		None
	"""
	client = MongoClient('mongodb://' + db_address)
	client[db].create_collection(collection, storageEngine={'wiredTiger': {'configString': "block_compressor=zstd"}})

def get_num_comments(subreddit, db='reddit', collection='submissions', batch_size=1000000, db_address=
'127.0.0.1:27017'):
	"""
	Loads a list of number of comments in a submission.
	Args:
		subreddit (str): Subreddit to get data from
		db (str): DB name
		collection (str): collection name
		batch_size: maximum number of documents to return per query. Limited to 16MB of data.
		db_address (str): IP address of the DB

	Returns:
		list of number of comments (num_comments)
	"""

	client = MongoClient('mongodb://' + db_address)
	result = client[db][collection].find(filter={'subreddit': subreddit}, projection={'num_comments': 1, '_id': 0},
	                                     batch_size=batch_size)
	result_list = list(result)
	return np.array([x['num_comments'] for x in result_list])


def add_subreddit_statistics(db='reddit', collection='submissions', db_address='127.0.0.1:27017'):
	"""
	Updates the db collection [collection]_statistics, which stores # of submissions and comments in the subreddit.
	Args:
		db (str): DB name
		collection (str): collection name
		db_address (str): IP address of the DB

	"""
	client = MongoClient('mongodb://' + db_address)

	pipeline = [
    {'$match': {'subreddit': {'$ne': None}}},
		{'$project': {'subreddit': 1, '_id': 0, 'num_comments': 1}},
		{'$group': {'_id': '$subreddit',
		            'submissions': {'$sum': 1},
		            'comments': {'$sum': '$num_comments'}}},
		{'$sort': {'submissions': -1}},
		{'$out': 'subreddit_statistics'}]

	client[db][collection].aggregate(pipeline, **{'allowDiskUse': True})


def get_subreddit_statisics(db='reddit', collection='submissions', batch_size=1000000, db_address='127.0.0.1:27017'):
	"""
	Gets the DB statistics from [collection]_statistics, which stores # of submissions and comments in the subreddit.
	Args:
		db (str): DB name
		collection (str): collection name
		db_address (str): IP address of the DB

	Returns:
		list of dict results containings  {'_id': [subreddit name], 'submissions': int, 'comments': int}
	"""

	client = MongoClient('mongodb://' + db_address)

	result = client[db][collection + '_statistics'].find(batch_size=batch_size)

	return list(result)


def get_subreddit_count_single(subreddit, db='reddit', collection='submissions', batch_size=1000000,
                               db_address='127.0.0.1:27017'):

	"""
	Returns the subreddit statistics from a single subreddit, reading the DB directly.
	Args:
		db (str): DB name
		collection (str): collection name
		db_address (str): IP address of the DB

	Returns:
		list of dict results containings  {'_id': [subreddit name], 'submissions': int, 'comments': int}
	"""

	client = MongoClient('mongodb://' + db_address)
	pipeline = [
		{'$match': {'subreddit': subreddit}},
		#{'$project': {'subreddit': 1, '_id': 0, 'num_comments': 1}},
		{'$group': {'_id': '$subreddit',
		            'num_comments': {'$sum': '$num_comments'},
		            'submissions': {'$sum': 1}
		            }}
	]
	result = client[db][collection].aggregate(pipeline,**{'allowDiskUse': True, 'batchSize':batch_size})
	return list(result)
