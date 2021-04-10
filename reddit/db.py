"""
Module with functions to interface with and maintain the dataset MongoDB database.
"""

from pymongo import MongoClient

def get_num_comments(subreddit, db='pushshift', collection='submissions', batch_size=1000000, db_address=
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
	return [x['num_comments'] for x in result_list]


def update_subreddit_count(db='pushshift', collection='submissions', db_address='127.0.0.1:27017'):
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


def get_subreddit_count(db='pushshift', collection='submissions', batch_size=1000000, db_address='127.0.0.1:27017'):
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


def get_subreddit_count_single(subreddit, db='pushshift', collection='submissions', batch_size=1000000,
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
