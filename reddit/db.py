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
		batch_size: maximum number of documents to return per query. Limited to 16MB of data.

	Returns:
		list of number of comments (num_comments)

	"""

	client = MongoClient('mongodb://' + db_address)
	result = client[db][collection].find(filter={'subreddit': subreddit}, projection={'num_comments': 1, '_id': 0},
	                                     batch_size=batch_size)
	result_list = list(result)
	return [x['num_comments'] for x in result_list]


def update_subreddit_count(db='pushshift', collection='submissions', db_address='127.0.0.1:27017'):

	client = MongoClient('mongodb://' + db_address)

	client[db][collection].aggregate([
		{'$group': {'_id': '$subreddit',
		            'submissions': {'$sum': 1},
		            'comments': {'$sum': '$num_comments'}}},
		{'$sort': {'submissions': -1}},
		{'$out': collection + '_statistics'}
	], **{'allowDiskUse':True})

def get_subreddit_count(db='pushshift', collection='submissions', batch_size=1000000, db_address='127.0.0.1:27017'):

	client = MongoClient('mongodb://' + db_address)

	result = client[db][collection + '_statistics'].find(batch_size=batch_size)

	return list(result)
