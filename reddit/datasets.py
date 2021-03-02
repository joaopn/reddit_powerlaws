# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2021-03-02 00:44:06
# @Last Modified by:   joaopn
# @Last Modified time: 2021-03-02 00:48:19

'''
Module for handling the reddit data dump available at https://files.pushshift.io/reddit/
'''

def count_subreddits(file,savefile=None, chunksize=1000):
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

