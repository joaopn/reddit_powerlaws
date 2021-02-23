# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2020-01-08 20:32:02
# @Last Modified by:   joaopn
# @Last Modified time: 2020-01-31 12:59:59

from datetime import datetime
import pandas as pd
import requests
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import json
import praw
from time import sleep
import os

"""
    TODO
    - download_posts()
    	- Save intermediate pushift data
        - older pushift data lacks some fields: update *all* from praw.

 """

def download_posts(subreddit, date_min="2019-01-01", date_max = None, fields=['author', 'title', 'domain', "created_utc", 'id']):
    """Downloads all subreddit posts in a timeframe from the Pushshift API. Returns a dataframe.
    
    Args:
        subreddit (str): Subreddit to download
        date_min (str, optional): Oldest date to download posts
        date_max (None, optional): Newest date to download posts
        fields (list, optional): List of fields to return
    
    Returns:
        TYPE: dataframe contained the data specified in fields
    """

    #Parameters
    base_url = "https://api.pushshift.io/reddit/submission/search/"
    params = {"subreddit": subreddit, "sort": "desc","sort_type": "created_utc", "size": 500}

    #Get dates timestamps
    timestamp_min = datetime.fromisoformat(date_min).timestamp()
    if date_max is None:
        timestamp_max = int(datetime.now().timestamp())
    else:
        timestamp_max = int(datetime.fromisoformat(date_max).timestamp())
    params['before'] = timestamp_max 

    df = pd.DataFrame(columns=fields)

    timestamp_downloaded = timestamp_max

    while timestamp_downloaded > timestamp_min:

        #Requests data and stores it in a dataframe
        #print('Downloading from: ' + format(datetime.fromtimestamp(timestamp_downloaded).isoformat()))
        response = requests.get(base_url,params= params)
        df_temp = pd.DataFrame(response.json()['data'])
        #df_temp['date'] = df_temp['created_utc'].apply(lambda x: datetime.fromtimestamp(x).isoformat())

        #Gets timestamp of the earliest download, adds it to params
        timestamp_downloaded = df_temp['created_utc'].min()
        params['before'] = timestamp_downloaded

        #Adds relevant data to results dataframe
        if fields is None:
        	df = df.append(df_temp, sort = True)
        else:
        	df = df.append(df_temp[fields])

    #Resets dataframe index
    df.reset_index(inplace=True, drop=True)

    #Removes submissions before date_min
    df.drop(index=df[df.created_utc < timestamp_min].index, inplace=True)

    return df

def update_praw(df, auth_file = 'AUTH_user.json'):

    """Updates data using praw, adding 'score_updated', 'num_comments_updated', 'upvote_ratio_updated' to df.
    
    Args:
        df (DataFrame): reddit data dataframe, obtained from pushshift.io
    
    Returns:
        DataFrame: Contains updated metrics from praw
    """

    import praw

    #Estimate time to update df
    time_left_s = 2*len(df)/100/60
    print('Estimated time to update {:d} entries: {:0.0f} minutes'.format(len(df), time_left_s))
    print('0%...', end="")

    #Parameters and PASSWORDS
    with open(auth_file) as json_file: 
   		params = json.load(json_file) 

    #Opens praw session
    praw_session = praw.Reddit(client_id=params['client_id'], 
                     client_secret=params['client_secret'],
                     password=params['password'], 
                     user_agent=params['user_agent'],
                     username=params['username'])

    #Gets dataframes id, updates then to 'full format'
    id_full = list(df['id'].apply(lambda x: 't3_' + x))

    #Adds num_comments_updated, score_updated, upvote_ratio to the df
    #header_list = ['score_updated', 'num_comments_updated', 'upvote_ratio_updated', 'id']

    #Creates praw generator (100 ids per request) and dataframe to store results
    subs = praw_session.info(fullnames = id_full)
    #df_praw = pd.DataFrame(columns=header_list)

    #data_dict = {}
    data_dict = {'score_updated': [], 'num_comments_updated':[], 'id':[],'time_updated': []}

    count = 0
    for sub in subs:
        #data = {'score_updated': sub.score, 'num_comments_updated':sub.num_comments, 'upvote_ratio_updated':sub.upvote_ratio, 'id':sub.id,'time_updated': datetime.now().timestamp()}

        data_dict['score_updated'].append(sub.score)
        data_dict['num_comments_updated'].append(sub.num_comments)
        #data_dict['upvote_ratio_updated'].append(sub.upvote_ratio) #REDDIT BUG! Forces a new API request.
        data_dict['id'].append(sub.id)
        data_dict['time_updated'].append(datetime.now().timestamp())


        #data = sub
        #data_dict[count] = {'score_updated': sub.score, 'num_comments_updated':sub.num_comments, 'upvote_ratio_updated':sub.upvote_ratio, 'id':sub.id,'time_updated': datetime.now().timestamp()}

        count = count + 1

        if count == round(len(id_full)*0.25):
            print('25%...', end="")
        elif count == round(len(id_full)*0.5):
            print('50%...', end="")         
        if count == round(len(id_full)*0.75):
            print('75%...', end="") 

        #print(count)
        #data_dict[count] = {'score_updated': sub.score, 'num_comments':sub.num_comments, 'upvote_ratio_updated':sub.upvote_ratio, 'id':sub.id}

    print('done.')

    df_praw = pd.DataFrame(data_dict)
    df_updated = pd.merge(left=df, right = df_praw, how='left',left_on='id',right_on='id')
    #df_praw = df_praw.transpose()

    return df_updated

def plot_subreddits(subreddit_list, date_min, date_max = None):
    """Downloads and analyzes subreddit data from a list of subreddits, in a certain timeframe.
    
    Args:
        subreddit_list (list): list of subreddits
        date_min (str): Oldest post to get, in the ISO format ("2020-12-31")
        date_max (str, optional): Youngest post to get, in the ISO format
    """

    df = pd.DataFrame()
    #Downloads data
    for subreddit in subreddit_list:
        print('Downloading data for r/' + subreddit)
        df = df.append(download_posts(subreddit, date_min, date_max, fields=None), sort=True)

    #Updates all using praw
    print('Updating scores and comments')
    df = update_praw(df)

    #Plots scatterplots
    print('Plotting results')
    sns.scatterplot(data=df,x='score_updated',y='num_comments_updated', hue='subreddit')

def save_posts_year(subreddit_list, year_list, folder = 'data/', fields = None, filetype = '.csv'):
    """Saves subreddit data on a file, separating by year.
    
    Args:
        subreddit_list (TYPE): list of subreddit names to download
        year_list (list): list of years to save
        folder (str, optional): Location to save the .csv files
        fields (None, optional): List of df fields to save
        filetype (str, optional): pandas .to_csv filetype (.csv for uncompressed, .zip for compressed zip files)

    """
    for j in range(len(subreddit_list)):
        for i in range(len(year_list)):

            print('Subreddit: ' + subreddit_list[j] + ', year:' + str(year_list[i]))

            DATE_MIN = '{:d}-01-01'.format(year_list[i])
            DATE_MAX = '{:d}-01-01'.format(year_list[i]+1)
            df_sub = download_posts(subreddit_list[j], date_min=DATE_MIN, date_max = DATE_MAX, fields = fields)
            df_sub = update_praw(df_sub)
            str_save = folder + subreddit_list[j] + '_{:d}'.format(year_list[i]) + filetype
            df_sub.to_csv(str_save, index=False)

def load_years(subreddit, year_list, fields):
    
    df = pd.DataFrame(columns=fields)
    for year in year_list:
        str_load = 'data/' + subreddit + '_' + str(year) + '.gzip'
        df_temp = pd.read_csv(str_load, usecols=fields)
        df = df.append(df_temp, sort=True)
    df.reset_index(inplace=True,drop=True)

    return df

def plot_powerlaws(df, ax, xmax=None, xmin=None):

    import powerlaw

    # count = np.array(df[field])
    # fit_obj = powerlaw.Fit(count[count>0], xmax=xmax, xmin=xmin)
    # fit_obj.plot_pdf(color=color)
    # fit_obj.power_law.plot_pdf(color=color, linestyle='--')
    # str_alpha_ncomm = r'$\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)

    #Plots n_comments and score distributions
    n_comm = np.array(df['num_comments_updated'])
    score = np.array(df['score_updated'])

    fit_ncomm = powerlaw.Fit(n_comm[n_comm>0], xmax=1e3, xmin=1)
    str_label = r'comments, $\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)
    fit_ncomm.plot_pdf(ax=ax,color='b', **{'label':str_label})
    fit_ncomm.power_law.plot_pdf(ax=ax,color='b', linestyle='--')
   #str_alpha_ncomm = r'$\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)

    fit_score = powerlaw.Fit(score[score>0],  xmax=1e4, xmin=10)
    str_label = r'score, $\alpha$ = {:0.3f}'.format(fit_score.power_law.alpha)
    fit_score.plot_pdf(ax=ax,color='r', **{'label':str_label})
    fit_score.power_law.plot_pdf(ax=ax,color='r', linestyle='--')
    #str_alpha_score = r'$\alpha$ = {:0.3f}'.format(fit_score.power_law.alpha)

    plt.legend()

def plot_subscribers(df):
    pass

def download_csv(subreddit, save_file, date_max = "2020-01-31", date_min ="2010-01-01", auth_file = 'AUTH_user.json'):

    """Downloads all posts from a subreddit, continuously updating save_file.
    
    Args:
        subreddit (TYPE): subreddit name
        save_file (TYPE): place to save the csv file
        date_max (str, optional): newest date
        date_min (str, optional): Description
        auth_file (str, optional): location of the OAUTH2 file with authentication.
    """

    #PRAW Auth
    with open(auth_file) as json_file: 
        params = json.load(json_file) 

    #Opens praw session
    praw_session = praw.Reddit(client_id=params['client_id'], 
                     client_secret=params['client_secret'],
                     password=params['password'], 
                     user_agent=params['user_agent'],
                     username=params['username'])

    #Pushshift parameters
    base_url = "https://api.pushshift.io/reddit/submission/search/"
    params = {"subreddit": subreddit, "sort": "desc","sort_type": "created_utc", "size": 100, "fields":['author', "created_utc", 'id']}

    #Date timestamps
    timestamp_max = int(datetime.fromisoformat(date_max).timestamp())
    timestamp_min = int(datetime.fromisoformat(date_min).timestamp())
    params['before'] = timestamp_max 

    timestamp_downloaded = timestamp_max

    count = 0

    file_exists = False  if os.path.isfile(save_file) else True

    while timestamp_downloaded > timestamp_min:

        #Requests data and stores it in a dataframe
        try:
            response = requests.get(base_url,params=params)
            df = pd.DataFrame(response.json()['data'])
            #Gets timestamp of the earliest download, adds it to params
            timestamp_downloaded = df['created_utc'].min()
            
            #Gets dataframes id, updates then to 'full format'
            id_full = list(df['id'].apply(lambda x: 't3_' + x))
            subs = praw_session.info(fullnames = id_full)
            data_dict = {'score': [], 'comments':[], 'id':[]}

            for sub in subs:
                data_dict['score'].append(sub.score)
                data_dict['comments'].append(sub.num_comments)
                data_dict['id'].append(sub.id)

            df_praw = pd.DataFrame(data_dict)
            df_updated = pd.merge(left=df, right = df_praw, how='left',left_on='id',right_on='id')
            df_updated.to_csv(save_file, mode='a', index=False, header=file_exists)

            params['before'] = timestamp_downloaded

            count = count + 100
            if count % 100000 == 0:
                date_last = datetime.fromtimestamp(timestamp_downloaded).strftime('%y-%m-%d')
                print(datetime.now().strftime('%y/%m/%d %H:%M:%S')+'{:d} submissions done. Last date: '.format(count) + date_last)

        except:
            print(datetime.now().strftime('%y/%m/%d %H:%M:%S') + ': Pushshift error, trying again in 1s.')
            sleep(1)