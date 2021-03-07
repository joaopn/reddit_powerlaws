# -*- coding: utf-8 -*-
# @Author: joaopn
# @Date:   2021-03-02 00:49:46
# @Last Modified by:   joaopn
# @Last Modified time: 2021-03-07 16:18:35

import powerlaw as plw
import seaborn as sns
from reddit import pushshift
import matplotlib.pyplot as plt
import numpy as np

def powerlaw(data, ax = None, show_fit = True, title = None, xlabel = None):

    """Plots the probability distribution of data with a power-law fit
    
    Args:
        data (float): list/numpy array of observations
        ax (None, optional): ax to plot the distribution
        show_fit (bool, optional): whether to show the power-law fit
        title (None, optional): plot title
        xlabel (None, optional): plot xlabel
    """ 
    if ax is None:
        ax = plt.gca()

    #Plots data
    data_nonzero = data[data>0]
    pl_obj = plw.Fit(data_nonzero,xmin=1)
    str_label = r'N = {:0.0f}, R = {:0.1f}'.format(data_nonzero.size,np.sum(data_nonzero)/data_nonzero.size)
    pl_obj.plot_pdf(ax=ax, **{'label':str_label})

    if show_fit:
        str_label_fit = r'$\alpha$ = {:0.3f}'.format(pl_obj.power_law.alpha)
        pl_obj.power_law.plot_pdf(ax=ax,color='k', linestyle='--', **{'label':str_label_fit})

    #Beautifies plot
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Probability')
    ax.legend()

def powerlaws_df(df, ax, xmax=None, xmin=None):

    # count = np.array(df[field])
    # fit_obj = powerlaw.Fit(count[count>0], xmax=xmax, xmin=xmin)
    # fit_obj.plot_pdf(color=color)
    # fit_obj.power_law.plot_pdf(color=color, linestyle='--')
    # str_alpha_ncomm = r'$\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)

    #Plots n_comments and score distributions
    n_comm = np.array(df['num_comments_updated'])
    score = np.array(df['score_updated'])

    fit_ncomm = plw.Fit(n_comm[n_comm>0], xmax=1e3, xmin=1)
    str_label = r'comments, $\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)
    fit_ncomm.plot_pdf(ax=ax,color='b', **{'label':str_label})
    fit_ncomm.power_law.plot_pdf(ax=ax,color='b', linestyle='--')
   #str_alpha_ncomm = r'$\alpha$ = {:0.3f}'.format(fit_ncomm.power_law.alpha)

    fit_score = plw.Fit(score[score>0],  xmax=1e4, xmin=10)
    str_label = r'score, $\alpha$ = {:0.3f}'.format(fit_score.power_law.alpha)
    fit_score.plot_pdf(ax=ax,color='r', **{'label':str_label})
    fit_score.power_law.plot_pdf(ax=ax,color='r', linestyle='--')
    #str_alpha_score = r'$\alpha$ = {:0.3f}'.format(fit_score.power_law.alpha)

    plt.legend()

def subreddits_live(subreddit_list, date_min, date_max = None):
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
        df = df.append(pushshift.download_posts(subreddit, date_min, date_max, fields=None), sort=True)

    #Updates all using praw
    print('Updating scores and comments')
    df = pushshift.update_praw(df)

    #Plots scatterplots
    print('Plotting results')
    sns.scatterplot(data=df,x='score_updated',y='num_comments_updated', hue='subreddit')