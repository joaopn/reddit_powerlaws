# reddit_powerlaws
Power-law analysis (number of comments, score) of [Reddit](https://www.reddit.com/). 
Uses both the [pushshift.io](https://pushshift.io/) API (to get thread information) and the Reddit API (to update data).

## Authentication
Reddit requires AUTH2 authentication to access their API, the information which must be put in the `AUTH.json` file. Instructions on how to authenticate are available [here](https://www.reddit.com/wiki/api).