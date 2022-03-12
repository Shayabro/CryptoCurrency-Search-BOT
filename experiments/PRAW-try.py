import praw
from praw.models import MoreComments
import config
import datetime

reddit = praw.Reddit(client_id = config.CLIENT_ID, client_secret = config.SECRET_KEY, user_agent='myAPI/0.0.1')
print(reddit.read_only)

subreddit = reddit.subreddit("wallstreetbets")

start_time = datetime.datetime(2021,7,5).timestamp()
end_time = start_time + 24*3600

for submission in subreddit:
    print(submission.title)

