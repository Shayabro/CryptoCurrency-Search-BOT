from psaw import PushshiftAPI
import datetime

#23:13:23

def get_timerange_comments():
        start_time = datetime.datetime(2021,7,5).timestamp()
        end_time = start_time + 24*3600
        print(f"fetching data from {str(datetime.datetime.fromtimestamp(start_time))} to {datetime.datetime.fromtimestamp(end_time)}")
        api=PushshiftAPI()
        comments = api.search_comments(after=start_time,before=end_time,subreddit='wallstreetbets',filter=['permalink','body','author','subreddit'])
        count = 0
        for comment in comments:
            print("hi")
            curr = comment.body.upper().count('$GME')
            if(curr>0):
                count+=curr
                print(f"found $GME at time {str(datetime.datetime.fromtimestamp(comment.created_utc))}. current counter={count}")
                print(comment.body)
                print("https://www.reddit.com"+comment.permalink)
                print(comment.author)
                print()
                print()
                print()

get_timerange_comments()