from psaw import PushshiftAPI


api=PushshiftAPI()
sub = list(api.search_comments(subreddit='wallstreetbets',limit=25))
for s in sub:
    print(s)