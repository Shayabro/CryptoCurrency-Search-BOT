from psaw import PushshiftAPI
import datetime
import threading
import config
import psycopg2
import psycopg2.extras

def get_timerange_comments(thread_id, currencies):
    print(f" thread id {thread_id} and currencies are {currencies}")
    while(True):
        #Pop a date from the shared dates list
        dates_lock.acquire()
        if len(dates)==0:
            dates_lock.release()
            return
        else:
            start_time = dates.pop()
        dates_lock.release()    

        #Communicating with the API
        end_time = start_time + 24*3600
        print(f"fetching data from {str(datetime.datetime.fromtimestamp(start_time))} to {datetime.datetime.fromtimestamp(end_time)}")
        api=PushshiftAPI()
        #comments = api.search_comments(after=start_time,before=end_time,subreddit='wallstreetbets',filter=['permalink','body','author','subreddit'])
        comments = api.search_comments(after=start_time,before=end_time,subreddit='CryptoCurrency',filter=['permalink','body','author','subreddit'])
        for comment in comments:
            count = {currency:0 for currency in currencies}
            words = comment.body.split()
            for word in words:
                if word in currencies:
                    count[word]+=1
                    comment_time= datetime.datetime.fromtimestamp(comment.created_utc)
                    comment_url= "https://reddit.com"+comment.permalink
                    comment_message=comment.body
                    print(f"found {word} in thread number {thread_id}. The subreddit is {comment.subreddit} and the time is {str(comment_time)}")
                    try:
                        db_lock.acquire()
                        cursor.execute("""
                            INSERT INTO mention (dt,stock_id,message,occurence,source,url) 
                            VALUES (%s,%s,%s,%d,%s,%s)
                        """, (comment_time,currencies[word],comment_message,count[word],comment.subreddit,comment_url))
                        connection.commit()
                        db_lock.release()
                    except Exception as e:
                        print(e)
                        connection.rollback()
                        db_lock.release()

def main():
    #Creating the relevant dates and threads
    threads = []
    for i in range(12):
        t = threading.Thread(target = get_timerange_comments, args=[i,currencies])
        threads.append(t)
        t.start()
    #awaiting for threads
    for t in threads:
        t.join()

def get_dates():
    dates = []
    curr = int(datetime.datetime(2021,5,25).timestamp())
    while(datetime.datetime.fromtimestamp(curr).month != 7):
        dates.append(curr)
        curr += 24*3600
    return dates

def get_currencies():
    connection = psycopg2.connect(host=config.DB_HOST, database = config.DB_NAME, user = config.DB_USER, password = config.DB_PASS)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("""SELECT * FROM stock""")
    currencies = {}
    for currency in cursor.fetchall():
        #currencies['$'+currency['symbol']]=currency['id']
        currencies[currency['symbol']]=currency['id']
    return currencies

#Global Enviroment
dates = get_dates()
currencies = get_currencies()
dates_lock = threading.Lock()
db_lock = threading.Lock()
connection = psycopg2.connect(host=config.DB_HOST, database = config.DB_NAME, user = config.DB_USER, password = config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
main()
