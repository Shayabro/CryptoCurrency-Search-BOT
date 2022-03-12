import config
import psycopg2
import psycopg2.extras
import requests
import time,datetime
import asyncpg, asyncio
import sys

async def write_to_db(pool, currency_id, currency_prices):
    try:
        async with pool.acquire() as connection:
            params = [(currency_id, datetime.datetime.utcfromtimestamp(bar['t'] / 1000.0), round(bar['o'], 2), round(bar['h'], 2), round(bar['l'], 2), round(bar['c'], 2), bar['v']) for bar in currency_prices]
            await connection.copy_records_to_table('stock_price', records=params)

    except Exception as e:
        print("Unable to Write to DB a price of {} due to {}.".format(currency_id, e.__class__))


async def writes_to_db(pool,response):
    try:
        # schedule aiohttp requests to run concurrently for all symbols
        ret = await asyncio.gather(*[write_to_db(pool, currency_id, response[currency_id]) for currency_id in response])
        print("Finalized all. Returned  list of {} outputs.".format(len(ret)))
    except Exception as e:
        print(e)


async def write_data(response):
    # create database connection pool
    pool = await asyncpg.create_pool(user=config.DB_USER, password=config.DB_PASS, database=config.DB_NAME, host=config.DB_HOST, command_timeout=60)
    # get a connection
    await writes_to_db(pool, response)
 

def get_prices():
    COUNT=0 #count for not passing the limitation of the POLYGON API free version
    connection = psycopg2.connect(host=config.DB_HOST, database = config.DB_NAME, user = config.DB_USER, password = config.DB_PASS)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("""SELECT * FROM stock""")
    #Creating the http requests for the POLYGON API
    id_urls = {}
    id_to_curr = {}
    for currency in cursor.fetchall():
        id = currency['id']
        id_to_curr[currency['id']]=currency['symbol']
        id_urls[id] = f"https://api.polygon.io/v2/aggs/ticker/X:{id_to_curr[id]}USD/range/1/minute/2021-06-01/2021-06-30?apiKey={config.POLYGON_API_KEY}&limit=50000"
    #Executing the requests and storing the returned value
    response = {}
    for id in id_urls:
        print("_______________________")
        print(id_to_curr[id])
        COUNT +=1
        if COUNT==5:
            time.sleep(60)
            COUNT=0
        res = requests.get(id_urls[id])
        res = res.json()
        if res['status']=='OK':
            if 'results' in res.keys():
                print("GOOD!")
                response[id]=res['results']
        elif res['status']=='ERROR':
            print("BAD!")
    #Closing The connection to DB and returning the data for each CryptoCurrency
    connection.commit()
    print("_______________________")
    print("Got all data from the API, Bye Bye")
    return response


#Main
start = time.time()
#asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
response = get_prices()
asyncio.get_event_loop().run_until_complete(write_data(response))
end = time.time()
print("Took {} seconds.".format(end - start))

