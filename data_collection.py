import tweepy
import json
from multiprocessing.dummy import Pool as ThreadPool
import time
import datetime

from pymongo import MongoClient


consumer_key = 'MYUSqjYtGuazxofljlSGZdUJi'
consumer_secret = 'wntBFMsyxXR60De2sfeUx7PorfKv4lxzPXQ3gO03UAN6qMOpbf'
access_token = '441815365-A7PkddHTgwQsVDm7oJvRJXhwfVhg0iTeYaWPgEsn'
access_secret = 'y6pTYWKX0c7ueXW5CntwvGAH7sBkHQrGKt86YxMuuOJkH'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)


api = tweepy.API(auth)
trends = ["BlackFriday","blackfriday","BLACKFRIDAY"]
places = api.geo_search(query="USA", granularity="country")


client = MongoClient()
db = client.Twitterdata
tweet_coll = db.Final_Data

def limit_handled(cursor):
    print ("Came here")
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print ("Received Rate Limit Error. Sleeping for 15 minutes , Inside tweets call " + str(datetime.datetime.now()+datetime.timedelta(0,900)))
            time.sleep(15 * 60)
        except tweepy.TweepError:
            print ("Received Rate Limit Error. Sleeping for 15 minutes , Inside tweets call " + str(datetime.datetime.now()+datetime.timedelta(0,900)))
            time.sleep(15 * 60)

# def trends_place(woeid):
#     try:
#         tplace=api.trends_place(woeid)
#         for trend in tplace[0]["trends"]:
#             for status in limit_handled(tweepy.Cursor(api.search, q=trend["name"]).items()):
#                 print json.dumps(status._json)
#                 tweet_coll.insert_one(json.loads(json.dumps(status._json)))
#         print "Finished querying tweets"
#     except tweepy.RateLimitError:
#         print "Received Rate Limit Error. Sleeping for 15 minutes " + str(datetime.datetime.now()+datetime.timedelta(0,900))
#         time.sleep(15 * 60)
#         trends_place(woeid)

def tweets(trend):
    try:
        for status in limit_handled(tweepy.Cursor(api.search, q=trend, until='2018-11-26', geocode='39.8,-95.583068847656,2500km').items()):
            twt = status.user.location
            if twt != "":
                print (twt +"-->" + trend)
                #print status.entities.get('hashtags')
                status._id = status.id
                del status.id
                tweet_coll.insert_one(json.loads(json.dumps(status._json)))
        print ("Finished querying tweets")
    except tweepy.RateLimitError:
        print ("Received Rate Limit Error. Sleeping for 15 minutes " + str(
            datetime.datetime.now() + datetime.timedelta(0, 900)))
        time.sleep(15 * 60)

# world_trends = api.trends_available()
# for trends in world_trends:
#     wtrends.add(trends["woeid"])

if __name__ == '__main__':
    pool = ThreadPool(len(trends))
    pool.map(tweets,trends)


# https://twitter.com/statuses/ID should work
