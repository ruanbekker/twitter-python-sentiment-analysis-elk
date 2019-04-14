import json
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch
from config import *

es = Elasticsearch(['es.domain.com'], http_auth=('user', 'password'), scheme='http', port=9200)

class TweetStreamListener(StreamListener):
    def on_data(self, data):
        dict_data = json.loads(data)
        #print(dict_data["user"]["screen_name"])
        tweet = TextBlob(dict_data["text"])
        #print(tweet.sentiment.polarity)
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        print(dict_data['user']['screen_name'], tweet.sentiment)
        if 'retweeted_status' in dict_data.keys():
            # add text and sentiment info to elasticsearch
            es.index(index="sentiment",
                doc_type="test-type",
                body={
                    "timestamp": datetime.now(),
                    "created_at": dict_data["created_at"],
                    "user_followers": dict_data["user"]["followers_count"],
                    "user_statuses": dict_data["user"]["statuses_count"],
                    "user_geo_enabled": dict_data["user"]["geo_enabled"],
                    "geo_location": dict_data["coordinates"],
                    "hashtags": dict_data["entities"]["hashtags"],
                    "author": dict_data["user"]["screen_name"],
                    "retweet_info": dict_data["retweeted_status"]["user"],
                    "date": dict_data["created_at"],
                    "message": dict_data["text"],
                    "polarity": tweet.sentiment.polarity,
                    "subjectivity": tweet.sentiment.subjectivity,
                    "sentiment": sentiment})
            return True
        else:
            es.index(index="sentiment",
                doc_type="test-type",
                body={
                    "timestamp": datetime.now(),
                    "created_at": dict_data["created_at"],
                    "user_followers": dict_data["user"]["followers_count"],
                    "user_statuses": dict_data["user"]["statuses_count"],
                    "user_geo_enabled": dict_data["user"]["geo_enabled"],
                    "geo_location": dict_data["coordinates"],
                    "hashtags": dict_data["entities"]["hashtags"],
                    "author": dict_data["user"]["screen_name"],
                    "date": dict_data["created_at"],
                    "message": dict_data["text"],
                    "polarity": tweet.sentiment.polarity,
                    "subjectivity": tweet.sentiment.subjectivity,
                    "sentiment": sentiment})
            return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, listener)
    stream.filter(track=['lingard'])
