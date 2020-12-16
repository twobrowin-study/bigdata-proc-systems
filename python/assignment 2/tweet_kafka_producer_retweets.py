# -*- coding: utf-8 -*-

import tweepy
from kafka import KafkaProducer


"""

KAFKA PRODUCER INIT


"""

producer = KafkaProducer(bootstrap_servers="kafka:9092")
topic_name = "tweets-kafka"


"""

TWITTER API AUTHENTICATION


"""

consumer_token = ""
consumer_secret = ""
access_token = ""
access_secret = ""

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


"""

LISTENER TO MESSAGES FROM TWITTER


"""

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        if hasattr(status, "retweeted_status"):
            original_tweet = status.retweeted_status
            tweet_id = original_tweet.id_str
            user_screen_name = original_tweet.user.screen_name
            if hasattr(tweet_id, "extended_tweet"):
                text = status.extended_tweet.full_text
            else:
                text = status.text
            output = "{}\t{}\t{}".format(tweet_id, user_screen_name, text)
            print(output)
            producer.send(topic_name, output.encode("utf-8"))

"""

RUN PROCESSING


"""

# Create instance of custom listener
streamListener = StreamListener()

# Set stream for twitter api with custom listener
stream = tweepy.Stream(auth=api.auth, listener=streamListener)

# Users IDs
user_ids = ['285532415', '147964447', '34200559', '338960856', '200036850', '72525490', '20510157', '99918629']

# Start filtering messages
stream.filter(follow=user_ids)