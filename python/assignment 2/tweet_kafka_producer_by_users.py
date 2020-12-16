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


# Users IDs
user_ids = ['285532415', '147964447', '34200559', '338960856', '200036850', '72525490', '20510157', '99918629']

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        if (status.user.id_str in user_ids):
            user_screen_name = status.user.screen_name
            producer.send(topic_name, user_screen_name.encode("utf-8"))
            print("{}::\t{}".format(user_screen_name, status.text))

"""

RUN PROCESSING


"""

# Create instance of custom listener
streamListener = StreamListener()

# Set stream for twitter api with custom listener
stream = tweepy.Stream(auth=api.auth, listener=streamListener)

# Start filtering messages
stream.filter(follow=user_ids)