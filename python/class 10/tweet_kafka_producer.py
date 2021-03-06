# -*- coding: utf-8 -*-

import tweepy
from tweepy.streaming import json
from kafka import KafkaProducer


"""

KAFKA PRODUCER INIT


"""

producer = KafkaProducer(bootstrap_servers="kafka:9092")
topic_name = "tweets-kafka"

if (producer.bootstrap_connected()):
    print ("Connected to bootstrap")
    producer.send(topic_name, "Hello from tweeter".encode("utf-8"))


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


class MoscowStreamListener(tweepy.StreamListener):

    def on_data(self, raw_data):

        data = json.loads(raw_data)

        if "extended_tweet" in data:
            text = data["extended_tweet"]["full_text"]

            #print(text)

            # put message into Kafka
            producer.send(topic_name, text.encode("utf-8"))
        else:
            if "text" in data:
                text = data["text"].lower()

                #print(data["text"])

                # put message into Kafka
                producer.send(topic_name, data["text"].encode("utf-8"))


"""

RUN PROCESSING


"""


# Create instance of custom listener
moscowStreamListener = MoscowStreamListener()

# Set stream for twitter api with custom listener
moscowStream = tweepy.Stream(auth=api.auth, listener=moscowStreamListener)

# Region that approximately corresponds to Moscow
region = [34.80, 49.87, 149.41, 74.13]

# Start filtering messages
moscowStream.filter(locations=region)