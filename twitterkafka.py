from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from kafka.client import KafkaClient
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
from datetime import timedelta

#kafka = KafkaClient("localhost:9092")
Topic_name = 'test'
#Variables that contains the user credentials to access Twitter API
access_token = 
access_token_secret = 
consumer_key = 
consumer_secret = 
producer = KafkaProducer(bootstrap_servers='localhost:9092')
#specify the variable paths of five columns
#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data);
        # print type(data)
        # for tweet in text:
        if tweet.get('text'): 
            # print tweet['text']
            text = tweet['text'].encode('utf-8')
            print text
            #producer = SimpleProducer(kafka)
            producer.send(Topic_name, text)
            #producer.send_messages(Topic_name, data)
        sleep(1)
        return True

    def on_error(self, status):
        print status

    
if __name__ == '__main__':
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
 
    #This line filter Twitter Streams to capture data by the keywords: 
    stream.filter(track=['Trump'])

	



















