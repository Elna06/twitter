import config
import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from urllib3.exceptions import ProtocolError
import sys


Hanoi = [105.2857, 20.5641, 106.0202, 21.3853]

file =  open('tweets.txt', 'a', encoding='utf8')

class listener(StreamListener):

    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        try:
            decoded = json.loads(data)
        except Exception as e:
            print(e) #we don't want the listener to stop
            return True

        if decoded.get('geo') is not None:
            location = decoded.get('geo').get('coordinates')
        else:
            location = '[,]'
        text = decoded['text'].replace('\n',' ')
        user = '@' + decoded.get('user').get('screen_name')
        created = decoded.get('created_at')
        tweet = '%s|%s|%s|%s\n' % (user,location,created,text)

        file.write(tweet)
        print(tweet)
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    print('Starting')

    auth = OAuthHandler(config.consumer_key, config.consumer_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)
    twitterStream = Stream(auth, listener())
    twitterStream.filter(locations=Hanoi)

