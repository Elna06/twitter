import pandas as pd
import numpy as np
import csv
import datetime

tweets_raw = pd.read_csv('tweets.txt', sep="\t", header=None, iterator=True)
while 1:
    tweets = tweets_raw.get_chunk(30000)
    tweets.columns = ['tweets']
    tweets['len'] = tweets.tweets.apply(lambda x: len(x.split('|')))
    tweets[tweets.len < 4] = np.nan
    del tweets['len']
    tweets = tweets[tweets.tweets.notnull()]
    tweets['user'] = tweets.tweets.apply(lambda x: x.split('|')[0])
    tweets['geo'] = tweets.tweets.apply(lambda x: x.split('|')[1])
    tweets['timestamp'] = tweets.tweets.apply(lambda x: x.split('|')[2])
    tweets['tweet'] = tweets.tweets.apply(lambda x: x.split('|')[3])
    tweets['lat'] = tweets.geo.apply(lambda x: x.split(',')[0].replace('[',''))
    tweets['lon'] = tweets.geo.apply(lambda x: x.split(',')[1].replace(']',''))
    del tweets['tweets']
    del tweets['geo']
    tweets['lon'] = tweets.lon.apply(pd.to_numeric, errors='ignore')
    tweets['lat'] = tweets.lat.apply(pd.to_numeric, errors='ignore')
    tweets.to_csv('tweets.csv', mode='a', header=False,index=False)
    tweets.shape
    tweets.dtypes
    min_lon = 105.2857
    max_lon = 106.0202
    min_lat = 20.5641
    max_lat = 21.3853
    tweets = tweets[(tweets.lat.notnull()) & (tweets.lon.notnull())]
    tweets = tweets[(tweets.lon > min_lon) & (tweets.lon < max_lon) & (tweets.lat > min_lat) & (tweets.lat < max_lat)]
    tweets.shape
    l1=(tweets[['timestamp','lat','lon']].to_string(header=False, index=False)).split(" ")
    i=0
    while (i<len(l1)):
        if ((i-1)%10==0):
            if (l1[i]=="Sat" or l1[i]=="Sun"):
                if ((float((l1[i+3])[:2])) >= 6 and (float((l1[i+3])[:2])) <= 14):
                    with open('tweets_locationWE_matin','a') as file:
                        file.write(str(l1[i+7])+" "+str(l1[i+7+2]))
                elif ((float((l1[i+3])[:2])) >= 14 and (float((l1[i+3])[:2])) <= 23):
                    with open('tweets_locationWE_aprem','a') as file:
                        file.write(str(l1[i+7])+" "+str(l1[i+7+2]))
                else:
                    with open('tweets_locationWE_soir','a') as file:
                        file.write(str(l1[i+7])+" "+str(l1[i+7+2]))
            else:
                if ((float((l1[i+3])[:2])) >= 6 and (float((l1[i+3])[:2])) <= 14):
                    with open('tweets_locationSEM_matin','a') as file:
                        file.write(str(l1[i+7])+" "+str(l1[i+7+2]))
                elif ((float((l1[i+3])[:2])) >= 14 and (float((l1[i+3])[:2])) <= 23):
                    with open('tweets_locationSEM_aprem','a') as file:
                        file.write(str(l1[i+7])+" "+str(l1[i+7+2]))
                else:
                    with open('tweets_locationSEM_soir','a') as file:
                        file.write(str(l1[i+7])+" "+str(l1[i+7+2]))                   
        i+=1
