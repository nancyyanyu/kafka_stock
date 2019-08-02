#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 12:57:07 2019

@author: yanyanyu

: get stock price every minute

"""

import json
import requests
import schedule
import datetime
import numpy as np
from pytz import timezone    
from util.config import config
from kafka import KafkaProducer
from newsapi import NewsApiClient
# =============================================================================
# Step 1: run zookeeper_starter.sh to start zookeeper
# Step 2: run kafka_starter.sh to start Kafka
# Step 3: run cassandra_starter.sh to start Cassandra
# Step 4: run producer.py to start sending data through Kafka
# =============================================================================

#logging.basicConfig(level=logging.DEBUG)

def get_historical_data(symbol='AAPL',outputsize='full'):
    """
    :param outputsize: (str) default to 'full' to get 20 years historical data; 
                                        'compact' to get the most recent 100 days' historical data
    :return: (dict) latest minute's stock price information 
        e.g.:
            {"symbol": 'AAPL',
             "time"  : '2019-07-26 16:00:00',
             'open'  : 207.98,
             'high'  : 208.0,
             'low'   : 207.74,
             'close' : 207.75,
             'adjusted_close': 207.74,
             'volume': 354454.0,
             'dividend_amount': 0.0,
             'split_coefficient': 1.0
            }
    
    """
    
    # get data using AlphaAvantage's API
    
    url="https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={}&outputsize={}&interval=1min&apikey={}".format(symbol,outputsize,config['api_key'])

    req=requests.get(url)
    if req.status_code==200:
        raw_data=json.loads(req.content)
        try:
            price=raw_data['Time Series (Daily)']

        except KeyError:
            print(raw_data)
            exit()
    
        rename={'symbol':'symbol',
                'time':'time',
                '1. open':'open',
                '2. high':'high',
                '3. low':'low',
                '4. close':'close',
                '5. adjusted close':'adjusted_close',
                '6. volume':'volume',
                '7. dividend amount':'dividend_amount',
                '8. split coefficient':'split_coefficient'}
        
        for k,v in price.items():
            v.update({'symbol':symbol,'time':k})
        price=dict((key,dict((rename[k],v) for (k,v) in value.items())) for (key, value) in price.items())
        price=list(price.values())
        print("Get {}/'s historical data today.".format(symbol))
        
        return price
    
    
    
def get_intraday_data(symbol='AAPL',outputsize='compact',freq='1min'):
    """
    :param outputsize: (str) 'compact' returns only the latest 100 data points in the intraday time series; 
                             'full' returns the full-length intraday time series. 
    :return: (dict) latest minute's stock price information 
        e.g.:
            {"symbol": 'AAPL',
             "time"  : '2019-07-26 16:00:00',
             'open'  : 207.98,
             'high'  : 208.0,
             'low'   : 207.74,
             'close' : 207.75,
             'volume': 354454.0
            }
    
    """
    
    
    # get data using AlphaAvantage's API
    url="https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&outputsize={}&interval={}&apikey={}".format(symbol,outputsize,freq,config['api_key'])
    req=requests.get(url)
    
    # if request success
    if req.status_code==200:
        raw_data=json.loads(req.content)
        try:
            price=raw_data['Time Series (1min)']
            meta=raw_data['Meta Data']

        except KeyError:
            print(raw_data)
            exit()
        time_zone=meta['6. Time Zone']
        
        if outputsize=='compact':
            #only get the most recent price
            last_price=price[max(price.keys())]
            
            #organize data to dict
            value={"symbol":symbol,
                   "time":meta['3. Last Refreshed'],
                   "open":last_price['1. open'],
                   "high":last_price['2. high'],
                   "low":last_price['3. low'],
                   "close":last_price['4. close'],
                   "volume":last_price['5. volume']}
            
            print('Get {}\'s latest min data at {}'.format(symbol,datetime.datetime.now(timezone(time_zone))))
        
        # if outputsize='full' get full-length intraday time series
        else:
            rename={'symbol':'symbol',
                    'time':'time',
                    '1. open':'open',
                    '2. high':'high',
                    '3. low':'low',
                    '4. close':'close',
                    '5. volume':'volume'}
            
            for k,v in price.items():
                v.update({'symbol':symbol,'time':k})
            price=dict((key,dict((rename[k],v) for (k,v) in value.items())) for (key, value) in price.items())
            value=list(price.values())
            print('Get {}\'s full length intraday data.'.format(symbol))

            
    # if request failed, return a fake data point
    else:
        time_zone='US/Eastern'
        print('  Failed: Cannot get {}\'s data at {}:{} '.format(symbol,datetime.datetime.now(timezone(time_zone)),req.status_code))
        value={"symbol":'None',
               "time":'None',
               "open":0.,
               "high":0.,
               "low":0.,
               "close":0.,
               "volume":0.}
        
    return value,time_zone

def check_trading_hour(data_time):
    if data_time.time()<datetime.time(9,30):
        last_day=data_time-datetime.timedelta(days=1)
        data_time=datetime.datetime(last_day.year,last_day.month,last_day.day,16,0,0)
        
    elif data_time.time()>datetime.time(16,0):
        data_time=datetime.datetime(data_time.year,data_time.month,data_time.day,16,0,0)
    return data_time

def get_tick_intraday_data(symbol='AAPL'):
    
    # get data using AlphaAvantage's API
    time_zone='US/Eastern'
    url="https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={}&apikey={}".format(symbol,config['api_key2'])
    req=requests.get(url)
    data_time=datetime.datetime.now(timezone(time_zone))
    data_time=check_trading_hour(data_time)
    
    # if request success
    if req.status_code==200:
        raw_data=json.loads(req.content)
        try:
            price=raw_data['Global Quote']

        except KeyError:
            print(raw_data)
            exit()
        
        #organize data to dict
        value={"symbol":symbol,
               "time":str(data_time)[:19],
               "open":price['02. open'],
               "high":price['03. high'],
               "low":price['04. low'],
               "close":price['05. price'],
               "volume":price['06. volume'],
               "last_trading_day":price['07. latest trading day'],
               "previous_close":price['08. previous close'],
               "change":price['09. change'],
               "change_percent":price['10. change percent']}
        
        print('Get {}\'s latest tick data at {}'.format(symbol,data_time))
        
            
    # if request failed, return a fake data point
    else:
        print('  Failed: Cannot get {}\'s data at {}:{} '.format(symbol,data_time))
        value={"symbol":symbol,
               "time":str(data_time)[:19],
               "open":0.,
               "high":0.,
               "low":0.,
               "close":0.,
               "volume":0.,
               "last_trading_day":'',
               "previous_close":0.,
               "change":0.,
               "change_percent":0.}
    return value, time_zone

def get_news():
    
    api='23e4c7e51a9a49d39dc4e7261305dd02'
    newsapi = NewsApiClient(api_key=api)
    top_headlines = newsapi.get_top_headlines(q='rate',country='us',category='business',page_size=30,language='en')
    return top_headlines    
    
    
    



    
def kafka_producer(producer,symbol='^GSPC',tick=False):
    """
    :param producer: (KafkaProducer) an instance of KafkaProducer with configuration written in config.py
    :return: None
    
    """
    # get data

    if tick==False:
        value,time_zone=get_intraday_data(symbol,outputsize='compact',freq='1min')
        
        now_timezone=datetime.datetime.now(timezone(time_zone))
        # transform ready-to-send data to bytes, record sending-time adjusted to the trading timezone
        producer.send(topic=config['topic_name1'], value=bytes(str(value), 'utf-8'))
        print("Sent {}'s min data at {}".format(symbol,now_timezone))
    # send tick data
    else:
        value,time_zone=get_tick_intraday_data(symbol)
        
        now_timezone=datetime.datetime.now(timezone(time_zone))
        # transform ready-to-send data to bytes, record sending-time adjusted to the trading timezone
        producer.send(topic=config['topic_name2'], value=bytes(str(value), 'utf-8'))
        print("Sent {}'s tick data at {}".format(symbol,now_timezone))


def kafka_producer_news(producer):
    news=get_news()
    if news['articles']!=[]:
        for article in news['articles']:
            now_timezone=datetime.datetime.now(timezone('US/Eastern'))
            producer.send(topic='news', value=bytes(str(article), 'utf-8'))
            print("Sent economy news : {}".format(now_timezone))
    



    
def kafka_producer_fake(producer,symbol):
    """
    send fake data to test visualization
    :param producer: (KafkaProducer) an instance of KafkaProducer with configuration written in config.py
    :return: None
    """
    close=3500
    close=close+np.random.uniform(-1,1)*50
    value={"symbol":symbol,
           "time":str(datetime.datetime.now(timezone('US/Eastern'))),
           "open":close+np.random.uniform(-1,1)*50,
           "high":close+np.random.uniform(0,1)*50,
           "low":close+np.random.uniform(-1,0)*50,
           "close":close,
           "volume":1316167424+np.random.uniform(-1,1)*50}
    producer.send(topic=config['topic_name2'], value=bytes(str(value), 'utf-8'))
    print("Sent {}'s fake data.".format(symbol))
    
    
    
    
    
if __name__=="__main__":

    # init an instance of KafkaProducer
    producer = KafkaProducer(bootstrap_servers=config['kafka_broker'])
    #kafka_producer(producer)

    # schedule to send data every minute
    #schedule.every().minute.at(":00").do(kafka_producer,producer,'^GSPC',False)
    schedule.every(18).seconds.do(kafka_producer,producer,'^GSPC',True)
    #schedule.every(2).seconds.do(kafka_producer_fake,producer,'^GSPC')
    schedule.every(2).minutes.do(kafka_producer_news,producer)
    while True:
        schedule.run_pending()
    
    
    pass




