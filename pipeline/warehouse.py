#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 15:00:55 2019

@author: yanyanyu
"""


# 
import ast
import time
from util.util import string_to_float,symbol_list
from util.config import config
from kafka import KafkaConsumer
from cassandra.cluster import Cluster,NoHostAvailable
from producer import get_intraday_data,get_historical_data


# =============================================================================
# Step 1: run zookeeper_starter.sh to start zookeeper
# Step 2: run kafka_starter.sh to start Kafka
# Step 3: run cassandra_starter.sh to start Cassandra
# Step 4: run producer.py to start sending data through Kafka
# =============================================================================


class CassandraStorage(object):
    
    """
    Kafka consumer reads the message and store the received data in Cassandra database
    
    """
    def __init__(self,symbol):
        if symbol=='^GSPC':
            self.symbol='GSPC'
        else:
            self.symbol=symbol
        
        self.key_space=config['key_space']
        
        # init a Cassandra cluster instance
        cluster = Cluster()
        
        # start Cassandra server before connecting       
        try:
            self.session = cluster.connect()
        except NoHostAvailable:
            print("Fatal Error: need to connect Cassandra server")            
        else:
            self.create_table()
        
        
    def create_table(self):
        """
        create Cassandra table of stock if not exist
        :return: None
        
        """
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % config['key_space'])
        self.session.set_keyspace(self.key_space) 
        
        # create table for intraday data
        self.session.execute("CREATE TABLE IF NOT EXISTS {} ( \
                                    	TIME timestamp,           \
                                    	SYMBOL text,              \
                                    	OPEN float,               \
                                    	HIGH float,               \
                                    	LOW float,                \
                                    	CLOSE float,              \
                                    VOLUME float,             \
                                    PRIMARY KEY (SYMBOL,TIME));".format(self.symbol))
        
        # create table for historical data
        self.session.execute("CREATE TABLE IF NOT EXISTS {} ( \
                                    	TIME timestamp,           \
                                    	SYMBOL text,              \
                                    	OPEN float,               \
                                    	HIGH float,               \
                                    	LOW float,                \
                                    	CLOSE float,              \
                                    ADJUSTED_CLOSE float,     \
                                    VOLUME float,             \
                                    dividend_amount float,    \
                                    split_coefficient float,  \
                                    PRIMARY KEY (SYMBOL,TIME));".format(self.symbol+'_historical')) 
        
        self.session.execute("CREATE TABLE IF NOT EXISTS {} ( \
                                    	TIME timestamp,           \
                                    	SYMBOL text,              \
                                    	OPEN float,               \
                                    	HIGH float,               \
                                    	LOW float,                \
                                    	CLOSE float,              \
                                    VOLUME float,             \
                                    last_trading_day text,    \
                                    previous_close float,     \
                                    change float,             \
                                    change_percent float,     \
                                    PRIMARY KEY (SYMBOL,TIME));".format(self.symbol+'_tick')) 
        self.session.execute("CREATE TABLE IF NOT EXISTS NEWS ( \
                                    DATE date, \
                                    	publishedAt timestamp,           \
                                    	TITLE text,              \
                                    	SOURCE text,               \
                                    	description text,               \
                                    	url text, PRIMARY KEY (DATE,publishedAt) \
                                ) \
                                 WITH CLUSTERING ORDER BY (publishedAt ASC);")  \

    def kafka_consumer(self):
        """
        initialize a Kafka consumer 
        :return: None
        
        """
        self.consumer1 = KafkaConsumer(
                                    config['topic_name1'],
                                    bootstrap_servers=config['kafka_broker'])    
        self.consumer2 = KafkaConsumer(
                                    config['topic_name2'],
                                    bootstrap_servers=config['kafka_broker']) 
        
        self.consumer3 = KafkaConsumer(
                                    'news',
                                    bootstrap_servers=config['kafka_broker']) 
    
    def historical_to_cassandra(self,price,intraday=False):
        """
        store historical data to Cassandra database
            :primary key: time,symbol
        :return: None

        """
        if intraday==False:
            for dict_data in price:
                for key in ['open', 'high', 'low', 'close', 'volume','adjusted_close','dividend_amount','split_coefficient']:
                    dict_data[key]=string_to_float(dict_data[key])
                query="INSERT INTO {}(time, symbol,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient) VALUES ('{}','{}',{},{},{},{},{},{},{},{});".format(self.symbol+'_historical', dict_data['time'],
                                dict_data['symbol'],dict_data['open'], \
                                dict_data['high'],dict_data['low'],dict_data['close'],dict_data['adjusted_close'],dict_data['volume'],dict_data['dividend_amount'],dict_data['split_coefficient']) 
                self.session.execute(query)
                print("Stored {}\'s historical data at {}".format(dict_data['symbol'],dict_data['time']))
        else:
            for dict_data in price:
                for key in ['open', 'high', 'low', 'close', 'volume']:
                    dict_data[key]=string_to_float(dict_data[key])
                    
                query="INSERT INTO {}(time, symbol,open,high,low,close,volume) VALUES ('{}','{}',{},{},{},{},{});".format(self.symbol, dict_data['time'],
                                dict_data['symbol'],dict_data['open'], \
                                dict_data['high'],dict_data['low'],dict_data['close'],dict_data['volume']) 
                                
                        
                self.session.execute(query)
                print("Stored {}\'s full length intraday data at {}".format(dict_data['symbol'],dict_data['time']))

            
    def stream_to_cassandra(self):
        """
        store streaming data of 1min frequency to Cassandra database
            :primary key: time,symbol
        :return: None
        
        """
        
        for msg in self.consumer1:
            # decode msg value from byte to utf-8
            dict_data=ast.literal_eval(msg.value.decode("utf-8"))
            
            # transform price data from string to float
            for key in ['open', 'high', 'low', 'close', 'volume']:
                dict_data[key]=string_to_float(dict_data[key])
                
            query="INSERT INTO {}(time, symbol,open,high,low,close,volume) VALUES ('{}','{}',{},{},{},{},{});".format(self.symbol, dict_data['time'],
                            dict_data['symbol'],dict_data['open'], \
                            dict_data['high'],dict_data['low'],dict_data['close'],dict_data['volume']) 
                            
                    
            self.session.execute(query)
            print("Stored {}\'s min data at {}".format(dict_data['symbol'],dict_data['time']))

    
    def tick_stream_to_cassandra(self):
        """
        store streaming data of second frequency to Cassandra database
            :primary key: time,symbol
        :return: None
        
        """
        for msg in self.consumer2:
            # decode msg value from byte to utf-8
            dict_data=ast.literal_eval(msg.value.decode("utf-8"))
            
            # transform price data from string to float
            for key in ['open', 'high', 'low', 'close', 'volume','previous_close','change']:
                dict_data[key]=string_to_float(dict_data[key])
                
            dict_data['change_percent']=float(dict_data['change_percent'].strip('%'))/100.
            
            query="INSERT INTO {}(time, symbol,open,high,low,close,volume,previous_close,change,change_percent, last_trading_day) VALUES ('{}','{}',{},{},{},{},{},{},{},{},'{}');".format(self.symbol+'_tick', dict_data['time'],
                            dict_data['symbol'],dict_data['open'], \
                            dict_data['high'],dict_data['low'],dict_data['close'],dict_data['volume'],dict_data['previous_close'],dict_data['change'],dict_data['change_percent'],dict_data['last_trading_day']) 
                            
                    
            self.session.execute(query)
            print("Stored {}\'s tick data at {}".format(dict_data['symbol'],dict_data['time']))

    def news_to_cassandra(self):
        for msg in self.consumer3:
            dict_data=ast.literal_eval(msg.value.decode("utf-8"))
            publishtime=dict_data['publishedAt'][:10]+' '+dict_data['publishedAt'][11:19]
            try:
                dict_data['description']=dict_data['description'].replace('\'','@@')
            except:
                pass
            query="INSERT INTO NEWS (date,publishedat,source,title,description,url) VALUES ('{}','{}','{}','{}','{}','{}');" \
                            .format(publishtime[:10],publishtime,
                                    dict_data['source']['name'],
                                    dict_data['title'].replace('\'','@@'),
                                    dict_data['description'],
                                    dict_data['url']) 
            self.session.execute(query)
            
            
            #print("Stored news '{}' at {}".format(dict_data['title'],dict_data['publishedAt']))


    def delete_table(self,table_name):
        self.session.execute("DROP TABLE {}".format(table_name))
        
def main_realtime(symbol='^GSPC',tick=True):
    """
    main funtion to store realtime data; recommend to set tick=False, as getting tick data would cause rate limiting error from API 
    """
    database=CassandraStorage(symbol)
    database.kafka_consumer()
    if tick==True:
        database.tick_stream_to_cassandra()
    else:
        database.stream_to_cassandra()
            
        
def main_realtime_news():
    database=CassandraStorage('AAPL')
    database.kafka_consumer()
    database.news_to_cassandra()

        
def main_aftertradingday():
    """
    main function to update recent trading day's daily price (mainly for updating the adjusted close price), and 1min frequency price(to fill in empty data points caused by errors)
    """
    for symbol in symbol_list[:]:
        value_daily = get_historical_data(symbol=symbol,outputsize='full')
        value_min, _ = get_intraday_data(symbol=symbol,outputsize='full',freq='1min')
    
        database=CassandraStorage(symbol)
        database.kafka_consumer()
        
        
        database.historical_to_cassandra(value_min,True)
        database.historical_to_cassandra(value_daily,False)
        time.sleep(15)

    
    
if __name__=="__main__":
    
    # update daily and 1 min freq data of all stocks
    #main_aftertradingday()
    #main_realtime(symbol='^GSPC',tick=True)
    #main_realtime_news()
    
    pass
