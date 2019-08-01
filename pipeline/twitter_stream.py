#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 17:44:45 2019

@author: yanyanyu
"""


import twitter
import socket
import json


TCP_IP = "localhost"
TCP_PORT = 9876
KEY_WORD = ['#']

def twt_app(TCP_IP,TCP_PORT):
    with open('./OAuth.json','r') as f:
        oauth=json.load(f)
    consumer_key=oauth['consumer_key']
    consumer_secret=oauth['consumer_secret']
    access_token=oauth['access_token']
    access_token_secret=oauth['access_token_secret']
    
    api = twitter.Api(consumer_key=consumer_key,
                      consumer_secret=consumer_secret,
                      access_token_key=access_token,
                      access_token_secret=access_token_secret,
                      sleep_on_rate_limit=True)
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(10)
    print("Waiting for TCP connection...")
    
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    return conn,api

if __name__=="__main__":
    try:
        conn,api=twt_app(TCP_IP,TCP_PORT)
    except:
        pass
    for line in api.GetStreamFilter(track=KEY_WORD,languages=['en']):
        conn.send( line['text'].encode('utf-8') )
        print(line['text'])
        print()







