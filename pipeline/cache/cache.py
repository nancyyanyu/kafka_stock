#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 16:52:34 2019

@author: yanyanyu
"""
import ast
from kafka import KafkaConsumer
from util.config import config,path
import json
import os



consumer = KafkaConsumer(
                        config['topic_name2'],
                        bootstrap_servers=config['kafka_broker'])    

for msg in consumer:
    dict_data=ast.literal_eval(msg.value.decode("utf-8"))
    with open(os.path.join(path,'./data.json'),'w') as f:
        json.dump(dict_data,f)
    print(str(dict_data['time']))




