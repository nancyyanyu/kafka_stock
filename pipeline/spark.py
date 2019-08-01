#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 17:53:01 2019

@author: yanyanyu
"""

import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row
from twitter_stream import TCP_IP, TCP_PORT,KEY_WORD
import seaborn as sns
import matplotlib.pyplot as plt
import time


def spark(TCP_IP,TCP_PORT,KEY_WORD):
    sc=SparkContext(appName="TwitterStreamming")
    sc.setLogLevel("ERROR")
    ssc=StreamingContext(sc,5)
    
    socket_stream = ssc.socketTextStream(TCP_IP,TCP_PORT)
    
    lines=socket_stream.window(300)
    df=lines.flatMap(lambda x:x.split(" "))  \
            .filter(lambda x:x.startswith("#"))  \
            .filter(lambda x:x!='#%s'%KEY_WORD)  
    
    def process(rdd):
        spark=SparkSession \
                .builder \
                .config(conf=rdd.context.getConf()) \
                .getOrCreate()
    
        rowRdd = rdd.map(lambda x: Row(word=x))
        wordsDataFrame = spark.createDataFrame(rowRdd)
    
        wordsDataFrame.createOrReplaceTempView("words")
        wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word order by 2 desc")       
        pd_df=wordCountsDataFrame.toPandas()
        
        plt.figure( figsize = ( 10, 8 ) )
        sns.barplot( x="total", y="word", data=pd_df.head(20))
        plt.show()
        
    df.foreachRDD(process)
    
    ssc.start()
    #time.sleep(900)
    #ssc.stop(stopSparkContext=True)
 
if __name__=="__main__":
    
    spark(TCP_IP,TCP_PORT,KEY_WORD)