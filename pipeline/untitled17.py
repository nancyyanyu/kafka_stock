#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 00:10:41 2019

@author: yanyanyu
"""

from lxml import html  
import requests
from time import sleep
import json
from collections import OrderedDict
import datetime 
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium import webdriver
import requests
import urllib
import json
import requests
import os


def parse(ticker):
    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker,ticker)
    browser = webdriver.Chrome()
    browser.get(url)
    
    print ("Parsing %s"%(url))
    parser = html.fromstring(browser.page_source)
    summary_table = parser.xpath('//div[contains(@data-test,"summary-table")]//tr')
    summary_data = OrderedDict()
    other_details_json_link = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{0}?formatted=true&lang=en-US&region=US&modules=summaryProfile%2CfinancialData%2CrecommendationTrend%2CupgradeDowngradeHistory%2Cearnings%2CdefaultKeyStatistics%2CcalendarEvents&corsDomain=finance.yahoo.com".format(ticker)
    summary_json_response=requests.get(other_details_json_link)

    try:
        json_loaded_summary =  json.loads(summary_json_response.text)
        y_Target_Est = json_loaded_summary["quoteSummary"]["result"][0]["financialData"]["targetMeanPrice"]['raw']
        earnings_list = json_loaded_summary["quoteSummary"]["result"][0]["calendarEvents"]['earnings']
        eps = json_loaded_summary["quoteSummary"]["result"][0]["defaultKeyStatistics"]["trailingEps"]['raw']
        datelist = []
        for i in earnings_list['earningsDate']:
            datelist.append(i['fmt'])
        earnings_date = ' to '.join(datelist)
        for table_data in summary_table:
            raw_table_key = table_data.xpath('.//td[contains(@class,"C(black)")]//text()')
            raw_table_value = table_data.xpath('.//td[contains(@class,"Ta(end)")]//text()')
            table_key = ''.join(raw_table_key).strip()
            table_value = ''.join(raw_table_value).strip()
            summary_data.update({table_key:table_value})
        summary_data.update({'1y Target Est':y_Target_Est,'EPS (TTM)':eps,'Earnings Date':earnings_date,'ticker':ticker,'url':url})
        print('get!'+str(datetime.datetime.now()))
        browser.close()
        
        return summary_data
    except:
        print ("Failed to parse json response")
        return {"error":"Failed to parse json response"}
    
		
if __name__=="__main__":
	
    ticker = 'AMZN'
    print ("Fetching data for %s"%(ticker)+str(datetime.datetime.now()))

    for i in range(2):
        scraped_data = parse(ticker)
    print ("Writing data to output file"+str(datetime.datetime.now()))
    
    
    
    