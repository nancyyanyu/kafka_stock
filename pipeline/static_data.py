#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 15:31:46 2019

@author: yanyanyu
"""
import requests
import json
import datetime
import pandas_datareader.data as web
from util.util import symbol_list


def get_yahoo():
    modules = [
    	'assetProfile', 'balanceSheetHistory', 'balanceSheetHistoryQuarterly', 'calendarEvents',
    	'cashflowStatementHistory', 'cashflowStatementHistoryQuarterly', 'defaultKeyStatistics', 'earnings',
    	'earningsHistory', 'earningsTrend', 'financialData', 'fundOwnership', 'incomeStatementHistory',
    	'incomeStatementHistoryQuarterly', 'indexTrend', 'industryTrend', 'insiderHolders', 'insiderTransactions',
    	'institutionOwnership', 'majorDirectHolders', 'majorHoldersBreakdown', 'netSharePurchaseActivity', 'price', 'quoteType',
    	'recommendationTrend', 'secFilings', 'sectorTrend', 'summaryDetail', 'summaryProfile', 'symbol', 'upgradeDowngradeHistory',
    	'fundProfile', 'topHoldings', 'fundPerformance',
    ]
    
    
    for symbol in symbol_list:
        if symbol!='^GSPC':
            url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{}?formatted=true&lang=en-US&region=US&modules={}&corsDomain=finance.yahoo.com".format(symbol,'%2C'.join(modules))
            req=requests.get(url)
            result=json.loads(req.text)["quoteSummary"]['result'][0] 
            with open('./visualization/company/{}.json'.format(symbol),'w') as f:
                json.dump(result,f)
    
def get_economy():
    start = datetime.datetime(2010, 1, 1)
    end = datetime.datetime.now()
    keys=['GDPC1','CPIAUCSL','INDPRO','DGS10','UNRATE','PAYEMS','FEDFUNDS','DTB3','DGS5','DGS10','DGS30','GFDEBTN']
    names=['Real_Gross_Domestic_Product','Consumer_Price_Index_for_All_Urban_Consumers',
           'Industrial_Production_Index','Treasury_Constant_MaturityRate_(10_Year)',
           'Civilian_Unemployment_Rate','All_Employees_Total_Nonfarm_Payrolls',
           'Effective_Federal_Funds_Rate','Treasury_Bill_Secondary_Market_Rate_(3_Month)',
           'Treasury_Constant_Maturity_Rate_(5_Year)','Treasury_Constant_Maturity_Rate_(10_Year)',
           'Treasury_Constant_Maturity_Rate_(30_Year)','Federal_Debt_Total_Public_Debt']
    
    for i in range(len(keys)):
        
        df=web.DataReader(keys[i], 'fred', start, end)
        df.to_csv('./visualization/economy/{}.csv'.format(names[i]))

    return 

