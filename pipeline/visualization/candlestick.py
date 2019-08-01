#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 11:47:47 2019

@author: yanyanyu
"""

"""
Tab1-plot1: candlestick

"""
import json
import pandas as pd
from math import pi
from random import choice
from bokeh.plotting import  figure,show
from bokeh.palettes import all_palettes,Set3
from bokeh.models import ColumnDataSource, Select,HoverTool,LinearAxis, LabelSet,Range1d,PreText,Div
from warehouse import CassandraStorage
from util.util import pandas_factory,symbol_list,splitTextToTriplet


def read_company(symbol):
    with open('./visualization/company/{}.json'.format(symbol),'r') as f:
        company=json.load(f)
    companyOfficers=company['assetProfile']['companyOfficers']
    
    officerString=''
    for officer in companyOfficers:
        officerString+=str('<br> &nbsp&nbsp&nbsp&nbsp&nbsp'+officer['name']+' - '+officer['title']) 
    buzzsummary='\n'.join(splitTextToTriplet('.'.join(company['summaryProfile']['longBusinessSummary'].split('.')[:3]),8))
    
    institutionOwnership=company['institutionOwnership']['ownershipList']
    institution_list=[]
    for institution in institutionOwnership:
        institution_list.append([institution['organization'],institution['position']['raw'],institution['pctHeld']['fmt']])
    institution_df=pd.DataFrame(institution_list,columns=['organization','position','pctHeld'])
    institution_df['organization']=[i.split(',')[0] for i in institution_df['organization']]
    return company,buzzsummary,officerString,institution_df


def candlestick():
    if '^GSPC' in symbol_list:
        symbol_list.remove('^GSPC')
    stock_select=Select(value='AAPL',options=symbol_list)
    summaryText = Div(text="",width=400)
    financialText=Div(text="",width=180)
    
    def update_summary(symbol):
        company,buzzsummary,officerString,institution_df=read_company(symbol)
        summaryText.text ="""<b><p style="color:blue;">Overview: </p></b>
                             <b>Company:</b> {}<br>
                             <b>Address:</b> {} <br>
                             <b>City:</b> {} <br>
                             <b>State:</b> {} <br>
                             <b>Website:</b> <a href="{}">{}</a> <br>
                             <b>Industry:</b> {} <br>
                             <b>Sector:</b> {} <br>
                             <b>Company Officers:</b> {} <br>                             
                             <b>Summary:</b> {} <br>""".format(company['price']['longName'],
                                                     company['summaryProfile']['address1'],
                                                     company['summaryProfile']['city'],
                                                     company['summaryProfile']['state'],
                                                     company['summaryProfile']['website'],
                                                     company['summaryProfile']['website'],
                                                     company['summaryProfile']['industry'],
                                                     company['summaryProfile']['sector'],
                                                     officerString,
                                                     buzzsummary)
        financialText.text="""<b><p style="color:blue;">Financial: </p></b>
                              <b>Recommendation: {}</b> <br>
                              <b>Enterprise Value:</b> {} <br>
                              <b>Profit Margins:</b> {} <br>
                              <b>Beta:</b> {} <br>
                              <b>EBITDA:</b> {} <br>
                              <b>Total Debt:</b> {} <br>
                              <b>Total Revenue:</b> {}<br>
                              <b>DebtToEquity:</b> {}<br>
                              <b>Earnings Growth:</b> {} <br>
                              <b>Revenue Growth:</b> {} <br>
                              <b>Current Ratio:</b> {} <br>
                              <b>ROE:</b> {} <br>
                              <b>ROA:</b> {} <br>
                              <b>Gross Profits:</b> {} <br>
                              <b>Quick Ratio:</b> {} <br>
                              <b>Free Cashflow:</b> {} <br>
                              """.format(company['financialData']['recommendationKey'].upper(),
                                         company['defaultKeyStatistics']['enterpriseValue']['fmt'],
                                         company['defaultKeyStatistics']['profitMargins']['fmt'],
                                         company['defaultKeyStatistics']['beta']['fmt'],
                                         company['financialData']['ebitda']['fmt'],
                                         company['financialData']['totalDebt']['fmt'],
                                         company['financialData']['totalRevenue']['fmt'],
                                         company['financialData']['debtToEquity']['fmt'],
                                         company['financialData']['earningsGrowth']['fmt'],
                                         company['financialData']['revenueGrowth']['fmt'],
                                         company['financialData']['earningsGrowth']['fmt'],
                                         company['financialData']['currentRatio']['fmt'],
                                         company['financialData']['returnOnAssets']['fmt'],
                                         company['financialData']['returnOnEquity']['fmt'],
                                         company['financialData']['grossProfits']['fmt'],
                                         company['financialData']['quickRatio']['fmt'],
                                         company['financialData']['freeCashflow']['fmt'])

    update_summary(stock_select.value)
    # connect to Cassandra database
    database=CassandraStorage('AAPL')
    database.session.row_factory = pandas_factory
    database.session.default_fetch_size = None
    
    query="SELECT * FROM {} WHERE time>'2015-01-01'  ALLOW FILTERING;".format('AAPL_historical')
    rslt = database.session.execute(query, timeout=None)
    df = rslt._current_rows
    
    # create color list
    color=df.close>df.open
    color=color.replace(True,'green')
    color=color.replace(False,'red')

    # set data source
    source = ColumnDataSource(data=dict(close=list(df.close.values),
                                        adjusted_close=list(df.adjusted_close.values),
                                        open=list(df.open.values),
                                        high=list(df.high.values),
                                        low=list(df.low.values),
                                        volume=list(df.volume.values),
                                        time=list(df.time.dt.date.values),
                                        color=list(color.values)))
    
    

    # hover setting
    TOOLTIPS = [
            ("time", "@time{%F}"),
            ("adjusted close", "$@adjusted_close"),
            ("close", "$@close"),
            ("open", "$@open"),
            ("high", "$@high"),
            ("low", "$@low"),
            ("volume","@volume")]
    formatters={
        'time'      : 'datetime'}
    hover = HoverTool(tooltips=TOOLTIPS,formatters=formatters,mode='vline')
    
    # create figure
    p = figure(title='{} Candlestick'.format(stock_select.value),plot_height=400, 
               tools="crosshair,save,undo,xpan,xwheel_zoom,xbox_zoom,reset", 
               active_scroll='xwheel_zoom',
               x_axis_type="datetime")  
    p.add_tools(hover)
    


    p.line('time', 'close', alpha=0.2, line_width=1, color='navy', source=source)
    p.segment('time', 'high', 'time', 'low', line_width=1,color="black", source=source)
    p.segment('time', 'open', 'time', 'close', line_width=3, color='color', source=source)
    p.y_range = Range1d(min(source.data['close'])*0.3, max(source.data['close'])*1.05)

    
    p.extra_y_ranges = {"volumes": Range1d(start=min(source.data['volume'])/2, 
                                           end=max(source.data['volume'])*2)}
    p.add_layout(LinearAxis(y_range_name="volumes"), 'right')
    p.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source)

    p.xaxis.axis_label = 'Time'
    
    # set data source
    _,_,_,institution_df=read_company('AAPL')
    source_ins = ColumnDataSource(data=dict(organization=list(institution_df.organization.values),
                                            pctHeld=list(institution_df.pctHeld.values),
                                            position=list(institution_df.position.values),
                                            color=Set3[12][:len(institution_df)]))
    s1=figure(x_range=source_ins.data['organization'],plot_height=300,plot_width=700,title='Institution Ownership')
    s1.vbar(x='organization', top='position', width=0.8, color='color', source=source_ins)
    s1.xaxis.major_label_orientation = pi/7
    labels = LabelSet(x='organization', y='position', text='pctHeld', level='glyph',
              x_offset=-15, y_offset=-10, source=source_ins, render_mode='canvas',text_font_size="8pt")
    s1.add_layout(labels)
    # callback funtion for Select tool 'stock_select'
    def callback(attr,old,new):
        symbol=stock_select.value
        _,_,_,institution=read_company(symbol)

        if symbol=='S&P500':
            symbol='^GSPC'
        database=CassandraStorage(symbol)
        database.session.row_factory = pandas_factory
        database.session.default_fetch_size = None
        if symbol=='^GSPC':
            symbol='GSPC'
        query="SELECT * FROM {} WHERE time>'2015-01-01'  ALLOW FILTERING;".format(symbol+'_historical')
        rslt = database.session.execute(query, timeout=None)
        df = rslt._current_rows
    
        color=df.close>df.open
        color=color.replace(True,'green')
        color=color.replace(False,'red')
        
        # update source data 
        source.data=dict(close=list(df.close.values),
                                        adjusted_close=list(df.adjusted_close.values),
                                        open=list(df.open.values),
                                        high=list(df.high.values),
                                        low=list(df.low.values),
                                        volume=list(df.volume.values),
                                        time=list(df.time.dt.date.values),
                                        color=list(color.values))
        source_ins.data=dict(organization=list(institution.organization.values),
                                        pctHeld=list(institution.pctHeld.values),
                                        position=list(institution.position.values),
                                        color=Set3[12][:len(institution)])
        
        p.title.text=symbol+' Candlestick'
        p.y_range.start=min(source.data['close'])*0.3
        p.y_range.end=max(source.data['close'])*1.05
        p.extra_y_ranges['volumes'].start=min(source.data['volume'])/2.
        p.extra_y_ranges['volumes'].end=max(source.data['volume'])*2.

        s1.x_range.factors=source_ins.data['organization']
        update_summary(symbol)
        
    stock_select.on_change('value', callback)
    
    return p,stock_select,summaryText,financialText,s1

#candlestick()


