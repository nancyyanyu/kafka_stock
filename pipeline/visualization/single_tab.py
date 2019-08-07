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
import datetime
import pandas as pd
from math import pi
from random import choice
from pytz import timezone    
from bokeh.plotting import  figure,show
from bokeh.palettes import all_palettes,Set3
from bokeh.models import ColumnDataSource, Select,HoverTool,LinearAxis, LabelSet,Range1d,PreText,Div
from warehouse import CassandraStorage
from util.util import pandas_factory,symbol_list,splitTextToTriplet,prev_weekday
from util.config import path,timeZone

def read_company(symbol):
    with open(path+'visualization/company/{}.json'.format(symbol),'r') as f:
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
    stock_select=Select(value=symbol_list[0],options=symbol_list)
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
                                         company['financialData']['revenueGrowth']['fmt'],
                                         company['financialData']['currentRatio']['fmt'],
                                         company['financialData']['returnOnAssets']['fmt'],
                                         company['financialData']['returnOnEquity']['fmt'],
                                         company['financialData']['grossProfits']['fmt'],
                                         company['financialData']['quickRatio']['fmt'],
                                         company['financialData']['freeCashflow']['fmt'])

    update_summary(stock_select.value)
    # connect to Cassandra database
    database=CassandraStorage(symbol_list[0])
    database.session.row_factory = pandas_factory
    database.session.default_fetch_size = None
    
    query="SELECT * FROM {} WHERE time>'2015-01-01'  ALLOW FILTERING;".format('{}_historical'.format(symbol_list[0]))
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
    _,_,_,institution_df=read_company(symbol_list[0])
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

def stream_price():
    
    # connect to s&p500's database
    plot_symbol='^GSPC'
    database=CassandraStorage(plot_symbol)
    database.session.row_factory = pandas_factory
    database.session.default_fetch_size = None
    
#    if datetime.datetime.now(timezone('US/Eastern')).time()<datetime.time(9,30):
#        query_time=str(datetime.datetime.now().date())
    
    
    last_trading_day= datetime.datetime.now(timezone(timeZone)).date()
    
    query="SELECT * FROM {} WHERE time>='{}'  ALLOW FILTERING;".format(plot_symbol[1:]+'_tick',last_trading_day)
    rslt = database.session.execute(query, timeout=None)
    df = rslt._current_rows

        # wrangle timezone (Cassandra will change datetime to UTC time)
    trans_time=pd.DatetimeIndex(pd.to_datetime(df.time,unit='ms')).tz_localize('GMT').tz_convert('US/Pacific').to_pydatetime()
    trans_time=[i.replace(tzinfo=None) for i in trans_time]
    source= ColumnDataSource()
    
    # hover setting
    TOOLTIPS = [
            ("time", "@time{%F %T}"),
            ("close", "$@close"),
            ("volume","@volume")]
    formatters={
        'time'      : 'datetime'}
    hover = HoverTool(tooltips=TOOLTIPS,formatters=formatters,mode='vline')

    # create plot
    p = figure(title='S&P500 Realtime Price',
                plot_height=200,  
                tools="crosshair,save,undo,xpan,xwheel_zoom,ybox_zoom,reset", 
                x_axis_type="datetime", 
                y_axis_location="left")
    p.add_tools(hover)
    p.x_range.follow = "end"
    p.x_range.follow_interval = 1000000
    p.x_range.range_padding = 0

    # during trading
    if len(df)>0 \
        and datetime.datetime.now(timezone(timeZone)).time()<datetime.time(16,0,0) \
        and datetime.datetime.now(timezone(timeZone)).time()>datetime.time(9,30,0):
        # init source data to those already stored in Cassandra dataase - '{}_tick', so that streaming plot will not start over after refreshing
        source= ColumnDataSource(dict(time=list(trans_time),  
                                       close=list(df.close.values),
                                       volume=list(df.volume.values)))
        p.y_range = Range1d(min(source.data['close'])/1.005, max(source.data['close'])*1.005)
        p.extra_y_ranges = {"volumes": Range1d(start=min(source.data['volume'])*0.5, 
                                               end=max(source.data['volume'])*2)}
    # no trading history or not during trading hour
    else:
        source= ColumnDataSource(dict(time=[],  
                                       close=[],
                                       volume=[]))
    
        p.y_range = Range1d(0,1e4)
        p.extra_y_ranges = {"volumes": Range1d(start=0, 
                                               end=1e10)}
        
    p.line(x='time', y='close', alpha=0.2, line_width=3, color='blue', source=source)    
    p.add_layout(LinearAxis(y_range_name="volumes"), 'right')    
    p.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source)
    
    
    # get update data from a json file overwritter every ~18 seconds
    def _create_prices():
        with open(path+'cache/data.json','r') as f:
            dict_data = json.load(f)
        return float(dict_data['close']),dict_data['volume'],dict_data['time']
    
    # update function for stream plot
    def update():
        close,volume,time=_create_prices()
        new_data = dict(
            time=[datetime.datetime.strptime(time[:19], "%Y-%m-%d %H:%M:%S")],
            close=[close],
            volume=[volume]
        )
        #print(new_data)
        source.stream(new_data)
        #print ('update source data',str(time))
    return p,update

