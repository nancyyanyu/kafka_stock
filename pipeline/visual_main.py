#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 16:48:28 2019

@author: yanyanyu
"""
import json
import datetime
import pandas as pd
from bokeh.models.widgets import Tabs
from warehouse import CassandraStorage
import util.util as util
from util.util import pandas_factory,symbol_list
from bokeh.models import ColumnDataSource, Panel
from bokeh.layouts import column, row, gridplot
from bokeh.plotting import curdoc, figure, show
from bokeh.models import LinearAxis, Range1d
from bokeh.models import HoverTool
from bokeh.palettes import all_palettes
from random import choice
from visualization.compare_tab import compare_tab
from visualization.candlestick import candlestick
from visualization.economy_tab import economy_tab
from pytz import timezone    



"""
Tab1-plot2: realtime AAPL price
"""


# connect to AAPL's database
plot2_symbol='^GSPC'
database2=CassandraStorage(plot2_symbol)
database2.session.row_factory = pandas_factory
database2.session.default_fetch_size = None

if datetime.datetime.now(timezone('US/Eastern')).time()<datetime.time(9,30):
    query_time=str(datetime.datetime.now().date())

query2="SELECT * FROM {} WHERE time>='{}'  ALLOW FILTERING;".format(plot2_symbol[1:]+'_tick',str(datetime.datetime.now().date()))
rslt = database2.session.execute(query2, timeout=None)
df = rslt._current_rows

# wrangle timezone (Cassandra will change datetime to UTC time)
trans_time=pd.DatetimeIndex(pd.to_datetime(df.time,unit='ms')).tz_localize('GMT').tz_convert('US/Pacific').to_pydatetime()

# init source data to those already stored in Cassandra dataase - 'aapl_tick', so that streaming plot will not start over after refreshing
source2= ColumnDataSource(dict(time=list(trans_time),  
                               close=list(df.close.values),
                               volume=list(df.volume.values)))
    
# hover setting
TOOLTIPS2 = [
        ("time", "@time{%F %T}"),
        ("close", "$@close"),
        ("volume","@volume")]
formatters2={
    'time'      : 'datetime'}
hover2 = HoverTool(tooltips=TOOLTIPS2,formatters=formatters2,mode='vline')
    
# create plot
p2 = figure(title='S&P500 Realtime Price',
            plot_height=200,  
            tools="crosshair,save,undo,xpan,xwheel_zoom,ybox_zoom,reset", 
            x_axis_type="datetime", 
            y_axis_location="left")
p2.add_tools(hover2)
p2.x_range.follow = "end"
p2.x_range.follow_interval = 1000000
p2.x_range.range_padding = 0
p2.line(x='time', y='close', alpha=0.2, line_width=3, color='blue', source=source2)

p2.y_range = Range1d(min(source2.data['close'])/1.005, max(source2.data['close'])*1.005)


p2.extra_y_ranges = {"volumes": Range1d(start=min(source2.data['volume'])*0.5, 
                                       end=max(source2.data['volume'])*2)}
p2.add_layout(LinearAxis(y_range_name="volumes"), 'right')

p2.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source2)


# get update data from a json file overwritter every ~18 seconds
def _create_prices():
    with open('./cache/data.json','r') as f:
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
    source2.stream(new_data)



"""
Tabs definition
    
"""

    

# Tab1's main funtion
def single_tab():
    # layout
    p,stock_select,summaryText,financialText,s=candlestick()
    #l=column(stock_select,p,row(summaryText,financialText,s),p2, width=1300)
    
    
    l=column(row(stock_select),gridplot([[p],[p2]], toolbar_location="right", plot_width=1300),row(summaryText,financialText,s))
    tab1 = Panel(child = l, title = 'Single Stock')
    return tab1



#query3="SELECT * FROM NEWS ORDER BY publishedAt;"
#rslt2 = database2.session.execute(query3, timeout=None)
#df2 = rslt2._current_rows

def update_news():
    pass




"""
Main

"""
tab1=single_tab()
tab2=compare_tab()
tab3=economy_tab()
dashboard = Tabs(tabs = [tab1,tab2,tab3])
curdoc().add_root(dashboard)
curdoc().add_periodic_callback(update, 6000)
curdoc().title = "Stock Visualization"



