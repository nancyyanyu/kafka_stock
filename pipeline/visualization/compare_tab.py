#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 11:39:36 2019

@author: yanyanyu
"""
import pandas as pd
from random import choice
from bokeh.plotting import  figure
from bokeh.palettes import all_palettes
from bokeh.layouts import column, row, gridplot
from bokeh.models import LinearAxis, Range1d,HoverTool,ColumnDataSource, Panel,PreText, Select
from warehouse import CassandraStorage
from util.util import pandas_factory,symbol_list


def compare_tab():

    stats = PreText(text='', width=500)
    corr = PreText(text='', width=500)
    def _ma(series,n):
        return series.rolling(window=n).mean()
    
    # connect to Cassandra database
    def make_dataset(start='2014-01-01'):
        df=pd.DataFrame()
        for comp in symbol_list:
            database3=CassandraStorage(comp)
            database3.session.row_factory = pandas_factory
            database3.session.default_fetch_size = None
            
            query="SELECT * FROM {} WHERE time>'{}' ALLOW FILTERING;".format(database3.symbol+'_historical',start)
            rslt = database3.session.execute(query, timeout=None)
            df_comp = rslt._current_rows
            df_comp['ma5']=_ma(df_comp.adjusted_close,5)
            df_comp['ma10']=_ma(df_comp.adjusted_close,10)
            df_comp['ma30']=_ma(df_comp.adjusted_close,30)
            df=df.append(df_comp)
        return df
    
    
    df_all=make_dataset(start='2014-01-01')
    df_init=df_all[df_all.symbol=='AAPL']
    source3=ColumnDataSource(data= df_init.to_dict('list'))
    
    # hover setting
    TOOLTIPS3 = [
            ("time", "@time{%F}"),
            ("adjusted close", "$@adjusted_close"),
             ("close", "$@close"),
            ("open", "$@open"),
            ("high", "$@high"),
            ("low", "$@low"),
            ("volume","@volume")]
    formatters3={
        'time'      : 'datetime'}
    hover3 = HoverTool(tooltips=TOOLTIPS3,formatters=formatters3,mode='vline')
    
    # create plot
    
    p3 = figure(title='AAPL (Click on legend entries to hide the corresponding lines)',
                plot_height=300,  
                tools="crosshair,save,undo,xpan,xwheel_zoom,ybox_zoom,reset", 
                active_scroll='xwheel_zoom',
                x_axis_type="datetime", 
                y_axis_location="left")
    
    p3.add_tools(hover3)
    palte=all_palettes['Set2'][8]
    p3.line('time', 'adjusted_close', alpha=1.0, line_width=2, color=palte[3], legend='Adjusted Close',source=source3)
    p3.line('time', 'ma5', line_width=1, color=palte[0], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA5',source=source3)
    p3.line('time', 'ma10', line_width=1, color=palte[1], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA10',source=source3)
    p3.line('time', 'ma30', line_width=1, color=palte[2], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA30',source=source3)

    p3.y_range = Range1d(min(source3.data['close'])*0.3, max(source3.data['close'])*1.05)

    
    p3.extra_y_ranges = {"volumes": Range1d(start=min(source3.data['volume'])/2, 
                                           end=max(source3.data['volume'])*2)}
    p3.add_layout(LinearAxis(y_range_name="volumes"), 'right')
    p3.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source3)



    p3.legend.location = "top_left"
    p3.legend.click_policy="hide"
    
    
    #p3.line('time', 'adjusted_close', alpha=0.5, line_width=1, color='black', source=benchmark)
    
    def callback2(attr,old,new):
        symbol1, symbol2=compare_select1.value, compare_select2.value
        df_init1=df_all[df_all.symbol==symbol1]
        df_init2=df_all[df_all.symbol==symbol2]

        source3.data.update( df_init1.to_dict('list'))
        p3.title.text =symbol1+' (Click on legend entries to hide the corresponding lines)'
        p3.y_range.start=min(source3.data['close'])*0.3
        p3.y_range.end=max(source3.data['close'])*1.05
        p3.extra_y_ranges['volumes'].start=min(source3.data['volume'])/2.
        p3.extra_y_ranges['volumes'].end=max(source3.data['volume'])*2.

        source4.data.update( df_init2.to_dict('list'))
        p4.title.text =symbol2+' (Click on legend entries to hide the corresponding lines)'
        p4.y_range.start=min(source4.data['close'])*0.3
        p4.y_range.end=max(source4.data['close'])*1.05
        p4.extra_y_ranges['volumes'].start=min(source4.data['volume'])/2.
        p4.extra_y_ranges['volumes'].end=max(source4.data['volume'])*2.

        update_stat(symbol1,symbol2)
    
        
    
    def update_stat(symbol1,symbol2):
        des=pd.DataFrame()
        des[symbol1]=df_all[df_all.symbol==symbol1]['adjusted_close']
        des[symbol2]=df_all[df_all.symbol==symbol2]['adjusted_close']
        des[symbol1+'_return']=des[symbol1].pct_change()
        des[symbol2+'_return']=des[symbol2].pct_change()
        stats.text = "Statistics \n"+str(des.describe())
        
        correlation=des[[symbol1+'_return',symbol2+'_return']].corr()
        corr.text="Correlation \n"+str(correlation)
    
    update_stat('AAPL','GOOG')
    compare_select1=Select(value='AAPL',options=symbol_list)
    compare_select1.on_change('value', callback2)
    
    
    
    
    """
    Tab2-plot2: 
        
    """
    
    df_init2=df_all[df_all.symbol=='GOOG']
    source4=ColumnDataSource(data= df_init2.to_dict('list'))
    
    # create plot
    # hover setting
    p4 = figure(title='GOOG  (Click on legend entries to hide the corresponding lines)',plot_height=300,  
                tools="crosshair,save,undo,xpan,xwheel_zoom,ybox_zoom,reset", 
                active_scroll='xwheel_zoom',
                x_axis_type="datetime", 
                y_axis_location="left")
    
    p4.add_tools(hover3)
    p4.line('time', 'adjusted_close', alpha=1.0, line_width=2, color=palte[4], legend='Adjusted Close',source=source4)
    p4.line('time', 'ma5', line_width=1, color=palte[5], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA5',source=source4)
    p4.line('time', 'ma10', line_width=1, color=palte[6], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA10',source=source4)
    p4.line('time', 'ma30', line_width=1, color=palte[7], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA30',source=source4)

    p4.y_range = Range1d(min(source3.data['close'])*0.3, max(source4.data['close'])*1.05)

    
    p4.extra_y_ranges = {"volumes": Range1d(start=min(source4.data['volume'])/2, 
                                           end=max(source4.data['volume'])*2)}
    p4.add_layout(LinearAxis(y_range_name="volumes"), 'right')
    p4.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source4)


    p4.legend.location = "top_left"
    p4.legend.click_policy="hide"
    
    compare_select2=Select(value='GOOG',options=symbol_list)
    compare_select2.on_change('value', callback2)
    
    widget=column(compare_select1,compare_select2)
    
    l=column(row(widget,stats,corr),gridplot([[p3],[p4]], toolbar_location="right", plot_width=1300))
    tab2=Panel(child = l, title = 'Two Stocks Comparison')
    return tab2



