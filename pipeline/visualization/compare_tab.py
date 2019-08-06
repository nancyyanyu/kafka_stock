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


def compare_plot():

    stats = PreText(text='', width=500)
    corr = PreText(text='', width=500)
    def _ma(series,n):
        return series.rolling(window=n).mean()
    
    # connect to Cassandra database
    def make_dataset(start='2014-01-01'):
        df=pd.DataFrame()
        for comp in symbol_list:
            database=CassandraStorage(comp)
            database.session.row_factory = pandas_factory
            database.session.default_fetch_size = None
            
            query="SELECT * FROM {} WHERE time>'{}' ALLOW FILTERING;".format(database.symbol+'_historical',start)
            rslt = database.session.execute(query, timeout=None)
            df_comp = rslt._current_rows
            df_comp['ma5']=_ma(df_comp.adjusted_close,5)
            df_comp['ma10']=_ma(df_comp.adjusted_close,10)
            df_comp['ma30']=_ma(df_comp.adjusted_close,30)
            df=df.append(df_comp)
        return df
    
    
    df_all=make_dataset(start='2014-01-01')
    df_init=df_all[df_all.symbol==symbol_list[0]]
    source=ColumnDataSource(data= df_init.to_dict('list'))
    
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
    
    # create plot
    p = figure(title='{} (Click on legend entries to hide the corresponding lines)'.format(symbol_list[0]),
                plot_height=300,  
                tools="crosshair,save,undo,xpan,xwheel_zoom,ybox_zoom,reset", 
                active_scroll='xwheel_zoom',
                x_axis_type="datetime", 
                y_axis_location="left")
    
    p.add_tools(hover)
    palte=all_palettes['Set2'][8]
    p.line('time', 'adjusted_close', alpha=1.0, line_width=2, color=palte[3], legend='Adjusted Close',source=source)
    p.line('time', 'ma5', line_width=1, color=palte[0], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA5',source=source)
    p.line('time', 'ma10', line_width=1, color=palte[1], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA10',source=source)
    p.line('time', 'ma30', line_width=1, color=palte[2], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA30',source=source)
    p.y_range = Range1d(min(source.data['adjusted_close'])*0.3, max(source.data['adjusted_close'])*1.05)
    p.extra_y_ranges = {"volumes": Range1d(start=min(source.data['volume'])/2, 
                                           end=max(source.data['volume'])*2)}
    p.add_layout(LinearAxis(y_range_name="volumes"), 'right')
    p.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source)
    p.legend.location = "top_left"
    p.legend.click_policy="hide"
        
    def callback(attr,old,new):
        symbol1, symbol2=compare_select1.value, compare_select2.value
        df_init1=df_all[df_all.symbol==symbol1]
        df_init2=df_all[df_all.symbol==symbol2]

        source.data.update( df_init1.to_dict('list'))
        p.title.text =symbol1+' (Click on legend entries to hide the corresponding lines)'
        p.y_range.start=min(source.data['adjusted_close'])*0.3
        p.y_range.end=max(source.data['adjusted_close'])*1.05
        p.extra_y_ranges['volumes'].start=min(source.data['volume'])/2.
        p.extra_y_ranges['volumes'].end=max(source.data['volume'])*2.

        source2.data.update( df_init2.to_dict('list'))
        p2.title.text =symbol2+' (Click on legend entries to hide the corresponding lines)'
        p2.y_range.start=min(source2.data['adjusted_close'])*0.3
        p2.y_range.end=max(source2.data['adjusted_close'])*1.05
        p2.extra_y_ranges['volumes'].start=min(source2.data['volume'])/2.
        p2.extra_y_ranges['volumes'].end=max(source2.data['volume'])*2.

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
    
    update_stat(symbol_list[0],symbol_list[1])
    compare_select1=Select(value=symbol_list[0],options=symbol_list)
    compare_select1.on_change('value', callback)
    
    
    
    
    """
    Tab2-plot2: 
        
    """
    
    df_init2=df_all[df_all.symbol==symbol_list[1]]
    source2=ColumnDataSource(data= df_init2.to_dict('list'))
    
    # create plot
    # hover setting
    p2 = figure(title='{}  (Click on legend entries to hide the corresponding lines)'.format(symbol_list[1]),plot_height=300,  
                tools="crosshair,save,undo,xpan,xwheel_zoom,ybox_zoom,reset", 
                active_scroll='xwheel_zoom',
                x_axis_type="datetime", 
                y_axis_location="left")
    
    p2.add_tools(hover)
    p2.line('time', 'adjusted_close', alpha=1.0, line_width=2, color=palte[4], legend='Adjusted Close',source=source2)
    p2.line('time', 'ma5', line_width=1, color=palte[5], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA5',source=source2)
    p2.line('time', 'ma10', line_width=1, color=palte[6], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA10',source=source2)
    p2.line('time', 'ma30', line_width=1, color=palte[7], alpha=0.8, muted_color='navy', muted_alpha=0.1, legend='MA30',source=source2)

    p2.y_range = Range1d(min(source2.data['adjusted_close'])*0.3, max(source2.data['adjusted_close'])*1.05)

    
    p2.extra_y_ranges = {"volumes": Range1d(start=min(source2.data['volume'])/2, 
                                           end=max(source2.data['volume'])*2)}
    p2.add_layout(LinearAxis(y_range_name="volumes"), 'right')
    p2.vbar('time', width=3,top='volume', color=choice(all_palettes['Set2'][8]),alpha=0.5, y_range_name="volumes",source=source2)


    p2.legend.location = "top_left"
    p2.legend.click_policy="hide"
    
    compare_select2=Select(value=symbol_list[1],options=symbol_list)
    compare_select2.on_change('value', callback)
    
    widget=column(compare_select1,compare_select2)
    
    return p,p2,widget,stats,corr



