#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 23:53:27 2019

@author: yanyanyu
"""

import os
import json
import xmltodict
import locale
from locale import atof
import requests
import pandas as pd
from math import pi
from bokeh.io import show
from random import choice
from bokeh.plotting import  figure
from bokeh.layouts import column, row, gridplot
from bokeh.palettes import all_palettes,Set3,Viridis6
from bokeh.models import LogColorMapper,ColumnDataSource,HoverTool,Panel, Select,HoverTool,LinearAxis, LabelSet,Range1d,PreText,Div


def geomap():
    path='./visualization/bea/'
    list_d=os.listdir(path)
    bea_list=[i[:-4] for i in list_d]
    with open('./visualization/shapefile/statesGeo.json','r') as f:
        geoinfo=json.load(f)
    
    name_default='Real personal income'
    
    def read_bea(name):
        df=pd.read_csv(os.path.join(path,name+'.csv'))
        df=df[(df.GeoName.isin(geoinfo['state_name'])) & (df.TimePeriod==df.TimePeriod.max())]
        try:
            df.DataValue=df.DataValue.str.replace(',','').astype(float)
        except:
            df.DataValue=df.DataValue.astype(float)
            
        data=[df.loc[df.GeoName==i,'DataValue'].values[0] for i in geoinfo['state_name']]
        geoinfo['data']=data
        
        source=dict(x=geoinfo['state_lon'],
                    y=geoinfo['state_lat'],
                    name=geoinfo['state_name'],
                    rate=geoinfo['data'])
        return source
    
    def make_plot(name):
        source=ColumnDataSource(data=read_bea(name))
        color_mapper = LogColorMapper(palette=Viridis6)
        
        TOOLS = "pan,wheel_zoom,reset,hover,save"
        
        p = figure(plot_width=800,
            title=name, tools=TOOLS,active_scroll='wheel_zoom',
            x_axis_location=None, y_axis_location=None,
            tooltips=[
                ("Name", "@name"), (name, "@rate"), ("(Long, Lat)", "($x, $y)")
            ])
        p.grid.grid_line_color = None
        p.hover.point_policy = "follow_mouse"
        
        p.patches('x', 'y', source=source,
                  fill_color={'field': 'rate', 'transform': color_mapper},
                  fill_alpha=0.7, line_color="white", line_width=0.5)
        return p,source
    p,source=make_plot(name_default)






    def callback(attr,old,new):
        name=bea_select.value
        source.data=read_bea(name)
        p.title.text=name
        p.hover.tooltips=[
                ("Name", "@name"), (name, "@rate"), ("(Long, Lat)", "($x, $y)")
            ]

    bea_select=Select(value=name_default,options=bea_list)
    bea_select.on_change('value', callback)


    return p,bea_select


def economy_tab():
    path='./visualization/economy/'
    list_d=os.listdir(path)
    indicator_list=[' '.join(i[:-4].split('_')) for i in list_d]
    
    name1='Real_Gross_Domestic_Product.csv'
    name2='All_Employees_Total_Nonfarm_Payrolls.csv'
    name3='Consumer_Price_Index_for_All_Urban_Consumers.csv'
    name4='Effective_Federal_Funds_Rate.csv'
    
    def read_economy(name):
        df=pd.read_csv(os.path.join(path,name))
        df.columns=['DATE','DATA']
        df.DATE=pd.to_datetime(df.DATE).dt.date
        return df
    
        
    def make_plot(name):
        
        df=read_economy(name) 
        source = ColumnDataSource(data=df)
        title=' '.join(name[:-4].split('_'))
        hover = HoverTool(tooltips=[("date", "@DATE{%F}"),('data', "@DATA")],formatters={'DATE': 'datetime'},mode='vline')
        
        e=figure(title=title,plot_height=200, 
                       tools="crosshair,save,undo,xpan,xwheel_zoom,xbox_zoom,reset", 
                       active_scroll='xwheel_zoom',
                       x_axis_type="datetime")
        e.add_tools(hover)
        e.line('DATE','DATA',color='black', source=source)
        return e,source,title
    e1,source1,title1=make_plot(name1)
    e2,source2,title2=make_plot(name2)
    e3,source3,title3=make_plot(name3)
    e4,source4,title4=make_plot(name4)
    
    
    def callback3(attr,old,new):
        indicator1,indicator2,indicator3,indicator4=economy_select1.value,economy_select2.value,economy_select3.value,economy_select4.value
        name1,name2,name3,name4=(['_'.join(i.split())+'.csv' for i in [indicator1,indicator2,indicator3,indicator4]])
        df1,df2,df3,df4=read_economy(name1),read_economy(name2),read_economy(name3),read_economy(name4)   
        source1.data,source2.data,source3.data,source4.data = df1.to_dict('list'),df2.to_dict('list'),df3.to_dict('list'),df4.to_dict('list')
    
        e1.title.text,e2.title.text,e3.title.text,e4.title.text =indicator1,indicator2,indicator3,indicator4
        e1.y_range.start,e2.y_range.start,e3.y_range.start,e4.y_range.start=min(source1.data['DATA'])/1.05,min(source2.data['DATA'])/1.05,min(source3.data['DATA'])/1.05,min(source4.data['DATA'])/1.05
        e1.y_range.end,e2.y_range.end,e3.y_range.end,e4.y_range.end=max(source1.data['DATA'])*1.05,max(source2.data['DATA'])*1.05,max(source3.data['DATA'])*1.05,max(source4.data['DATA'])*1.05
    
        
    economy_select1=Select(value=title1,options=indicator_list)
    economy_select2=Select(value=title2,options=indicator_list)
    economy_select3=Select(value=title3,options=indicator_list)
    economy_select4=Select(value=title4,options=indicator_list)
    economy_select1.on_change('value', callback3)
    economy_select2.on_change('value', callback3)
    economy_select3.on_change('value', callback3)
    economy_select4.on_change('value', callback3)
    
    
    
    p,bea_select=geomap()
    
    
    
    
    l=column(gridplot([[column(economy_select1,e1),column(economy_select2,e2)],
                        [column(economy_select3,e3),column(economy_select4,e4)],
                        [column(bea_select,p)]], 
                        toolbar_location="right", plot_width=1300))
    tab3 = Panel(child = l, title = 'Economy')

    return tab3
