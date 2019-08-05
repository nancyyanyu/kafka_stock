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
from bokeh.layouts import column, row, gridplot,layout
from bokeh.plotting import curdoc, figure, show
from bokeh.models import LinearAxis, Range1d
from bokeh.models import HoverTool
from bokeh.palettes import all_palettes
from random import choice
from visualization.compare_tab import compare_plot
from visualization.single_tab import candlestick,stream_price
from visualization.economy_tab import geomap,economy_plot,stream_news
from pytz import timezone    


"""TAB 1"""
# layout    
p1,stock_select,summaryText,financialText,s=candlestick()
p2,update=stream_price()
l1=column(row(stock_select),
          gridplot([[p1],[p2]], toolbar_location="right", plot_width=1300),
          row(summaryText,financialText,s))
tab1 = Panel(child = l1, title = 'Stock: Streaming & Fundamental')

"""TAB 2"""
p,p2,widget,stats,corr=compare_plot()
l2=column(row(widget,stats,corr),gridplot([[p],[p2]], toolbar_location="right", plot_width=1300))
tab2=Panel(child = l2, title = 'Stock: Comparison')

"""TAB 3"""
div,update2=stream_news()
p,bea_select=geomap()
e1,e2,e3,e4,economy_select1,economy_select2,economy_select3,economy_select4=economy_plot()
l3=column(row(p,column(bea_select,div)),
          gridplot([[column(economy_select1,e1),column(economy_select2,e2)],
                   [column(economy_select3,e3),column(economy_select4,e4)]], 
                    toolbar_location="right", plot_width=1300))
tab3 = Panel(child = l3, title = 'Economy')


"""document"""
dashboard = Tabs(tabs = [tab1,tab2,tab3])
curdoc().add_root(dashboard)
curdoc().add_periodic_callback(update, 6000)
curdoc().add_periodic_callback(update2, 6000)
curdoc().title = "Financial Market Visualization & Analysis"



