# Realtime Financial Market Data Visualization and Analysis


## Introduction

In this project, I developed a financial data processing and visualization platform using Apache Kafka, Apache Cassandra, and Bokeh. I used Kafka for realtime stock price and market news streaming, Cassandra for historical and realtime stock data warehousing, and Bokeh for visualization on web browsers. I also wrote a web crawler to scrape companys' financial statements and basic information from Yahoo Finance, and played with various economy data APIs. 




**Please check the platform's website and play with each plot** $\rightarrow$  [magiconch.me](http://magiconch.me/)


## Architecture

There are currently 3 tabs in the webpage:

- ***Stock: Streaming & Fundamental*** 
  - Visualize a single stock's candlestick plot, basic company & financial information;
  - Plot realtime S&P500 price during trading hours (fake date during non-trading hours)
- ***Stock: Comparison***
  - Plot 2 user-selected stocks' price, and print their statstical summay and correlation
  - Visualize the 5,10,30-day moving average of adjusted close price
- ***Economy***
  - Plot a geomap to visualize various economy data by state
  - Plot 4 economy indicators nationwide for comparison
  - Present the most recent market news 

&nbsp;


Here is the architecture of the platform, and I will go through each part of it.

<img src="./kafka_stock.png" width="800" />



