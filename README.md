# CME_Parser
Python code to parse raw data from NYMEX

This repository contains three files

1) OrderBookParser_v1.py
    This file is used to construct the orderbook from the raw messages from NYMEX. The messages we receive will be in FIX format and we developed
    this custom parser to parse the message and construct the book by filling market depth information at various levels
    
2) Tickparser.py
     NYMEX provides tick data in FIX format. This file parses the messages and retrieves the necessary information which could be later used to 
     merge tick data with the market depth data based on nearest time stamps.
     
3) Sorter.py
     This is a general utility which is used by both the above files to sort the data based on the timestamps. Under the hood it uses mergesort algorithm to sort the generated output files.
     
 4) data_capture.py
      This program is used to capture live market data and market depth data on a daily basis and write the the raw and prepared data in
      separate files and push them to s3 buckets.
    
 5) scheduler.py
       This is an intelligent scheduler which starts capturing the data when the market is active at 6:00 PM ET and stops at the close of the day at 5:00 PM ET. The scheduler puts the process to sleep for extended hours during weekends.
       
 6) gmail_alerts.py
        This file is used to send regular alerts via email in case of any exceptions.
        
  
