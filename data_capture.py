import argparse  # command line argument parser
import datetime as dt  # date and time functions
import time  # time module for timestamping
import os  # used to create directories
import redis

# --------ibpy imports ----------------------
from contracts import createContract
from ib.opt import ibConnection, message
from ib.ext.Contract import Contract
import datetime
from threading import Lock
from scipy.stats import skew
from scipy.stats import kurtosis
import numpy as np
import time
import boto3

import sys
#import threading
#import scheduler

# tick type definitions, see IB api manual
priceTicks = {1: 'bid', 2: 'ask', 4: 'last', 6: 'high', 7: 'low', 9: 'close', 14: 'open'}
sizeTicks = {0: 'bid', 3: 'ask', 5: 'last', 8: 'volume'}
lastPrice = []
lastSize = []
lock = Lock()
prev_mktdepth_file = ''
prev_tick_file = ''
prev_mkt_aggregated = ''
prev_final_data = ''


# market_snapshot_file = 'marketdepth_latest\\snapshot.txt'
# snapshot = open(market_snapshot_file,"r+")


class TickLogger(object):
    ''' class for handling incoming ticks and saving them to file
        will create a subdirectory 'tickLogs' if needed and start logging
        to a file with current timestamp in its name.
        All timestamps in the file are in seconds relative to start of logging

    '''

    def __init__(self, tws, subscriptions):
        ''' init class, register handlers '''
        global prev_mktdepth_file
        global prev_tick_file
        global prev_mkt_aggregated
        global prev_final_data
        tws.register(self._updateMktDepth, 'UpdateMktDepth')
        tws.register(self._updateMktDepthL2, 'UpdateMktDepthL2')
        tws.register(self._priceHandler, 'TickPrice')
        tws.register(self._sizeHandler, 'TickSize')
        self.tws = tws
        self.subscriptions = subscriptions
        #self.redis_conn = redis_conn
        self.write_YN = True
        self.last_price = 0
        self.last_size = 0
        self.marketdepth_volume_dict = dict()
        self.bid_vol = [0] * 10
        self.ask_vol = [0] * 10
        self.marketdepth_volume_dict['Bid'] = self.bid_vol
        self.marketdepth_volume_dict['Ask'] = self.ask_vol
        self.bid_list = []
        self.ask_list = []
        self.prev_obv = 0
        self.prev_cv_bid = 0
        self.prev_cv_ask = 0
        self.prev_price = 0
        self.obv = 0
        self.ask_volume = 0
        self.bid_volume = 0
        self.cv_bid = 0
        self.cv_ask = 0

        # save starting time of logging. All times will be in seconds relative
        # to this moment
        self._startTime = datetime.datetime.now()
        s3 = boto3.resource()
        self.s3 = s3

        # create data directory if it does not exist
        if not os.path.exists('marketdepth_latest_v5'): os.mkdir('marketdepth_latest_v5')

        # open data file for writing

        fileName_mktdepth = 'marketdepth_latest_v5\\marketdepthLogs_Aggregated_%s.csv' % (
            datetime.datetime.today().strftime('%Y-%m-%d'))

        print('Logging market depth to ', fileName_mktdepth)
        if os.path.isfile(fileName_mktdepth):
            self.dataFile_marketdepth = open(fileName_mktdepth, 'a')
        else:
            print("Writing to a new file")
            self.dataFile_marketdepth = open(fileName_mktdepth, 'w')
        prev_mkt_aggregated = self.dataFile_marketdepth

        fileName_mktdepth_raw = 'marketdepth_latest_v5\\marketdepthLogs_Raw_%s.csv' % (
            datetime.datetime.today().strftime('%Y-%m-%d'))
        self.mkrawfile = fileName_mktdepth_raw
        print('Logging market depth to ', fileName_mktdepth)
        if os.path.isfile(fileName_mktdepth_raw):
            self.dataFile_marketdepth_raw = open(fileName_mktdepth_raw, 'a')
        else:
            self.dataFile_marketdepth_raw = open(fileName_mktdepth_raw, 'w')
            self.dataFile_marketdepth.write(
                'Datetimestamp    , Bid_Volume, Ask_Volume, Bid_Ask_Ratio, Skew_Bid, Skew_Ask, Kurtosis_Bid, Kurtosis_Ask\n\n')
            print("Creating new file....")
        prev_mktdepth_file = self.dataFile_marketdepth_raw

        fileName_tick = 'marketdepth_latest_v5\\ticklog_%s.csv' % (datetime.datetime.today().strftime('%Y-%m-%d'))
        self.tickfile = fileName_tick
        print('Logging market depth to ', fileName_tick)
        if os.path.isfile(fileName_tick):
            self.fileName_tick = open(fileName_tick, 'a')
        else:
            self.fileName_tick = open(fileName_tick, 'w')
            print("Creating new file....")
        prev_tick_file = self.fileName_tick
        fileName_agg = 'marketdepth_latest_v5\\data_to_model_%s.csv' % (datetime.datetime.today().strftime('%Y-%m-%d'))
        self.mkaggfile = fileName_agg
        print('Logging market depth to ', fileName_agg)
        if os.path.isfile(fileName_agg):
            self.datafile_agg = open(fileName_agg, 'a')
        else:  # print("Creating new file....")
            self.datafile_agg = open(fileName_agg, 'w')
            self.datafile_agg.write(
                'Datetimestamp, Future_Price, Bid_Volume, Ask_Volume, Tick_Count, OBV, CV_Bid, CV_Ask, Volume_Bid, Volume_Ask, Skew_Bid, Kurtosis_Bid, Skew_Ask, Kurtosis_Ask\n\n')
            print("Creating new file....")

        prev_final_data = self.datafile_agg
        self.closeTiming = datetime.time(17, 0, 0, 0).strftime('%H:%M:%S:%f')
        self.closeTiming = datetime.time(16, 59, 0, 0).strftime('%H:%M:%S:%f')
        print(self.closeTiming)
        self.openTiming = datetime.time(18, 0, 0, 0).strftime('%H:%M:%S:%f')
        print(self.openTiming)

    def restart_capture(self, time_to_sleep):
        self.write_YN = False
        self.close_files()
        data_tick = open(self.tickfile, 'rb')
        self.s3.Bucket('tick-raw').put_object(Key=str(self.tickfile), Body=data_tick)
        data_tick.close()
        os.remove(self.tickfile)
        data_mkt_agg = open(self.mkaggfile, 'rb')
        self.s3.Bucket('data-for-model').put_object(Key=str(self.mkaggfile).split('\\')[1], Body=data_mkt_agg)
        data_mkt_agg.close()
        os.remove(self.mkaggfile)
        data_mkt_raw = open(self.mkrawfile, 'rb')
        self.s3.Bucket('marketdepth-raw').put_object(Key=str(self.mkrawfile).split('\\')[1], Body=data_mkt_raw)
        data_mkt_raw.close()
        os.remove(self.mkrawfile)
        self.tws.cancelMktData(902)
        self.tws.cancelMktDepth(901)
        # self.tws.disconnect()
        time.sleep(time_to_sleep)
        logTicks(contracts)

    def _sleep(self):

        self.tws.disconnect()
        self.redis_conn.flushall()
        try:
            if datetime.datetime.today().weekday() == 4:
                self.restart_capture(176400)
            else:
                self.restart_capture(3600)

        finally:
            print("Done!!!")

    def close_files(self):
        if prev_mktdepth_file != '':
            prev_mktdepth_file.close()
            prev_tick_file.close()
            prev_mkt_aggregated.close()
            prev_final_data.close()

    def time_to_sleep(self):
        millis = datetime.datetime.now().time().strftime('%H:%M:%S:%f')
        delta = datetime.datetime.strptime(self.closeTiming, '%H:%M:%S:%f') - datetime.datetime.strptime(millis,
                                                                                                         '%H:%M:%S:%f')
        if delta.seconds == 0:
            self._sleep()

    def write_snapshot(self):
        self.redis_conn.set("mktdepth_snapshot", str(self.marketdepth_volume_dict))

    def add_to_dict(self, msg):
        if msg.side == 1:
            if msg.operation == 0 or msg.operation == 1:
                self.marketdepth_volume_dict['Bid'][msg.position] = msg.size
            else:
                self.marketdepth_volume_dict['Bid'][msg.position] = 0
            # self.bid_list.append(msg.size)
            self.write_snapshot()
        elif msg.side == 0:
            if msg.operation == 0 or msg.operation == 1:
                self.marketdepth_volume_dict['Ask'][msg.position] = msg.size
            else:
                self.marketdepth_volume_dict['Ask'][msg.position] = 0
            self.write_snapshot()

    def _updateMktDepth(self, msg):
        if self.write_YN:
            self._writeRawMarketDepth(msg)
        self.add_to_dict(msg)

    def _writeRawMarketDepth(self, msg):
        timestamp = datetime.datetime.now()
        dataLine = ','.join(
            str(bit) for bit in [timestamp, msg.position, msg.operation, msg.side, msg.price, msg.size]) + '\n'
        self.dataFile_marketdepth_raw.write(dataLine)

    def _updateMktDepthL2(self, reqId: 5, position: int, marketMaker: str,
                          operation: int, side: int, price: float, size: int):
        super().updateMktDepthL2(reqId, position, marketMaker, operation, side,
                                 price, size)
        print("UpdateMarketDepthL2. ", reqId, "Position:", position, "Operation:",
              operation, "Side:", side, "Price:", price, "Size", size)

    def _priceHandler(self, msg):
        ''' price tick handler '''

        data = ['price', priceTicks[msg.field],
                msg.price]  # data, second field is price tick type
        if 'last' in data:
            self.last_price = msg.price

    def _sizeHandler(self, msg):
        ''' size tick handler '''
        data = ['size', sizeTicks[msg.field], msg.size]
        if 'last' in data:
            lock.acquire()
            try:
                self.price = self.last_price
                if self.prev_price == 0:
                    self.prev_price = self.last_price
                if self.prev_price != 0:
                    if self.price < self.prev_price:
                        self.bid_volume = msg.size
                        self.ask_volume = 0
                    elif self.price > self.prev_price:
                        self.ask_volume = msg.size
                        self.bid_volume = 0
                    elif self.price == self.prev_price:
                        self.ask_volume = float(msg.size / 2)
                        self.bid_volume = float(msg.size / 2)
                    self.obv = self.prev_obv + self.ask_volume - self.bid_volume
                    self.prev_obv = self.obv
                    self.cv_bid = self.prev_cv_bid + self.bid_volume
                    self.prev_cv_bid = self.cv_bid
                    self.cv_ask = self.prev_cv_ask + self.ask_volume
                    self.prev_cv_ask = self.cv_ask
                    self._writeData(self.price, msg.size)
                self.prev_price = self.last_price
            finally:
                lock.release()

    def _writeMarketDepth(self, timestamp):
        try:
            bid_vol = sum(self.marketdepth_volume_dict['Bid'])
            ask_vol = sum(self.marketdepth_volume_dict['Ask'])
            try:
                if bid_vol == 0 or ask_vol == 0:
                    bid_ask_ratio = 0
                else:
                    bid_ask_ratio = bid_vol / (1. * ask_vol)
                skewness_bid = skew(self.marketdepth_volume_dict['Bid'])
                skewness_ask = skew(self.marketdepth_volume_dict['Ask'])
                skewness_bid = 0 if np.isnan(skewness_bid) else skewness_bid
                skewness_ask = 0 if np.isnan(skewness_ask) else skewness_ask
                kurtosis_bid = kurtosis(self.marketdepth_volume_dict['Bid'])
                kurtosis_ask = kurtosis(self.marketdepth_volume_dict['Ask'])
                kurtosis_bid = 0 if np.isnan(kurtosis_bid) else kurtosis_bid
                kurtosis_ask = 0 if np.isnan(kurtosis_ask) else kurtosis_ask
                dataLine = ','.join(str(bit) for bit in
                                    [timestamp, bid_vol, ask_vol, bid_ask_ratio, skewness_bid, skewness_ask,
                                     kurtosis_bid, kurtosis_ask]) + '\n'
                dataLine_agg = ','.join(str(bit) for bit in
                                        [timestamp, self.price, self.bid_volume, self.ask_volume, 1, self.obv,
                                         self.cv_bid, self.cv_ask, bid_vol, ask_vol, skewness_bid, kurtosis_bid,
                                         skewness_ask, kurtosis_ask]) + '\n'
                self.dataFile_marketdepth.write(dataLine)
                self.datafile_agg.write(dataLine_agg)
            except ZeroDivisionError as e:
                print(str(e))
        except Exception as e:
            print(str(e))

    def _writeData(self, price, size):
        ''' write data to log file while adding a timestamp '''
        timestamp = datetime.datetime.now()  # 1 ms resolution

        try:
            if self.write_YN:
                self._writeMarketDepth(timestamp)
                dataLine = ','.join(str(bit) for bit in [timestamp, price, size]) + '\n'
                self.fileName_tick.write(dataLine)
        except Exception as e:
            print("Excepiton " + str(e))

    def flush(self):
        ''' commits data to file'''
        self.dataFile_marketdepth.flush()
        self.fileName_tick.flush()

    def close(self):
        '''close file in a neat manner '''
        print('Closing data file')
        self.dataFile_marketdepth.close()
        self.fileName_tick.close()


def printMessage(msg):
    ''' function to print all incoming messages from TWS '''
    print
    '[msg]:', msg


def logTicks(contracts, verbose=False):
    '''
    log ticks from IB to a csv file

    Parameters
    ----------
    contracts : ib.ext.Contract objects
    verbose : print out all tick events
    '''
    # check for correct input
    assert isinstance(contracts, (list, Contract)), 'Wrong input, should be a Contract or list of contracts'

    # ---create subscriptions  dictionary. Keys are subscription ids
    subscriptions = {}
    redis_host = "localhost"
    redis_port = 6379
    redis_password = ""
    try:
        for idx, c in enumerate(contracts):
            subscriptions[idx + 1] = c
    except TypeError:  # not iterable, one contract provided
        subscriptions[1] = contracts
    tws = ibConnection(port=4001, clientId=101)
    #redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    logger = TickLogger(tws, subscriptions)
    #threading.Thread(target=scheduler.every, args=(1, logger)).start()
    if verbose: tws.registerAll(printMessage)
    tws.connect()

    # -------subscribe to data
    for subId, c in subscriptions.items():
        assert isinstance(c, Contract), 'Need a Contract object to subscribe'
        tws.reqMktDepth(subId + 501, c, 10)
        tws.reqMktData(subId + 502, c, "", False)
    # ------start a loop that must be interrupted with Ctrl-C
    print('Press Ctr-C to stop loop')

    try:
        while True:
            time.sleep(2)  # wait a little
            # logger.flush()
            # sys.stdout.write ('.')  # print a dot to the screen


    except KeyboardInterrupt:
        print('Interrupted with Ctrl-c')

    logger.close()
    tws.disconnect()
    print('All done')


if __name__ == '__main__':
    symbol = "CL"
    contracts = createContract(symbol)
    logTicks(contracts)
