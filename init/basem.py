import time
import datetime
import functools

from ib_insync import IB
from ib_insync import Stock
from ib_insync import Index

from utils.common import log
from config.config import Config
from .base_db import Basedb


class Basem:
    ''' 导入分钟级别股票信息类
    '''
    def __init__(self):
        self.log   = log(__name__, 'logs/basem.log')
        self.db    = Basedb()
        self.empty = []
        self.total = 0
        self.i     = 0

        self.ib = IB()
        self.ib.connect(Config.ib_host, Config.ib_port, Config.ib_client_id)

    def __del__(self):
        self.ib.disconnect()

    def deal_data(self, future, symbol):
        ''' 回调函数，处理接口返回的股票数据
        '''
        self.i += 1
        print('(%d/%d) 正在导入 %s HK' % (self.i, self.total, symbol), flush=True)

        data = future.result()
        if not data:
            self.empty.append((symbol,))
            return

        open_sql    = 'insert into `open_5m` (`code`, `code_type`, `date`, `value`) values '
        high_sql    = 'insert into `high_5m` (`code`, `code_type`, `date`, `value`) values '
        low_sql     = 'insert into `low_5m` (`code`, `code_type`, `date`, `value`) values '
        close_sql   = 'insert into `close_5m` (`code`, `code_type`, `date`, `value`) values '
        volume_sql  = 'insert into `volume_5m` (`code`, `code_type`, `date`, `value`) values '
        average_sql = 'insert into `average_5m` (`code`, `code_type`, `date`, `value`) values '

        for bar_data in data:
            date       = bar_data.date
            open_price = bar_data.open
            high       = bar_data.high
            low        = bar_data.low
            close      = bar_data.close
            average    = bar_data.average

            # volume 有不存在的情况, 16:00 收市，交易量不存在
            try:
                volume = bar_data.volume
            except AttributeError:
                volume = 0

            open_sql    += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=open_price)
            high_sql    += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=high)
            low_sql     += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=low)
            close_sql   += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=close)
            volume_sql  += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=volume)
            average_sql += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=average)

        open_rows    = self.db.query(open_sql.rstrip(','))
        high_rows    = self.db.query(high_sql.rstrip(','))
        low_rows     = self.db.query(low_sql.rstrip(','))
        close_rows   = self.db.query(close_sql.rstrip(','))
        volume_rows  = self.db.query(volume_sql.rstrip(','))
        average_rows = self.db.query(average_sql.rstrip(','))

        if open_rows.rowcount == 0:
            raise RuntimeError('open_sql 语句执行失败：%s' % open_sql)
        elif high_rows.rowcount == 0:
            raise RuntimeError('high_sql 语句执行失败：%s' % high_sql)
        elif low_rows.rowcount == 0:
            raise RuntimeError('low_sql 语句执行失败：%s' % low_sql)
        elif close_rows.rowcount == 0:
            raise RuntimeError('close_sql 语句执行失败：%s' % close_sql)
        elif volume_rows.rowcount == 0:
            raise RuntimeError('volume_sql 语句执行失败：%s' % volume_sql)
        elif average_rows.rowcount == 0:
            raise RuntimeError('average_sql 语句执行失败：%s' % average_sql)
        else:
            pass

    def crawl_data(self, codes):
        ''' 爬取 IB 接口股票的交易信息
        '''
        futures = []
        i = 0
        for code in codes:
            i += 1
            symbol, _ = code
            stock  = Stock(symbol, Config.hk_exchange, Config.hk_currency)
            future = self.ib.reqHistoricalDataAsync(stock, endDateTime='', durationStr='900 S',
                                                    barSizeSetting='5 mins', whatToShow='TRADES', useRTH=True)
            self.ib.sleep(0.02)
            future.add_done_callback(functools.partial(self.deal_data, symbol=symbol))
            futures.append(future)

        return futures

    def get_codes_data(self, codes=None):
        ''' 爬取股票信息
        1个月的5分钟交易信息
        '''
        t1 = time.time()

        # codes => None 则从数据库获取股票列表
        # 否则，使用传递进来的codes list，目的是再次爬取那些空数据的股票
        # 以确保股票数据为空而不会遗漏有数据的股
        # 因为有时连接超时，接口会返回空列表，但此股是有数据的
        if codes is None:
            codes = self.db.get_codes()
            if not codes.rowcount:
                raise RuntimeError('获取股票失败，stock 表返回空.')

        codes      = list(codes)
        self.total = len(codes)
        self.i     = 0

        futures = self.crawl_data(codes)
        self.ib.run(*futures)

        # 爬取完成，记录爬取的endDateTime时间，供下次增量爬取使用
        end_date_time = '2017-12-31 23:59:59'
        res           = self.db.set_record(end_date_time)
        if not res.rowcount:
            raise RuntimeError('记录一个月5分钟的end_date_time失败.')

        t2 = time.time()
        t3 = t2 - t1
        print('HK 股票交易信息全部导入完成，耗时：%.2fs' % t3)
        self.log.info('导入股票信息完成，数据为空的股票有：{}'.format(self.empty))

    def get_hsi_data(self):
        ''' 获取 HSI 一个月5分钟的信息
        '''
        symbol   = 'HSI'
        exchange = 'HKFE'
        currency = 'HKD'
        index    = Index(symbol, exchange, currency)

        data = self.ib.reqHistoricalData(index, endDateTime='20180119 15:00:00', durationStr='900 S',
                                         barSizeSetting='5 mins', whatToShow='TRADES', useRTH=True)
        if not data:
            raise RuntimeError('HSI 数据接口返回空.')

        sql = 'insert into `hsi_5m` (`date`, `open`, `high`, `low`, `close`) values '
        for bar_data in data:
            date       = bar_data.date
            open_price = bar_data.open
            high       = bar_data.high
            low        = bar_data.low
            close      = bar_data.close

            sql += "('{date}', {open:.4f}, {high:.4f}, {low:.4f}, {close:.4f}),".format(date=date, open=open_price, high=high, low=low, close=close)
        res = self.db.query(sql.rstrip(','))

        if res.rowcount == 0:
            raise RuntimeError('SQL 语句执行异常， 插入数据库失败：%s' % sql)
        else:
            print('HSI Index 1个月5分钟数据导入完成.', flush=True)
