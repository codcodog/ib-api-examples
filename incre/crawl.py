import datetime
import time
import functools

from ib_insync import Stock
from ib_insync import Index

from config.config import Config
from .crawl_db import Crawldb
from utils.common import log


class Crawl:
    def __init__(self, ib):
        self.log    = log(__name__, 'logs/incre/crawl.log')
        self.db     = Crawldb()
        self.ib     = ib
        self.empty  = []
        self.f      = []
        self.i      = 0
        self.total  = 0

    def crawl_data(self, codes):
        ''' IB 接口爬取数据
        '''
        # 计算与上一次爬取日期的差值
        records = self.db.get_records()
        if not records.rowcount:
            raise Exception('获取上次爬取数据日期失败，数据库 records 表返回空.')
        last_time = list(records)[0].date
        now_time  = datetime.datetime.now()
        diff      = now_time - last_time
        dur       = diff.days
        i         = 0
        total     = len(codes)

        for code in codes:
            symbol = code[0]
            stock  = Stock(symbol, Config.hk_exchange, Config.hk_currency)
            future = self.ib.reqHistoricalDataAsync(stock, endDateTime='', durationStr='%d D' % dur,
                                                    barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
            self.ib.sleep(0.02)
            future.add_done_callback(functools.partial(self.deal_data, symbol=symbol, last_time=last_time))
            self.f.append(future)

    def deal_data(self, future, symbol, last_time):
        ''' 回调函数，处理爬取回来的数据
        '''
        self.i += 1
        print('({i}/{total}) {symbol} HK 正在导入数据库'.format(i=self.i, total=self.total, symbol=symbol), flush=True)
        data = future.result()
        if not data:
            self.empty.append((symbol,))
            self.log.warn('%s 股没有数据' % symbol)
            return

        open_sql    = 'insert into `open` (`code`, `code_type`, `date`, `value`) values '
        high_sql    = 'insert into `high` (`code`, `code_type`, `date`, `value`) values '
        low_sql     = 'insert into `low` (`code`, `code_type`, `date`, `value`) values '
        close_sql   = 'insert into `close` (`code`, `code_type`, `date`, `value`) values '
        volume_sql  = 'insert into `volume` (`code`, `code_type`, `date`, `value`) values '
        average_sql = 'insert into `average` (`code`, `code_type`, `date`, `value`) values '

        # 标志，看是否该股是否有新数据更新，没有则跳过
        # 因为 IB 接口返回数据的时候，存在会有数据返回，但是数据的日期小于上次更新的日期的情况
        # 即，该股目前没有新数据更新
        flag = 0
        for bar_data in data:
            date_time = datetime.datetime(*bar_data.date.timetuple()[:6])
            if date_time > last_time:
                flag = 1
                date       = bar_data.date
                open_price = bar_data.open
                high       = bar_data.high
                low        = bar_data.low
                close      = bar_data.close
                volume     = bar_data.volume
                average    = bar_data.average

                open_sql    += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=open_price)
                high_sql    += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=high)
                low_sql     += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=low)
                close_sql   += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=close)
                volume_sql  += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=volume)
                average_sql += "('{code}', '{code_type}', '{date}', {value:.4f}),".format(code=symbol, code_type='hk', date=date, value=average)

        # 没有新数据更新
        if not flag:
            self.log.warn('{} HK 没有新数据更新'.format(symbol))
            return

        open_rows    = self.db.query(open_sql.rstrip(','))
        high_rows    = self.db.query(high_sql.rstrip(','))
        low_rows     = self.db.query(low_sql.rstrip(','))
        close_rows   = self.db.query(close_sql.rstrip(','))
        volume_rows  = self.db.query(volume_sql.rstrip(','))
        average_rows = self.db.query(average_sql.rstrip(','))

        if open_rows == 0:
            print('open 表插入失败：{}'.format(open_sql))
            raise SystemExit
        elif high_rows == 0:
            print('high 表插入失败：{}'.format(high_sql))
            raise SystemExit
        elif low_rows == 0:
            print('low 表插入失败：{}'.format(low_sql))
            raise SystemExit
        elif close_rows == 0:
            print('close 表插入失败：{}'.format(close_sql))
            raise SystemExit
        elif volume_rows == 0:
            print('volume 表插入失败：{}'.format(volume_sql))
            raise SystemExit
        elif average_rows == 0:
            print('average 表插入失败：{}'.format(average_sql))
            raise SystemExit
        else:
            pass

    def async_crawl(self, codes):
        ''' 事件循环
        '''
        self.crawl_data(codes)
        self.ib.run(*self.f)
        self.f.clear()

    def crawl_codes_data(self):
        ''' 爬取数据主程序
        '''
        t1 = time.time()
        codes = self.db.get_hk_codes()
        if not codes.rowcount:
            raise Exception('获取香港股失败，数据库 stock 表返回空.')

        codes = list(codes)
        self.total = len(codes)
        self.i     = 0
        self.async_crawl(codes)

        # 重新爬取那些数据为空的股
        i = 0
        while self.empty:
            i += 1
            empty_total = len(self.empty)
            self.log.info('第{}次重新爬取数据为空的股票：{}'.format(i, self.empty))
            print('######## 第{}次重新爬取数据为空的股票 ########'.format(i))
            stocks = list(self.empty)
            self.empty.clear()
            self.total = len(stocks)
            self.i     = 0
            self.async_crawl(stocks)

            # 爬取完，如果空的股数还是和之前一样，则默认这些股都没有数据，不再爬取
            if len(self.empty) == empty_total:
                break

        t2 = time.time()
        t3 = t2 - t1

        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        row = self.db.update_records(now)
        if  not row:
            self.log.error('更新 record 失败.')

        print('香港股全部爬完, 耗时：{:.2f}'.format(t3))

    def crawl_index_data(self):
        ''' 爬取 HSI Index 数据
        '''
        symbol   = 'HSI'
        exchange = 'HKFE'
        currency = 'HKD'

        records = self.db.get_hsi_record()
        if not records.rowcount:
            raise Exception('获取上次爬取数据日期失败，数据库 records 表返回空.')

        last_time = list(records)[0].date
        now_time  = datetime.datetime.now()
        diff      = now_time - last_time
        dur       = diff.days

        print('正在获取 HSI 数据.')

        index  = Index(symbol, exchange, currency)
        data   = self.ib.reqHistoricalData(index, endDateTime='', durationStr='%d D' % dur,
                                     barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
        if not data:
            raise RuntimeError('HSI 数据接口返回空.')

        sql = 'insert into `hsi` (`date`, `open`, `high`, `low`, `close`) values '
        for  bar_data in data:
            date_time = datetime.datetime(*bar_data.date.timetuple()[:6])
            if date_time > last_time:
                date       = bar_data.date
                open_price = bar_data.open
                high       = bar_data.high
                low        = bar_data.low
                close      = bar_data.close

                sql += "('{date}', {open:.4f}, {high:.4f}, {low:.4f}, {close:.4f}),".format(date=date, open=open_price, high=high, low=low, close=close)

        rows = self.db.query(sql.rstrip(','))
        if rows == 0:
            raise Exception('SQL 语句执行异常， 插入数据库失败：%s' % sql)
        else:
            now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            row = self.db.set_hsi_record(now)
            if not row:
                raise Exception('更新 HSI 日期记录失败.')
