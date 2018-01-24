import os
import csv
import logging
import time
import sys

from ib_insync import *

from init.base_db import Basedb
from config.config import Config

class Base:
    ''' 初始化类
    '''
    def __init__(self):
        self.db = Basedb()

    def hk_to_db(self):
        ''' 香港股票基本信息导入数据库
        '''
        pwd      = os.path.dirname(os.path.realpath(__file__))
        csv_file = pwd + '/docs/hk.csv'

        if not os.path.exists(csv_file):
            raise RuntimeError('hk.csv 文件不存在.')

        with open(csv_file) as fp:
            rows   = csv.reader(fp)
            header = next(rows)
            sql    = 'insert into `stock` (`symbol`, `type`) values '

            for row in rows:
                tmp                  = row[0].split(' ')
                symbol, code_type, _ = tmp
                sql                 += "({symbol}, '{code_type}'),".format(symbol=symbol, code_type=code_type.lower())

            sql  = sql.rstrip(',')

            rows = self.db.query(sql)

            if rows == 0:
                logging.error('hk.csv 导入失败.')
            else:
                print('hk.csv 导入成功，共有 {} 股导入.'.format(rows))

    def get_codes_data(self, stock_id = None):
        ''' 获取股票数据
        @param  stock_id 股票id，用来设置断点异常恢复

        当某个股票导致程序异常的时候，下次恢复，输入上次异常股的id，
        则程序从上次异常退出的股票继续执行.
        '''
        ib = IB()
        ib.connect(Config.ib_host, Config.ib_port, Config.ib_client_id)

        stocks   = self.db.get_codes(stock_id)
        total    = stocks.rowcount
        empty    = []
        error    = []
        i        = 0
        for stock in stocks:
            i += 1
            symbol, code_type = stock

            if code_type == 'hk':
                print('(%d/%d) 正在导入：%s %s ' % (i, total, symbol, 'HK'), end='')
                sys.stdout.flush()

                stock = Stock(symbol, Config.hk_exchange, Config.hk_currency)
                t1    = time.time()
                data  = ib.reqHistoricalData(stock, endDateTime='20171231 00:00:00', durationStr='10 Y',
                                             barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
                t2 = time.time()
                t3 = round(t2 - t1, 2)

                # 如果数据为空，跳过
                if not len(data):
                    print('数据为空，导入失败.')
                    empty.append(symbol)
                    continue

                open_sql    = 'insert into `open` (`code`, `code_type`, `date`, `value`) values '
                high_sql    = 'insert into `high` (`code`, `code_type`, `date`, `value`) values '
                low_sql     = 'insert into `low` (`code`, `code_type`, `date`, `value`) values '
                close_sql   = 'insert into `close` (`code`, `code_type`, `date`, `value`) values '
                volume_sql  = 'insert into `volume` (`code`, `code_type`, `date`, `value`) values '
                average_sql = 'insert into `average` (`code`, `code_type`, `date`, `value`) values '

                # 每次请求完，睡眠0.02s，防止1s内请求超过50个
                time.sleep(0.02)

                for bar_data in data:
                    date       = bar_data.date
                    open_price = bar_data.open
                    high       = bar_data.high
                    low        = bar_data.low
                    close      = bar_data.close
                    volume     = bar_data.volume
                    average    = bar_data.average

                    open_sql    += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(open_price, 4))
                    high_sql    += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(high, 4))
                    low_sql     += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(low, 4))
                    close_sql   += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(close, 4))
                    volume_sql  += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=volume)
                    average_sql += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(average, 4))

                open_sql     = open_sql.rstrip(',')
                high_sql     = high_sql.rstrip(',')
                low_sql      = low_sql.rstrip(',')
                close_sql    = close_sql.rstrip(',')
                volume_sql   = volume_sql.rstrip(',')
                average_sql  = average_sql.rstrip(',')

                # 防止出现未知异常中断程序
                try:
                    open_rows    = self.db.query(open_sql)
                    high_rows    = self.db.query(high_sql)
                    low_rows     = self.db.query(low_sql)
                    close_rows   = self.db.query(close_sql)
                    volume_rows  = self.db.query(volume_sql)
                    average_rows = self.db.query(average_sql)
                except:
                    print('SQL执行异常，导入失败.')
                    error.append(symbol)
                    continue

                if open_rows.rowcount == 0:
                    logging.error('open 表插入失败：{}'.format(open_sql))
                    raise SystemExit
                elif high_rows.rowcount == 0:
                    logging.error('high 表插入失败：{}'.format(high_sql))
                    raise SystemExit
                elif low_rows.rowcount == 0:
                    logging.error('low 表插入失败：{}'.format(low_sql))
                    raise SystemExit
                elif close_rows.rowcount == 0:
                    logging.error('close 表插入失败：{}'.format(close_sql))
                    raise SystemExit
                elif volume_rows.rowcount == 0:
                    logging.error('volume 表插入失败：{}'.format(volume_sql))
                    raise SystemExit
                elif average_rows.rowcount == 0:
                    logging.error('average 表插入失败：{}'.format(average_sql))
                    raise SystemExit
                else:
                    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    print('导入成功，请求数据耗时：%.2fs %s' % (t3, now_time))
                    sys.stdout.flush()

        if len(empty) or len(error):
            print('股票导入完成，获取数据为空的股：{empty}, 执行SQL语句异常的股：{error}'.format(empty=empty, error=error))
        elif not len(empty) and not len(error):
            print('全部股票导入完成.')
        ib.disconnect()

    def continue_codes_data(self, stocks):
        ''' 把数据为空的股票再次导入，防止意外
        '''
        ib = IB()
        ib.connect(Config.ib_host, Config.ib_port, Config.ib_client_id)

        total    = len(stocks)
        empty    = []
        error    = []
        i        = 0

        for stock in stocks:
            i += 1
            symbol    = stock
            code_type = 'hk'

            if code_type == 'hk':
                print('(%d/%d) 正在导入：%s %s ' % (i, total, symbol, 'HK'), end='')
                sys.stdout.flush()

                stock = Stock(symbol, Config.hk_exchange, Config.hk_currency)
                t1    = time.time()
                data  = ib.reqHistoricalData(stock, endDateTime='20171231 00:00:00', durationStr='10 Y',
                                             barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
                t2 = time.time()
                t3 = round(t2 - t1, 2)

                # 如果数据为空，跳过
                if not len(data):
                    print('数据为空，导入失败.')
                    empty.append(symbol)
                    continue

                open_sql    = 'insert into `open` (`code`, `code_type`, `date`, `value`) values '
                high_sql    = 'insert into `high` (`code`, `code_type`, `date`, `value`) values '
                low_sql     = 'insert into `low` (`code`, `code_type`, `date`, `value`) values '
                close_sql   = 'insert into `close` (`code`, `code_type`, `date`, `value`) values '
                volume_sql  = 'insert into `volume` (`code`, `code_type`, `date`, `value`) values '
                average_sql = 'insert into `average` (`code`, `code_type`, `date`, `value`) values '

                # 每次请求完，睡眠0.02s，防止1s内请求超过50个
                time.sleep(0.02)

                for bar_data in data:
                    date       = bar_data.date
                    open_price = bar_data.open
                    high       = bar_data.high
                    low        = bar_data.low
                    close      = bar_data.close
                    volume     = bar_data.volume
                    average    = bar_data.average

                    open_sql    += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(open_price, 4))
                    high_sql    += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(high, 4))
                    low_sql     += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(low, 4))
                    close_sql   += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(close, 4))
                    volume_sql  += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=volume)
                    average_sql += "('{code}', '{code_type}', '{date}', {value}),".format(code=symbol, code_type='hk', date=date, value=round(average, 4))

                open_sql     = open_sql.rstrip(',')
                high_sql     = high_sql.rstrip(',')
                low_sql      = low_sql.rstrip(',')
                close_sql    = close_sql.rstrip(',')
                volume_sql   = volume_sql.rstrip(',')
                average_sql  = average_sql.rstrip(',')

                # 防止出现未知异常中断程序
                try:
                    open_rows    = self.db.query(open_sql)
                    high_rows    = self.db.query(high_sql)
                    low_rows     = self.db.query(low_sql)
                    close_rows   = self.db.query(close_sql)
                    volume_rows  = self.db.query(volume_sql)
                    average_rows = self.db.query(average_sql)
                except:
                    print('SQL执行异常，导入失败.')
                    error.append(symbol)
                    continue

                if open_rows.rowcount == 0:
                    logging.error('open 表插入失败：{}'.format(open_sql))
                    raise SystemExit
                elif high_rows.rowcount == 0:
                    logging.error('high 表插入失败：{}'.format(high_sql))
                    raise SystemExit
                elif low_rows.rowcount == 0:
                    logging.error('low 表插入失败：{}'.format(low_sql))
                    raise SystemExit
                elif close_rows.rowcount == 0:
                    logging.error('close 表插入失败：{}'.format(close_sql))
                    raise SystemExit
                elif volume_rows.rowcount == 0:
                    logging.error('volume 表插入失败：{}'.format(volume_sql))
                    raise SystemExit
                elif average_rows.rowcount == 0:
                    logging.error('average 表插入失败：{}'.format(average_sql))
                    raise SystemExit
                else:
                    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    print('导入成功，请求数据耗时：%.2fs %s' % (t3, now_time))
                    sys.stdout.flush()

        if len(empty) or len(error):
            print('股票导入完成，获取数据为空的股：{empty}, 执行SQL语句异常的股：{error}'.format(empty=empty, error=error))
        elif not len(empty) and not len(error):
            print('全部股票导入完成.')
        ib.disconnect()

    def get_index_data(self, symbol, exchange, currency):
        ''' 获取 HSI Index 数据
        '''
        ib = IB()
        ib.connect(Config.ib_host, Config.ib_port, Config.ib_client_id)

        index  = Index(symbol, exchange, currency)
        data   = ib.reqHistoricalData(index, endDateTime='20171231 00:00:00', durationStr='10 Y',
                                     barSizeSetting='1 day', whatToShow='TRADES', useRTH=True)
        if not data:
            raise RuntimeError('HSI 数据接口返回空.')

        sql = 'insert into `hsi` (`date`, `open`, `high`, `low`, `close`) values '
        for  bar_data in data:
            date       = bar_data.date
            open_price = bar_data.open
            high       = bar_data.high
            low        = bar_data.low
            close      = bar_data.close

            sql += "('{date}', {open:.4f}, {high:.4f}, {low:.4f}, {close:.4f}),".format(date=date, open=open_price, high=high, low=low, close=close)
        rows = self.db.query(sql.rstrip(','))

        if rows.rowcount == 0:
            raise Exception('SQL 语句执行异常， 插入数据库失败：%s' % sql)
        else:
            print('HSI Index 10年数据导入完成.')
        ib.disconnect()
