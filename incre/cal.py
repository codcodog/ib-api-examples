import time
import datetime
import queue
import threading
import sys

from utils.common import log
from utils.common import redis
from .cal_db import Caldb


# 线程局部变量
thread_data = threading.local()
class Cal:
    def __init__(self):
        self.redis = redis()
        self.log   = log(__name__, 'logs/incre/cal.log')
        self.db    = Caldb()

        self.codes = queue.Queue()
        self.codes_total = 0
        self.dates = queue.Queue()
        self.dates_total = 0

    def get_codes_queue(self):
        ''' 组装 codes 队列
        '''
        t1  = time.time()
        res = self.db.get_codes()

        if not res.rowcount:
            raise Exception('获取Codes失败，stock 表返回空')

        for code in res:
            symbol, code_type = code
            self.codes.put((symbol, code_type))

        self.codes_total = self.codes.qsize()
        t2 = time.time()
        t3 = t2 - t1
        print('Codes 队列组装完成，共 %d 股，耗时：%.2fs' % (self.codes.qsize(), t3))

    def chunk(self, data, ma):
        ''' 生成器函数，截取
        '''
        length = len(data) - ma
        for i in range(length):
            yield data[i:i+ma]

    def cal_ma(self, ma, record_date):
        ''' 计算ma'''
        while not self.codes.empty():
            symbol, code_type = self.codes.get()
            num               = self.codes_total - self.codes.qsize()

            print('(%d/%d) 正在计算 %s HK MA' % (num, self.codes_total, symbol))
            sys.stdout.flush()

            now_date = datetime.datetime.now()
            diff = now_date - record_date
            limit = diff.days + ma - 1

            t2   = time.time()
            res  = self.db.get_code_data(symbol, limit)

            t3   = time.time()
            t4   = t3 - t2

            rows   = res.rowcount

            # 如果没有数据，跳过
            if not rows:
                continue

            if rows < limit:
                self.log.error('{} 股返回数据量不足以完成{}MA的计算，忽略该股'.format(symbol, ma))
                continue

            old_date = datetime.datetime.date(record_date)
            for per in self.chunk(list(res), ma):
                date      = per[0].date
                now_price = per[0].value
                total     = 0

                # 如果日期已经超过记录日期，则结束
                if date <= old_date:
                    break

                for p in per:
                    total += p.value
                ave = round(total / ma, 2)

                name = 'ma_{type}_{code}_{code_type}_{date}'.format(type=ma, code=symbol, code_type=code_type, date=date)
                if now_price > ave:
                    self.redis.set(name, 1)
                else:
                    self.redis.set(name, 0)

            t5 = time.time()
            t6 = t5 - t3
            self.log.info('计算 {ma}MA {symbol} HK 完成，请求数据：{t1:.2f}s，计算耗时：{t2:.2f}s'.format(ma=ma, symbol=symbol, t1=t4, t2=t6))

    def ma_main(self, ma):
        ''' 计算ma主程序
        '''
        t1 = time.time()
        # 组装codes队列
        self.get_codes_queue()

        thread_num  = 10
        thread_list = []

        record = self.db.get_record()
        if not record.rowcount:
            raise Exception('获取记录日期失败，records 表返回空.')
        record_date = list(record)[0].date

        for i in range(thread_num):
            thread_list.append(threading.Thread(target=self.cal_ma, args=(ma, record_date)))

        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

        t2 = time.time()
        t3 = t2 - t1
        print('{}Ma 计算完成，耗时：{:.2f}s'.format(ma, t3))

    def get_date_queue(self):
        ''' HSI 日期队列
        '''
        t1          = time.time()
        record      = self.db.get_record()
        record_date = datetime.datetime.date(list(record)[0].date)
        hsi         = self.db.get_hsi_date(record_date)

        if not hsi.rowcount:
            raise Exception('增量获取 HSI 日期失败, hsi 表返回空')

        for h in hsi:
            date = h.date
            self.dates.put(date)

        self.dates_total = self.dates.qsize()
        t2               = time.time()
        t3               = t2 - t1
        print('HSI 日期队列组装完成, 共 {total} 天，耗时：{t:.2f}s \n'.format(total=self.dates.qsize(), t=t3))

    def cal_senti(self, codes, ma):
        ''' 计算 sentiment
        '''
        while not self.dates.empty():
            t1                = time.time()
            date              = self.dates.get()
            num               = self.dates_total - self.dates.qsize()
            thread_data.total = 0
            thread_data.num   = 0
            sql               = 'insert into `sentiment` (`type`, `ma_type`, `date`, `per`) values '

            print('(%d/%d) 正在计算：%s' % (num, self.dates_total, date))
            sys.stdout.flush()

            for symbol, code_type in codes:
                name = 'ma_{ma}_{code}_{code_type}_{date}'.format(ma=ma, code=symbol, code_type=code_type, date=date)

                try:
                    t = self.redis.get(name).decode('utf-8')

                    if t == '1':
                        thread_data.num += 1

                    thread_data.total += 1

                    # 清除缓存
                    res = self.redis.delete(name)
                except AttributeError: # redis 不存在此 name
                    self.log.warn('{ma}MA {code} {code_type} {date} 不存在'.format(ma=ma, code=symbol, code_type=code_type, date=date))
                    continue

            # 如果都不存在，则跳过该日期
            if thread_data.total == 0:
                self.log.warn('{date} 没有任何code 符合'.format(date=date))
                continue

            per = thread_data.num / thread_data.total * 100
            sql += "('{type}', {ma_type}, '{date}', {per:.4f})".format(type=code_type, ma_type=ma, date=date, per=per)
            res = self.db.multi_query(sql)
            rows = res.rowcount

            if rows == 0:
                self.log.error('插入SQL语句失败：{sql}'.format(sql=sql))

            t2 = time.time()
            t3 = t2 - t1

            self.log.info('{date} 计算成功, True数目：{num}, 总数：{total}, 百分比：{per:.4f}, 耗时：{t:.2f}s'.format(date=date, t=t3, num=thread_data.num, total=thread_data.total, per=per))

    def senti_main(self, ma):
        ''' 计算 sentiment 主程序
        '''
        print('开始计算 Sentiment \n')
        t1 = time.time()
        self.get_date_queue()

        thread_num  = 10
        thread_list = []
        codes       = list(self.db.get_codes())

        for i in range(thread_num):
            thread_list.append(threading.Thread(target=self.cal_senti, args=(codes, ma)))

        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

        t2 = time.time()
        t3 = t2 - t1
        print('Sentiment 计算完成，耗时：{t:.2f}s \n'.format(t=t3))

    def main(self):
        ''' 主程序调用
        '''
        ma = [20, 50]
        for i in ma:
            self.ma_main(i)
            self.senti_main(i)

        # 计算完成之后，更新 record date
        now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        row = self.db.set_record_date(now)
        if not row:
            raise Exception('更新 HSI 日期记录失败.')

