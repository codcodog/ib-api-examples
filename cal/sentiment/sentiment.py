import time
import queue
import threading
import sys

from operator import itemgetter

from .sentiment_db import Sentimentdb
from utils.log import Log
from utils.redis import Redis

# 线程局部变量
thread_data = threading.local()

class Sentiment:

    def __init__(self):
        self.codes = queue.Queue()
        self.db    = Sentimentdb()
        self.log   = self.get_log()
        self.redis = self.get_redis()
        self.dates = queue.Queue()

        self.codes_total = 0
        self.dates_total = 0

    def get_redis(self):
        ''' 获取redis
        '''
        r = Redis()
        return r.r

    def get_log(self):
        ''' 设置日志文件
        '''
        logger_name = __name__
        file        = 'logs/sentiment.log'
        log         = Log(logger_name, file)

        return log.create_log()

    def get_codes_queue(self):
        ''' 组装codes队列
        '''
        t1 = time.time()
        codes = self.db.get_codes()

        for code in codes:
            symbol, code_type, *rest = code
            self.codes.put((symbol, code_type))

        t2 = time.time()
        t3 = t2 - t1
        self.codes_total = self.codes.qsize()
        self.log.info('Codes 队列组装完成，共 %d 股， 耗时：%.2fs' % (self.codes.qsize(), t3))
        print('Codes 队列组装完成， 共 %d 股，耗时：%.2f \n' % (self.codes.qsize(), t3))

    def get_hsi_date(self):
        ''' 获取HSI日期
        '''
        res = self.db.get_hsi_date()
        rows = res.rowcount

        if rows == 0:
            raise Exception('获取HSI日期失败：HSI日期返回空.')
        else:
            return res

    def chunk(self, data, ma):
        ''' 生成器函数，截取
        '''
        length = len(data) - ma
        for i in range(length):
            yield data[i:i+ma]

    def cal_ma(self, ma):
        ''' 计算ma
        ma: 20ma, 50ma
        '''
        while not self.codes.empty():
            symbol, code_type = self.codes.get()
            num = self.codes_total - self.codes.qsize()

            print('(%d/%d) 正在计算 %s HK MA' % (num, self.codes_total, symbol))
            sys.stdout.flush()

            t2   = time.time()
            res  = self.db.get_code_data(symbol)
            t3   = time.time()
            t4   = t3 - t2

            rows   = res.rowcount

            # 如果没有数据，跳过
            if rows == 0:
                continue

            for per in self.chunk(list(res), ma):
                date      = per[0].date
                now_price = per[0].value
                total     = 0

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

    def ma_main(self, ma=20):
        ''' 计算ma主线程
        '''
        t1          = time.time()
        thread_num  = 10
        thread_list = []

        # 组装codes队列
        self.get_codes_queue()

        print('正在计算 %dMA' % ma)
        for i in range(thread_num):
            thread_list.append(threading.Thread(target=self.cal_ma, args=(ma,)))

        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

        t2 = time.time()
        t3 = t2 - t1
        print('%dMA 计算完成, 耗时：%.2f \n' % (ma, t3))

    def get_date_queue(self):
        ''' 组装 HSI 日期队列
        '''
        t1 = time.time()
        hsi = self.get_hsi_date()

        for h in hsi:
            date = h.date
            self.dates.put(date)

        t2 = time.time()
        t3 = t2 - t1
        self.dates_total = self.dates.qsize()
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

    def senti_main(self, ma=20):
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
        ma = [20, 50]
        for i in ma:
            self.ma_main(i)
            self.senti_main(i)
