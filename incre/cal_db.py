from utils.db import DB
from cal.sentiment.sentiment_db import Sentimentdb


class Caldb(DB):
    def get_codes(self):
        ''' 获取 codes
        '''
        sql = "select `symbol`, `type` from `stock` where `type` = 'hk' order by `id`"
        res = self.query(sql)

        return res

    def get_code_data(self, code, limit):
        ''' 获取 code 数据
        '''
        sql = "select `date`, `value` from `close` where `code`='{}' and `code_type` = 'hk' order by `date` DESC limit {}".format(code, limit)
        res = self.multi_query(sql)

        return res

    def get_record(self):
        ''' 获取记录日期
        '''
        sql = "select `date` from `records` where `type` = 'cal' and `api` = 'calSentiment' and `level` = '1d'"
        res = self.query(sql)

        return res

    def get_hsi_date(self, record_date):
        ''' 获取 HSI 日期
        '''
        sql = "select `date` from `hsi` where `date` > '{}' order by `date` DESC".format(record_date)
        res = self.query(sql)

        return res

    def set_record_date(self, date):
        ''' 更新 record date
        '''
        sql = "update `records` set `date` = '{}' where `type` = 'cal' and `api` = 'calSentiment' and `level` = '1d'".format(date)
        res = self.query(sql)

        return res.rowcount
