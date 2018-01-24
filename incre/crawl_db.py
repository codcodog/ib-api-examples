from utils.db import DB

class Crawldb(DB):
    def get_hk_codes(self):
        ''' 获取香港股
        '''
        sql = 'select `symbol` from `stock` where `type` = "hk" order by `id`'
        res = self.query(sql)

        return res

    def get_records(self):
        ''' 获取记录
        '''
        sql = "select `date` from `records` where `type` = 'req' and `api` = 'stockReqHistoricalData' and `level` = '1d'"
        res = self.query(sql)

        return res

    def update_records(self, date):
        ''' 更新接口请求日期记录
        '''
        sql = "update `records` set `date` = '{}' where `type` = 'req' and `api` = 'stockReqHistoricalData' and `level` = '1d'".format(date)
        res = self.query(sql)

        return res.rowcount

    def get_hsi_record(self):
        ''' 获取 HSI 日期记录'''
        sql = "select `date` from `records` where `type` = 'req' and `api` = 'hsiReqHistoricalData' and `level` = '1d'"
        res = self.query(sql)

        return res

    def set_hsi_record(self, date):
        sql = "update `records` set `date` = '{}' where `type` = 'req' and `api` = 'hsiReqHistoricalData' and `level` = '1d'".format(date)
        res = self.query(sql)

        return res.rowcount
