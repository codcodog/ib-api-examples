from utils.db import DB

class Sentimentdb(DB):

    def get_codes(self):
        ''' 获取codes
        '''
        sql = "select distinct `symbol`, `type` from `stock` where `type` = 'hk' order by `id`"
        res = self.query(sql)

        return res

    def get_hsi_date(self):
        ''' 获取 HSI 日期
        '''
        sql = "select `date` from `hsi` order by `date` DESC"
        res = self.query(sql)

        return res

    def get_code_data(self, code):
        ''' 获取code数据
        '''
        sql = "select `date`, `value` from `close` where `code`={} and `code_type` = 'hk' order by `date` DESC".format(code)
        res = self.multi_query(sql)

        return res
