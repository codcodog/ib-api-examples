from utils.db import DB

class Basedb(DB):
    ''' base db 操作
    '''
    def get_codes(self, stock_id = None):
        ''' 获取股票基本信息
        '''
        if stock_id is None:
            sql = 'select `symbol`, `type` from `stock` order by `id`'
        else:
            sql = 'select `symbol`, `type` from `stock` where `id` > %d order by `id`' % stock_id
        result = self.conn.execute(sql)

        return result

    def set_record(self, date):
        ''' 设置record日期时间
        '''
        sql = 'insert into `records` (`type`, `api`, `level`, `date`) values ';
        sql += "('req', '5mStockReqHistoricalData', '5m', '{}')".format(date)
        res = self.query(sql)

        return res

