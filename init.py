import logging

from init.base import Base
from init.basem import Basem

class Init:
    def __init__(self):
        logging.basicConfig(filename='logs/error.log', level=logging.ERROR)
        self.base = Base()

    def import_codes(self):
        ''' 初始化
        csv文件导入股票基本信息,
        '''
        self.base.hk_to_db()

    def get_codes_data(self):
        ''' 导入股票数据
        '''
        self.base.get_codes_data()

    def continue_codes_data(self):
        ''' 断点续传股票数据
        '''
        stocks = ['82822', '87001', '83188', '246', '8293', '2821', '593', '1706', '625', '8292', '8455', '8029', '1561', '8039', '8170', '2289', '8465', '83199', '2203', '6083', '8059', '1557', '82843', '8321', '3789', '82811', '8275', '8027', '2863', '1667', '2193', '8429', '2663', '1552', '8353', '8231', '8365', '8240', '83147', '8369', '83128', '8460', '8430', '986', '83074', '8406', '8013', '8491', '8375', '8035', '8346', '8415', '8402', '83012', '83095', '83127', '83100', '83118', '8139', '8222', '8161', '83170', '83162', '82834', '83168', '83146', '83155', '83122', '82847', '83156', '82808', '3153', '82832', '83129', '83132', '83008', '770', '83149', '83136', '83137', '83107', '3054', '83180', '83120', '83150']
        self.base.continue_codes_data(stocks)

    def get_index_data(self):
        ''' 导入 HSI Index 数据
        '''
        symbol   = 'HSI'
        exchange = 'HKFE'
        currency = 'HKD'
        self.base.get_index_data(symbol, exchange, currency)

    def get_codes_data_5m(self):
        ''' 获取股票交易信息，1个月5分钟
        '''
        b = Basem()
        b.get_codes_data()
        b.get_hsi_data()

if __name__ == '__main__':
    init = Init()
    init.get_codes_data_5m()
