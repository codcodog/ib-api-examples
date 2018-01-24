import logging

from ib_insync import IB

from config.config import Config
from incre.crawl import Crawl
from incre.cal import Cal

if __name__ == '__main__':
    # 记录 IB 接口及其他错误信息
    logging.basicConfig(filename='logs/incre/error.log', level=logging.ERROR)

    ib = IB()
    ib.connect(Config.ib_host, Config.ib_port, Config.ib_client_id)

    # 增量爬取数据
    c = Crawl(ib)
    c.crawl_codes_data() # 爬取 stock 数据
    c.crawl_index_data() # 爬取 HSI 数据

    ib.disconnect()

    # 增量计算数据
    ca = Cal()
    ca.main()
