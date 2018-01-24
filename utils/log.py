import logging

class Log:

    def __init__(self, logger_name, file):
        self.logger_name = logger_name
        self.file = file

    def create_log(self):
        ''' 创建日志
        '''
        # 创建 logger 实例
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(logging.DEBUG)

        # 创建 Formatter
        formatter = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')

        # 创建 Handler
        h = logging.FileHandler(self.file)
        h.setLevel(logging.DEBUG)
        h.setFormatter(formatter)

        logger.addHandler(h)

        return logger

