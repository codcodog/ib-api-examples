from utils.log import Log
from utils.redis import Redis


def log(name, file):
    ''' 创建日志
    '''
    l = Log(name, file)
    return l.create_log()


def redis():
    ''' 创建redis
    '''
    r = Redis()
    return r.r

