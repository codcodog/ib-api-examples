import redis

from config.config import Config

class Redis:

    def __init__(self):
        ''' redis连接
        '''
        self.r = redis.StrictRedis(host=Config.redis_host, port=Config.redis_port)

