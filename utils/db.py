from urllib.parse import quote

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session

from config.config import Config

class DB:

    def __init__(self):
        '''初始化，连接数据库
        '''
        DB_CONN   = 'mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}'.format(user=Config.db_user, pwd=quote(Config.db_password), host=Config.db_host, db=Config.db_database, port=Config.db_port)
        self.engine = create_engine(DB_CONN, echo=False)
        self.conn = self.engine.connect()

    def query(self, sql):
        ''' 执行sql语句
        '''
        DBSession = sessionmaker(bind=self.engine)
        session = DBSession()
        result = session.execute(sql)
        session.commit()
        session.close()

        return result

    def multi_query(self, sql):
        ''' 多线程query
        '''
        session_factory = sessionmaker(bind=self.engine)
        Session = scoped_session(session_factory)

        result = Session().execute(sql)
        Session.commit()
        Session.remove()

        return result
