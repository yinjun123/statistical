#encoding=utf8
from db.mysqlHelp import MysqlHelper
from pymongo import MongoClient
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def mysqlExtract():
    return MysqlHelper('rr-bp11g011q3zl129iqo.mysql.rds.aliyuncs.com',3306,'zhixun_fws','ax_user0001','iZtBC2XWscAoFan3')
def mysqlStorage():

    return MysqlHelper('127.0.0.1', 3306, 'tongji_shuju', 'root', 'root')

def mongoExtract():
    client = MongoClient('116.62.59.61', 27017)
    # 建立和数据库系统的连接,创建Connection时，指定host及port参数
    db_auth = client.zhixun
    db = db_auth.authenticate("zhixun", "zhixun@product0401")
    return client.zhixun