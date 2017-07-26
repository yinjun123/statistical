# encoding=utf8
from link.main import *
import sys, time
reload(sys)
sys.setdefaultencoding('utf-8')

def mongo(db, mysqlSC, mysqlbendi):
    cursor = db.tongdun_risk_table.find({}, no_cursor_timeout=True)
    for tongdun in cursor:

        print tongdun['userId']
        sql = "select count(*) as num from t_order_info where user_id = '%s'" % tongdun['userId']
        shu = mysqlSC.get_one(sql)
        print shu
        if shu['num'] != None:
            chunchu(tongdun, mysqlbendi)
        else:
            continue
    cursor.close()
def chunchu(tongdun, mysqlbendi):
    sql = "INSERT INTO tongdun_statistics1 (td_user_id,financial_loans_p,p2p_loans_p,bank_loans_p,tongdun_score,phone_loans_number) VALUES (%s,%s,%s,%s,%s,%s)"
    params = [tongdun['userId'], tongdun['TD33111'], tongdun['TD33131'], tongdun['TD33121'], tongdun['TD10000'],
              tongdun['TD33000']]
    a = mysqlbendi.insert(sql, params)
    print a

def main():
    mysqlSC = mysqlExtract()
    mysqlbendi = mysqlStorage()
    db = mongoExtract()
    mongo(db, mysqlSC, mysqlbendi)

if __name__ == '__main__':
    main()