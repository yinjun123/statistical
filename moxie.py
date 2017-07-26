# encoding=utf8
from db.mysqlHelp import MysqlHelper
from pymongo import MongoClient
from link.main import *
import sys, time, json, numpy, threading,Queue

reload(sys)
sys.setdefaultencoding('utf-8')


xinxi_queue = Queue.Queue()
dbzhixun = mongoExtract()
db = dbzhixun.moxie_transportation_contact_static
mysqlbendi = MysqlHelper('127.0.0.1', 3306, 'tongji_shuju', 'root', 'root')
mysqlSC = MysqlHelper('rr-bp11g011q3zl129iqo.mysql.rds.aliyuncs.com',3306,'zhixun_fws','ax_user0001','iZtBC2XWscAoFan3')
apply = False

def mongo(xinxi_queue):

    cursor = db.distinct('base_id')
    for i in cursor:
        xinxi_queue.put(i)

def MZ(jset,xinxi_queue):
    while not apply:
        i = xinxi_queue.get()
        print i
        moxie = {}

        # 得到user_id
        sql = "select user_id from t_sy_transport_base where id = '%s'"%i

        shuju = mysqlSC.get_all(sql)
        print 11111111111111111111111111111111
        print shuju
        print 22222222222222222222222222222222
        # 如果没有这个user_id退出循环
        if shuju == ():
            continue
        # 如果有
        moxie['t_user_id'] = shuju[0]['user_id']
        try:
            # 命中用户风险商户的个数
            moxie['hit_risk_number'] = db.find({'$and': [{'base_id': i}, {
                '$or': [{'group_name': '贷款公司'}, {'ensitive_type': {'$in': [2, 3]}}]}]}).count()

            # 运营商联系人与拒绝用户匹配的个数
            yuyinlist = yunyin_num(i, jset)
            moxie['operator_refuse_number'] = yuyinlist[1]

            # 通讯录联系人与拒绝用户匹配的个数
            tongdun = tongxun_num(shuju, jset)
            moxie['contact_refuse_number'] = tongdun[1]

            # 运营商top10中不在通讯录中的个数
            top = top10(i, tongdun[0])
            moxie['operator_not_in_number'] = top[1]

            # 有通话记录的通讯录个数
            moxie['call_number'] = txjilu(top[0])

            # 命中借贷宝热线次数
            moxie['jiedaibao_hit'] = jietai(yuyinlist[0])

            # ln(各个月通话（主叫+被叫）次数的标准差+1)
            moxie['call_number_poor'] = biaozhuncha(i)

            Mysql(moxie)
        except Exception, e:
            print 'Error', e
            continue


def yunyin_num(i,jset):
    yset = set()
    # 查询出这个用户通话的所有通话记录
    for n in db.find({'base_id': i, 'call_cnt_three_month': {'$gte': 4}}):
        # print i['phone']
        yset.add(n['peer_num'])
    yunyin = len(yset & jset)
    return [yset, yunyin]


def tongxun_num(shuju, jset):
    tset = set()
    jslist = dbzhixun.Contacts_Info.find({'user_id': shuju[0]['user_id']}, {'user_id': 0, '_id': 0})
    for i in jslist:

        for n in json.loads(i['json_str']):
            tset.add(n['phone'])
        tongdun = len(tset & jset)
        return [tset, tongdun]

def top10(i, tset):
    tpset = set()
    t = db.find({'base_id': i, 'user_name': {'$ne': ''}})
    tp = t.limit(10).sort('call_cnt_three_month', -1)

    for i in tp:
        tpset.add(i['peer_num'])
    top_num = len(tpset) - len(tpset & tset)
    return [t, top_num]


def txjilu(tx):
    num = tx.count()
    return num

def biaozhuncha(i):
    # find({'$and': [{'base_id':i['base_id']},{'$or': [{'month': '2017-06'}, {'month': '2017-05'}, {'month': '2017-04'}, {'month': '2017-03'},{'month': '2017-02'}]}]})
    two = 0
    three = 0
    four = 0
    five = 0
    six = 0
    bzlist = []
    for i in dbzhixun.moxie_transportation_contact_detail.find({'$and': [{'base_id': i}, {
        '$or': [{'month': '2017-06'}, {'month': '2017-05'}, {'month': '2017-04'}, {'month': '2017-03'},
                {'month': '2017-02'}]}]}):
        if i['month'] == '2017-06':
            six += 1
        elif i['month'] == '2017-05':
            five += 1
        elif i['month'] == '2017-04':
            four += 1
        elif i['month'] == '2017-03':
            three += 1
        elif i['month'] == '2017-02':
            two += 1
    bzlist.append(six)
    bzlist.append(five)
    bzlist.append(four)
    bzlist.append(three)
    bzlist.append(two)

    cha_num = numpy.std(bzlist) + 1
    return str(cha_num)


def jietai(yset):
    jiedaitset = set(['4001006699', '01066010650', '66010650', '01059842345'])
    return len(jiedaitset & yset)


def jujue_list():
    jset = set()
    sql = "SELECT DISTINCT account FROM t_client_user as a,(select user_id from t_order_info group by user_id having sum(review_status in (1,3)) = 0) as newtable WHERE a.id = newtable.user_id"

    alist = mysqlSC.get_all(sql)
    for i in alist:
        jset.add(i['account'])
    return jset


def Mysql(moxie):
    sql = "INSERT INTO moxie_statistics (m_user_id,hit_risk_number,operator_refuse_number,contact_refuse_number,call_number_poor,operator_not_in_number,call_number,jiedaibao_hit) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

    params = [moxie['t_user_id'], moxie['hit_risk_number'], moxie['operator_refuse_number'],
              moxie['contact_refuse_number'], moxie['call_number_poor'], moxie['operator_not_in_number'],
              moxie['call_number'], moxie['jiedaibao_hit']]

    a = mysqlbendi.insert(sql, params)
    print a

if __name__ == '__main__':

    jset = jujue_list()
    thread = threading.Thread(target=mongo,args=(xinxi_queue,))
    thread.start()
    chaxunthreads = []
    for i in range(1):
        thread = threading.Thread(target=MZ, args=(jset,xinxi_queue,))
        thread.start()
        chaxunthreads.append(thread)
    thread.join()
    while not xinxi_queue.empty():
         pass
    apply = True
    for i in chaxunthreads:
        i.join()
    print "主线程退出"
