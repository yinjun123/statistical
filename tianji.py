#encoding=utf8
from link.main import *
import sys,time,json,numpy,threading,Queue
reload(sys)
sys.setdefaultencoding('utf-8')

xinxi_queue = Queue.Queue()
dbzhixun = mongoExtract()
db = dbzhixun.tianji_call_contact_static
mysqlbendi = mysqlStorage()
mysqlSC = mysqlExtract()
apply = False
baselist = []

def mongo(xinxi_queue):

    cursor = db.distinct('baseId',no_cursor_timeout=True)

    for i in cursor:
        xinxi_queue.put(i)

def MZ(jset,xinxi_queue):

    while not apply:
        i = xinxi_queue.get()

        tianji = {}

        #得到user_id
        sql = "select user_id from t_tianji_base where id = '%s'"%i

        shuju = mysqlSC.get_all(sql)

        #如果没有这个user_id退出循环
        if shuju ==():
            continue
        #如果有
        tianji['t_user_id'] = shuju[0]['user_id']
        try:
            #命中用户风险商户的个数
            tianji['hit_risk_number'] = db.find({'$and':[{'baseId':i},{'$or':[{'phone_label':'贷款公司'},{'ensitive_type':{'$in' : [2,3]}}]}]}).count()

            #运营商联系人与拒绝用户匹配的个数
            yuyinlist = yunyin_num(i,jset)
            tianji['operator_refuse_number'] = yuyinlist[1]



            #通讯录联系人与拒绝用户匹配的个数
            tongdun = tongxun_num(shuju[0]['user_id'],jset)
            tianji['contact_refuse_number'] = tongdun[1]


            #运营商top10中不在通讯录中的个数
            top = top10(i,tongdun[0])
            tianji['operator_not_in_number'] = top[1]

            #有通话记录的通讯录个数
            tianji['call_number'] = txjilu(top[0])

            #命中借贷宝热线次数
            tianji['jiedaibao_hit'] = jiedai(yuyinlist[0])


            #ln(各个月通话（主叫+被叫）次数的标准差+1)
            tianji['call_number_poor'] = biaozhuncha(i)

            print tianji
            Mysql(tianji)
        except Exception, e:
            print 'Error', e
            continue


def yunyin_num(i,jset):
    yset = set()

    for i in db.find({'baseId':i,'cantact_3m':{'$gte':4}}):

        yset.add(i['phone'])
    yunyin = len(yset & jset)
    return [yset,yunyin]


def tongxun_num(shuju,jset):
    tset = set()
    joist = dbzhixun.Contacts_Info.find({'user_id':shuju},{'user_id':0,'_id':0})
    for i in joist:
        for n in json.loads(i['json_str']):
            tset.add(n['phone'])
            tongdun = len(tset & jset)
        return [tset,tongdun]

def top10(i,tset):
    tpset = set()
    t = db.find({'baseId':i,'remark':{'$ne':''}})
    tp = t.limit(10).sort('contact_3m', -1)

    for i in tp:
        tpset.add(i['phone'])
    top_num = len(tpset)-len(tpset & tset)
    return [t,top_num]
def txjilu(tx):
    num = tx.count()
    return num

def biaozhuncha(i):
    bzlist = []
    for i in dbzhixun.tianji_call_contact_statistics.find({'baseId':i}):
        print i
        if i['month_d'] == '2017-06' or i['month_d'] == '2017-05' or i['month_d'] =='2017-04' or i['month_d'] =='2017-03' or i['month_d'] =='2017-02':
            bzlist.append(i['talk_cnt'])
    while len(bzlist)<5:
        bzlist.append(0)
    cha_num = numpy.std(bzlist) + 1
    return str(cha_num)

def jiedai(yset):
    jiedaitset = set(['4001006699','01066010650','66010650','01059842345'])
    return len(jiedaitset & yset)

def jujue_list():
    jset = set()
    sql = "SELECT DISTINCT account FROM t_client_user as a,(select user_id from t_order_info group by user_id having sum(review_status in (1,3)) = 0) as newtable WHERE a.id = newtable.user_id"

    alist = mysqlSC.get_all(sql)
    for i in alist:
        jset.add(i['account'])
    return jset
def Mysql(tianji):

    sql = "INSERT INTO tianji_statistics (t_user_id,hit_risk_number,operator_refuse_number,contact_refuse_number,call_number_poor,operator_not_in_number,call_number,jiedaibao_hit) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"

    params = [tianji['t_user_id'],tianji['hit_risk_number'],tianji['operator_refuse_number'],tianji['contact_refuse_number'],tianji['call_number_poor'],tianji['operator_not_in_number'],tianji['call_number'],tianji['jiedaibao_hit']]

    a= mysqlbendi.insert(sql,params)
    print a
if __name__=='__main__':
    #返回被拒绝用户电话号码集合
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