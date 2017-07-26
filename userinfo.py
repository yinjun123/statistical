# encoding=utf8

import sys, time
from link.main import *
reload(sys)
sys.setdefaultencoding('utf-8')

mysqlbendi = mysqlStorage()
mysqlSC = mysqlExtract()
def us_info():
    #一次查询出全部数据
    sql = "select DISTINCT user_id from t_order_info"  # 5秒  链接线上数据库表查出所有ID
    us_id = mysqlSC.get_all(sql)

    # 循环数据得到一个字典
    for i in us_id:

        sql = "select user_id,status,gmt_create,id,apply_nper,borrowing_amount from t_order_info where user_id = '%s' order by gmt_create" % \
              i['user_id']

        # 查询得到每个用户的所有订单信息
        shuju = {}
        c = 1
        dingdanlist = []
        for q in mysqlSC.get_all(sql):


            if q['status'] == 8:
                shuju['succeed_number'] = c
                c += 1

            else:
                shuju['succeed_number'] = -1  # 审核中
                c += 1

            # 判断申请天数，小于14天为0，大于为1，-1数据为空
            if q['apply_nper'] == 0:
                shuju['loan_length'] = -1
            elif q['apply_nper'] < 14:
                shuju['loan_length'] = 0
            else:
                shuju['loan_length'] = 1

            # 判断借款金额，小于1000为0，大于为1,-1数据为空
            if int(q['borrowing_amount']) == 0:
                shuju['loan_money'] = -1
            elif int(q['borrowing_amount']) < 1000:
                shuju['loan_money'] = 0
            else:
                shuju['loan_money'] = 1

            # 获得当前小时数
            a = time.strptime(str(q['gmt_create']), "%Y-%m-%d %H:%M:%S")
            # 申请时间（小时数）
            shuju['apply_length'] = a.tm_hour

            # USER_ID
            shuju['user_id'] = q['user_id']

            # 订单id,
            shuju['order_id'] = q['id']

            # 查询本地表
            sql = "select succeed_number,order_id from user_info where user_id = '%s' order by id" % (shuju['user_id'])

            sj = mysqlbendi.get_all(sql)

            # 无数据的情况 全部为N
            if sj == ():
                shuju['last_money'] = 'N'
                shuju['last_overdue_days'] = 'N'

            # 有数据
            else:
                num = len(sj)
                for i in sj:
                    l = 0
                    if i['succeed_number'] == -1:
                        l += 1
                        if l == num:
                            shuju['last_money'] = 'N'
                            shuju['last_overdue_days'] = 'N'

                    # 如果成功申请是老顾客,取出最后一条数据
                    else:
                        s = sj[-1]

                        sql = "select borrowing_amount,rec_date,reim_date,timeout_day from t_order_info where id = '%s' " %s['order_id']

                        money = mysqlSC.get_one(sql)
                        print money['borrowing_amount']
                        shuju['last_money'] = str(money['borrowing_amount'])
                        # 判断最后一条数据是否申请失败
                        if s['succeed_number'] == -1:
                            shuju['last_money'] = str(money['borrowing_amount'])
                            shuju['last_overdue_days'] = '申请失败'

                        # 是成功案例
                        else:
                            if money['timeout_day'] == 0:

                                if money['rec_date'] == None:  # 如果没有入账时间，默认上一笔逾期为0天
                                    shuju['last_overdue_days'] = '0'
                                    shuju['last_money'] = str(money['borrowing_amount'])
                                else:
                                    timeArray = time.strptime(str(money['rec_date']), "%Y-%m-%d %H:%M:%S")
                                    print timeArray
                                    timeArray1 = time.strptime(str(money['reim_date']), "%Y-%m-%d %H:%M:%S")
                                    timeStamp = int(time.mktime(timeArray))
                                    timeStamp1 = int(time.mktime(timeArray1))
                                    #超过24小时为2天
                                    t = int((timeStamp - timeStamp1) / 86400)   #-(int((timeStamp1 - timeStamp) / 3600))/24
                                    shuju['last_overdue_days'] = str(t)
                                    shuju['last_money'] = str(money['borrowing_amount'])

                            else:
                                shuju['last_overdue_days'] =str(money['timeout_day'])
                                shuju['last_money'] = str(money['borrowing_amount'])


                        break

            if shuju['user_id'] not in dingdanlist:
                dingdanlist.append(shuju['user_id'])
                # 查询school_pers表得到学历,0表示有学历，1数据为空，-1该用户为空
                sql = "select edu_level from t_school_pers_info where user_id = '%s'" % shuju['user_id']

                xueli = mysqlSC.get_one(sql)

                if xueli == None:
                    shuju['edu_level'] = -1
                else:
                    if xueli['edu_level'] != "" and xueli != '退学':
                        shuju['edu_level'] = 0
                    else:
                        shuju['edu_level'] = 1

                # 查询real_name表得到性别，出生日期
                sql = "select sex,date_birth from t_real_name where user_id = '%s'" % shuju['user_id']
                real = mysqlSC.get_one(sql)
                if real == None:
                    shuju['gender'] = -1
                    shuju['date_birth'] = '-1'
                else:
                    shuju['gender'] = real['sex']
                    shuju['date_birth'] = real['date_birth']

                # 查询user_info表得到职业和婚姻
                sql = "select professional,marriage from t_user_info where user_id = '%s'" % shuju['user_id']
                user_info = mysqlSC.get_one(sql)
                if user_info == None:
                    shuju['marriage'] = -1
                    shuju['professional'] = '-1'
                else:
                    if user_info['marriage'] == "未婚" and int(real['date_birth'][0:4]) < 1986:
                        shuju['marriage'] = 0
                    else:
                        shuju['marriage'] = 1
                    shuju['professional'] = user_info['professional']


                # 查询equipment_info判断该用户设备被几个用户使用
                sql = "select DISTINCT device_Id from t_equipment_info where user_id = '%s'" % shuju['user_id']
                equipment = mysqlSC.get_one(sql)

                if equipment == None:
                    shuju['binding_number'] = -1
                else:

                    sql1 = "select DISTINCT user_id from t_equipment_info where device_Id = '%s'" % equipment['device_Id']
                    num = mysqlSC.get_all(sql1)
                    number = len(num)
                    shuju['binding_number'] = number


                # 查询risk_control
                sql = "select risk_type,risk_reason from t_risk_control where user_id = '%s'" % shuju['user_id']
                risk = mysqlSC.get_all(sql)

                shuju['sesame_score'] = ""
                shuju['watermelon_score'] = ""
                shuju['ke_xin_score'] = ""
                if risk == ():
                    shuju['sesame_score'] = '-1'
                    shuju['watermelon_score'] = '-1'
                    shuju['ke_xin_score'] = '-1'
                else:
                    for i in risk:
                        if i['risk_type'] == 2:
                            shuju['sesame_score'] = i['risk_reason']

                        elif i['risk_type'] == 3:
                            shuju['watermelon_score'] = i['risk_reason']
                        elif i['risk_type'] == 4:
                            shuju['ke_xin_score'] = i['risk_reason']

                print shuju
                chunchu(shuju)


            else:
                print shuju
                chunchu(shuju)

def chunchu(shuju):

	sql = "INSERT INTO user_info (order_id,user_id,succeed_number,edu_level,gender,professional,marriage,date_birth,loan_length,loan_money,apply_length,binding_number,sesame_score,watermelon_score,ke_xin_score,last_money,last_overdue_days) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	print sql
	params = [shuju['order_id'],shuju['user_id'],shuju['succeed_number'],shuju['edu_level'],shuju['gender'],shuju['professional'],shuju['marriage'],shuju['date_birth'],shuju['loan_length'],shuju['loan_money'],shuju['apply_length'],shuju['binding_number'],shuju['sesame_score'],shuju['watermelon_score'],shuju['ke_xin_score'],shuju['last_money'],shuju['last_overdue_days']]
	print params

	a = mysqlbendi.insert(sql,params)
	print a
if __name__ == '__main__':
    us_info()
