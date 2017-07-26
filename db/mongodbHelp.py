#coding=utf8

from pymongo import MongoClient

client = MongoClient('116.62.59.61', 27017)
#建立和数据库系统的连接,创建Connection时，指定host及port参数
db_auth = client.zhixun
db = db_auth.authenticate("zhixun", "zhixun@product0401")
#admin 数据库有帐号，连接-认证-切换库

#连接数据库
# for i in range(10):
#     u = dict(name = 'user'+str(i),age = 10+i)
#     db.insert(u)
class Field(object):
    def __init__(self,name):
        self.name = name
    # 小于
    def __lt__(self, value):

        return { self.name: { "$lt":value } }
    # 小于等于
    def __le__(self, value):

        return { self.name: { "$lte":value } }

    # 大于
    def __gt__(self, value):

        return { self.name: { "$gt":value } }
    # 大于等于
    def __ge__(self, value):

        return { self.name: { "$gte":value } }

    # 等于
    def __eq__(self, value):

        return { self.name: value }
    # 不等于
    def __ne__(self, value):

        return { self.name: { "$ne":value } }

    # in (由于 in 是关键字，故该用首字母大写来避免冲突)
    def In(self, *value):

        return { self.name: { "$in":value } }
    # not in
    def not_in(self, *value):

        return { self.name: { "$nin":value } }

    def all(self, *value):
        '''
        注意和 in 的区别。in 是检查目标属性值是条件表达式中的一员，而 all 则要求属性值包含全部条件元素。
        '''
        return { self.name: { "$all":value } }

    def size(self, value):
        '''
        匹配数组属性元素的数量
        '''
        return { self.name: { "$size":value } }

    def type(self, value):
        '''
        判断属性类型
        @param value 可以是类型码数字，也可以是类型的字符串
        '''
        # int 类型，则认为是属性类型的编码，不再做其它处理
        if type(value) is int and value >= 1 and value <= 255:
            return { self.name: { "$type":value } }
        if type(value) is str:
            value = value.strip().lower()
            code = 2 # 默认为字符串类型
            # 数字类型
            if value in ("int", "integer", "long", "float", "double", "short", "byte", "number"):
                code = 1
            # 字符串类型
            elif value in ("str", "string", "unicode"):
                code = 2
            # object 类型
            elif value == "object":
                code = 3
            # array 类型
            elif value in ("array", "list", "tuple"):
                code = 4
            # binary data 类型
            elif value in ("binary data", "binary"):
                code = 5
            # object id 类型
            elif value in ("object id", "id"):
                code = 7
            # boolean 类型
            elif value in ("boolean", "bool"):
                code = 8
            # date 类型
            elif value == "date":
                code = 9
            # null 类型
            elif value in ("null", "none"):
                code = 10
            # regular expression 类型
            elif value in ("regular expression", "regular"):
                code = 11
            # javascript code 类型
            elif value in ("javascript code", "javascript", "script"):
                code = 13
            # symbol 类型
            elif value == "symbol":
                code = 14
            # javascript code with scope 类型
            elif value == "javascript code with scope":
                code = 15
            # 32-bit integer 类型
            elif value in ("32-bit integer", "32-bit"):
                code = 16
            # timestamp 类型
            elif value in ("timestamp", "time"):
                code = 17
            # 64-bit integer 类型
            elif value in ("64-bit integer", "64-bit"):
                code = 18
            # min key 类型
            elif value == "min key":
                code = 255
            # max key 类型
            elif value == "max key":
                code = 127
            return { self.name: { "$type":code } }
if __name__=="__main__":
    age = Field("age")
    for u in db.contanct666_count.find(age < 15):
        print u