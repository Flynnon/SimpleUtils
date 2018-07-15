# SimpleUtils

日常工作中写的一些小的工具集合.

## DBUtils

Flynnon在公司的某一段时间是负责日志分析的,当时需要和数据库进行一定的交互,而每次现写SQL太麻烦了,现有的ORM都太重(sqlalchemy得定义表,Django的ORM更是必须跟Django工程配合使用)...因此就对MySQLdb做了一个简单的封装..  
后来感觉用起来还行.但是对于涉及到多个表的SQL始终没有想好建立怎样的抽象...再加上后来就去做别的事了,因此就没有再优化了.  

使用:

``` python
#!/usr/bin/env python
# -*- coding:utf-8 -*-

from __future__ import print_function

from collections import defaultdict

from DBUtils import DBUtil

# 数据库配置
db_config = {
    'HOST': '127.0.0.1',
    'PORT': 3306,
    'NAME': 'test',
    'USER': 'flynnon',
    'PASSWORD': 'flynnon',
}

dbutil = DBUtil(db_config=db_config)

# 查询一条记录
# 目前支持的关系有: eq - 等于, neq - 不等于, lt - 小于, gt - 大于, lte - 小于等于, gte - 大于等于, in - IN, no_in - NOT IN.可以自己扩展
user = dbutil.select_one('auth_user', ('id',), {'phone__neq': '11100000000'})
user_id = user['id'] if user else None


# 分批次查询,在查询数据很大的情况下可以减少内存占用
ONE_PAGE_SIZE = 200
for users in dbutil.select_yield('auth_user', ('user_id', 'phone'), {1:1},
                                   extends='ORDER BY id DESC', row_count=ONE_PAGE_SIZE):
    for user in users:
        user_id = user['id']
        phone = user['phone']
        print('id:', user_id, 'phone:', phone, sep=' ')

# 查询多条记录
users = dbutil.select_many('auth_user', ('id',), {1: 1})
user_ids = [user['id'] for user in users]

# 删除记录
delete_count = dbutil.delete('auth_user', {'id': 1})

# 更新记录
update_count = dbutil.update('auth_user', {'pay_count': 0}, {1: 1})

# 插入一条记录
insert_count = dbutil.insert_one('auth_user', {'phone': '12345678901', 'pay_count': 0})

# 插入多条记录
# 这里我建立的抽象是多个平行的列...可能看起来有些奇怪....
insert_data = defaultdict(list)
for line in open('/home/flynnon/test.txt', 'r'):
    user_id, phone, pay_count = line.split(' ')
    insert_data['id'].append(user_id)
    insert_data['phone'].append(phone)
    insert_data['pay_count'].append(pay_count)

insert_count = dbutil.insert_many('auth_user', insert_data)

# 执行复杂SQL(例如多表联查)... 就退化为直接操作cursor了....
sql = "SELECT a.phone AS phone, SUM(b.ordering_price) AS pay_sum FROM auth_user AS a, ordering AS b WHERE a.id = b.buyer_id GROUP BY a.id"

cursor = dbutil.get_cursor()
cursor.execute(sql)
results = cursor.fetchall()

for result in results:
    print('phone:', result['phone'], '总支付金额:', round(float(result['pay_sum']), 2), sep=' ')

# 关闭连接
dbutil.close()
```

## cache_decorator

在开发过程中,感觉有些操作可以缓存起来避免重复操作,因此写了这个装饰器.  
很简单的使用dict实现缓存,并且由于使用场景是在脚本中,因此没有(无需)支持多线程(可以很方便的加入锁支持),同样没有加入内存限制及淘汰机制.  
在Python3中,可以使用更加优秀的,官方实现的functools.lru_cache.  

使用:

``` python
#!/usr/bin/env python
# coding=utf-8

from decorator import cache

@cache()
def test():
    print('test')
    return 1

# 结果是仅仅输出了一次 test
test()
test()
```