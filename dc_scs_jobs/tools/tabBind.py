#-*- coding: UTF-8 -*-
import time
import sys
import os
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
import config
from libs.DB_Mysql import Connection as m_db

"""调度系统所有任务依赖重新生成 全表更新，需要先删除原有数据"""

mdb = m_db(  config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD  )

## 先把表备份一份

now = time.localtime()
#%d    当月的第几天 [01,31]
d = time.strftime("%d", now)
#%m    十进制月份[01,12]
m = time.strftime("%m", now)

table_bak = 'job_bind_%s_%s' % (m,d)
mdb.execute("DROP TABLE IF EXISTS `%s`" % table_bak)
mdb.execute("CREATE TABLE IF NOT EXISTS `job_bind_tmp` LIKE `job_bind`")
mdb.execute("RENAME TABLE `job_bind` TO `%s`" % table_bak )
mdb.execute("RENAME TABLE `job_bind_tmp` TO `job_bind`")


# 把依赖相关的信息组装成一个字典
jconfig = mdb.query("""SELECT * FROM `jconfig` WHERE `open` = 1""")

info = {}
for row in jconfig:
    tab_from = row['tab_from'].split(',')
    tab_into = row['tab_into'].split(',')
    info.update({row['jid']:{'tab_from':tab_from, 'tab_into':tab_into, 'jobtype':row['jobtype']} } )


in_sql = """ INSERT INTO `job_bind` (`cid`, `pid`) VALUES"""

for jid, val in info.iteritems():
    sql_tmp = ''

    for k, v in info.iteritems():
        if jid == k:
            continue

        for t in val['tab_from']:
            if t == '':
                continue
            if t in v['tab_into']:
                sql_tmp += ' (%s,  %s),' % (int(jid), int(k))
                break

    if int(jid) < 1000:
        sql_tmp += ' (%s,  1000),' % int(jid)

    if '' == sql_tmp:
        print '注意：%s 任务没有父任务！' % jid    ## 没有父任务的任务打印出来 注意确认

    if u'结果数据同步任务' == val['jobtype']:
        sql_tmp += ' (%s,  1002),' % int(jid)

    in_sql += sql_tmp


in_sql = in_sql[:-1]


f = open( '/data/other/scs_log/bind.sql', 'a')
print >> f, in_sql
f.close()

mdb.execute( in_sql )

#特殊处理


print 'Done!'
##END