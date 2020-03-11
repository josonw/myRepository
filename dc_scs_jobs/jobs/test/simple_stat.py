#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BaseStat


class set_job_test(BaseStat):

    def create_tab(self):
        pass

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':self.stat_date}

        #res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        #if res != 0: return res

        hql = """SELECT A.dt,A.fbpid,A.fuid,COUNT(A.fuid) FROM
                  (SELECT * FROM stage.user_gameparty_stg WHERE fbpid in
                 ('76065328F2EB2B306175EB56B4306EF7','A5B1E70B931A60F4FB2DBAA1D85B45B5')
                 and (dt>='2016-09-26' AND dt<='2016-09-26')
                 and fpname = '马股' and fsubname = 'LV100') A
                 GROUP BY A.dt,A.fbpid,A.fuid limit 5
              """

        res = self.hq.exe_sql(hql)
        return res



#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = set_job_test(statDate)
a()
