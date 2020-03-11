#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_gameparty_uid_playcnt_mid(BaseStat):

    def create_tab(self):
        hql = """
        use stage;
        create table if not exists stage.gameparty_uid_playcnt_mid
        (
          fbpid       string,
          fname       string,
          ftype       int,
          fuid        bigint,
          fplaycnt    bigint,
          fmust_blind bigint
        )
        partitioned by (dt date)
        location '/dw/stage/gameparty_uid_playcnt_mid'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        insert overwrite table stage.gameparty_uid_playcnt_mid
        partition( dt="%(statdate)s" )
            select fbpid, fname, ftype, fuid, count(1) fplaycnt, fmust_blind
            from stage.finished_gameparty_uid_mid
            where dt = "%(statdate)s"
            group by fbpid, fname, ftype, fuid, fmust_blind
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_gameparty_uid_playcnt_mid(statDate, eid)
a()
