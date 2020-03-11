#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_gameparty_info_mid(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists stage.user_gameparty_info_mid
        (
          fdate                date,
          fbpid                   string,
          fuid                bigint,
          fpname              string,
          fsubname            string,
          fante                   string,
          fparty_num          decimal(32),
          fcharge             decimal(32),
          fwin_num            decimal(32),
          flose_num               decimal(32),
          fwin_party_num      decimal(32),
          flose_party_num     decimal(32),
          fplaytime               decimal(32)
        )
        partitioned by(dt date)
        stored as orc
        location '/dw/stage/user_gameparty_info_mid'
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

        hql = """    -- 牌局中间表
        insert overwrite table stage.user_gameparty_info_mid
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate, fbpid, fuid, fpname, fsubname, fblind_1 fante, count(1) fparty_num,
            sum( fcharge ) fcharge, sum( case when fgamecoins > 0 then fgamecoins else 0 end ) fwin_num,
            sum( case when fgamecoins < 0 then abs(fgamecoins) else 0 end) flose_num,
            count( case when fgamecoins > 0 then fgamecoins else null end) fwin_party_num,
            count( case when fgamecoins < 0 then fgamecoins else null end) flose_party_num,
            sum( case when fs_timer = '1970-01-01 00:00:00' then 0
                    when fe_timer = '1970-01-01 00:00:00' then 0
                else unix_timestamp(fe_timer)-unix_timestamp(fs_timer) end ) fplaytime
        from stage.user_gameparty_stg
        where dt = "%(statdate)s"
        group by fbpid, fuid, fpname, fsubname, fblind_1
        """    % query
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
a = load_gameparty_info_mid(statDate, eid)
a()
