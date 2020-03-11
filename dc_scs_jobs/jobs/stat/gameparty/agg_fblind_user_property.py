#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_fblind_user_property(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_gamepaty_gamecoin
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fnum         bigint,
          fname        varchar(15),
          fpartyname   varchar(50),
          fusernum     bigint,
          fpname       varchar(100)
        )
        partitioned by(dt date)
        location '/dw/analysis/user_gamepaty_gamecoin'
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

        hql = """ -- 每天各个盲注场产用户当天的首次登录资产分布
        insert overwrite table analysis.user_gamepaty_gamecoin
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                fnum,
                fname ,
                fpartyname,
                count(fuid) fusernum,
                fpname
            from (
                select a.fbpid,
                    case
                         when user_gamecoins <= 0 then 0 --存number
                         when user_gamecoins >= 1 and user_gamecoins < 5000 then 1
                         when user_gamecoins >= 5000 and user_gamecoins < 10000 then  5000
                         when user_gamecoins >= 10000 and user_gamecoins < 50000 then  10000
                         when user_gamecoins >= 50000 and user_gamecoins < 100000 then  50000
                         when user_gamecoins >= 100000 and user_gamecoins < 500000 then  100000
                         when user_gamecoins >= 500000 and user_gamecoins < 1000000 then  500000
                         when user_gamecoins >= 1000000 and user_gamecoins < 5000000 then  1000000
                         when user_gamecoins >= 5000000 and user_gamecoins < 10000000 then  5000000
                         when user_gamecoins >= 10000000 and user_gamecoins < 50000000 then  10000000
                         when user_gamecoins >= 50000000 and user_gamecoins < 100000000 then  50000000
                         when user_gamecoins >= 100000000 and user_gamecoins < 1000000000 then 100000000
                    else 1000000000 end fnum,
                    case
                         when user_gamecoins <= 0 then  '0' --存string
                         when user_gamecoins >= 1 and user_gamecoins < 5000 then '1-5000'
                         when user_gamecoins >= 5000 and user_gamecoins < 10000 then '5000-1万'
                         when user_gamecoins >= 10000 and user_gamecoins < 50000 then  '1万-5万'
                         when user_gamecoins >= 50000 and user_gamecoins < 100000 then '5万-10万'
                         when user_gamecoins >= 100000 and user_gamecoins < 500000 then '10万-50万'
                         when user_gamecoins >= 500000 and user_gamecoins < 1000000 then '50万-100万'
                         when user_gamecoins >= 1000000 and user_gamecoins < 5000000 then '100万-500万'
                         when user_gamecoins >= 5000000 and user_gamecoins < 10000000 then '500万-1000万'
                         when user_gamecoins >= 10000000 and user_gamecoins < 50000000 then '1000万-5000万'
                         when user_gamecoins >= 50000000 and user_gamecoins < 100000000 then '5000万-1亿'
                         when user_gamecoins >= 100000000 and user_gamecoins < 1000000000 then '1亿-10亿'
                    else '10亿+' end fname,
                    fante fpartyname, a.fuid, fpname
                from (
                    select distinct fbpid, fuid, fante, fpname
                      from stage.user_gameparty_info_mid
                      where dt="%(statdate)s"
                ) a
                join (
                    select fbpid, fuid, user_gamecoins
                    from (
                        select fbpid, fuid, user_gamecoins,
                            row_number() over(partition by fbpid, fuid order by flogin_at, user_gamecoins asc) rown
                        from stage.user_login_stg
                        where dt="%(statdate)s"
                    ) ss
                    where ss.rown = 1
                ) b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
            ) c
            join analysis.bpid_platform_game_ver_map d
                on c.fbpid = d.fbpid
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum, fname, fpartyname, fpname
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
a = agg_fblind_user_property(statDate, eid)
a()
