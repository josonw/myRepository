#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pay_game_coin_finace_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.pay_game_coin_finace_fct
        (
          fdate        date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fcointype    varchar(50),
          fdirection   varchar(50),
          ftype        varchar(50),
          fnum         bigint,
          fusernum     bigint,
          fpayusernum  bigint,
          fpaynum      bigint,
          fcnt         bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/pay_game_coin_finace_fct'
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

        res = self.hq.exe_sql("""use stage;""")
        if res != 0: return res

        hql = """    --
        insert overwrite table analysis.pay_game_coin_finace_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                ua.fcointype fcointype, ua.fdirection fdirection, ua.ftype ftype,
                sum(ua.fnum) fnum,
                sum(ua.fusernum) fusernum,
                sum(ua.fpayusernum) fpayusernum,
                sum(ua.fpaynum) fpaynum,
                sum(ua.fcnt) fcnt
            from (
                select fbpid, 'gamecoin' fcointype,
                    case when act_type=1 then 'in' else 'out' end fdirection,
                    act_id ftype, sum(abs(act_num)) fnum, count(distinct fuid) fusernum,
                    0 fpayusernum, 0 fpaynum, count(fuid) fcnt
                from stage.pb_gamecoins_stream_stg
                where dt="%(statdate)s" and act_type in (1, 2)
                group by fbpid, case when act_type=1 then 'in' else 'out' end, act_id
                    union all
                select p.fbpid fbpid, 'gamecoin' fcointype,
                    case when p.act_type=1 then 'in' else 'out' end fdirection,
                    p.ftype ftype, 0 fnum, 0 fusernum, sum(p.usernum) fpayusernum, sum(p.gc) fpaynum, 0 fcnt
                from (
                    select a.fbpid fbpid,  b.act_id ftype,  b.act_type act_type, sum(abs(b.act_num)) gc,
                        count(distinct b.fuid) usernum
                    from stage.user_pay_info a
                    join stage.pb_gamecoins_stream_stg b
                        on a.fbpid = b.fbpid and a.fuid = b.fuid and b.act_type in (1, 2)
                            and b.dt="%(statdate)s"
                    where a.dt="%(statdate)s"
                    group by a.fbpid, b.act_id, b.act_type
                ) p
                group by p.fbpid, 'gamecoin', case when p.act_type=1 then 'in' else 'out' end, p.ftype
            ) ua
            join analysis.bpid_platform_game_ver_map bpm
                on ua.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                ua.fcointype, ua.fdirection, ua.ftype
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
a = agg_pay_game_coin_finace_day(statDate, eid)
a()
