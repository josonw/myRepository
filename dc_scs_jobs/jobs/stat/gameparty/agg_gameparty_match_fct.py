#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_match_fct(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.gameparty_match_fct
        (
          fdate           date,
          fgamefsk        bigint,
          fplatformfsk    bigint,
          fversionfsk     bigint,
          fterminalfsk    bigint,
          fusernum        bigint,
          fpartynum       bigint,
          fplayusernum    bigint,
          f2partynum      bigint,
          ftrustee_num    bigint,
          fweedout_num    bigint,
          fbankrupt_num   bigint,
          fquit_num       bigint,
          fmatch_cnt      bigint,
          fmatch_usercnt  bigint,
          fregplayusernum bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/gameparty_match_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.gameparty_match_fct
        partition( dt="%(statdate)s" )

            select  "%(statdate)s" fdate,
                    b.fgamefsk fgamefsk,
                    b.fplatformfsk fplatformfsk,
                    b.fversionfsk fversionfsk,
                    b.fterminalfsk fterminalfsk,
                    count(a.fuid) fusernum,
                    count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
                    count(distinct a.fuid) fplayusernum,
                    count(distinct case when fpalyer_cnt = 2 then concat_ws('0', ftbl_id, finning_id) end) f2partynum,
                    count(distinct case when ftrustee_num=0 then null else a.fuid end) ftrustee_num,
                    count(distinct case when fis_weedout=0 then null else a.fuid end) fweedout_num,
                    count(distinct case when fis_bankrupt=0 then null else a.fuid end) fbankrupt_num,
                    count(distinct case when fis_end=0 then null else a.fuid end) fquit_num,
                    count(distinct case when fmatch_id='0' then null else fmatch_id end) fmatch_cnt,
                    count(distinct case when fmatch_id='0' then null else concat(a.fuid,fmatch_id) end) fmatch_usercnt,
                    count(distinct c.fuid) fregplayusernum
                from stage.user_gameparty_stg a
                left join stage.user_dim c
                   on c.fbpid = a.fbpid
                  and c.fuid = a.fuid
                  and c.dt="%(statdate)s"
                join analysis.bpid_platform_game_ver_map b
                    on a.fbpid = b.fbpid
                where a.dt="%(statdate)s" and fpalyer_cnt != 0 and fmatch_id is not null and fmatch_id !='0'
                group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk

        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
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
a = agg_gameparty_match_fct(statDate, eid)
a()
