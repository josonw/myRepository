#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_bankrupt_pouring_day(BaseStat):

    def create_tab(self):
        if "TEZ" == self.hq.engine:
            self.hq.exe_sql("set hive.exec.reducers.bytes.per.reducer=10240000;")

        """ 底注分布 """
        hql = """
        use analysis;
        create table if not exists analysis.user_bankrupt_pouring_fct
        (
          fdate                  date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          pouring_num            bigint,
          fbankruptusercnt       bigint,
          fbankruptcnt           bigint,
          fbank_30min_payusernum bigint,
          fbank_30min_income     decimal(20,2),
          fbank_30min_paycnt     bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_bankrupt_pouring_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        """ 场次分布 """
        hql = """
        use analysis;
        create table if not exists analysis.user_bankrupt_partytype_fct
        (
          fdate                  date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fbankruptusercnt       bigint,
          fbankruptcnt           bigint,
          partytype              string,
          fbank_30min_payusernum bigint,
          fbank_30min_income     decimal(20,2),
          fbank_30min_paycnt     bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_bankrupt_partytype_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        """ 场景分布 """
        hql = """
        use analysis;
        create table if not exists analysis.user_bankrupt_scene_fct
        (
          fdate                  date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          bankruptscene          string,
          fbankruptusercnt       bigint,
          fbankruptcnt           bigint,
          fbank_30min_payusernum bigint,
          fbank_30min_income     decimal(20,2),
          fbank_30min_paycnt     bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_bankrupt_scene_fct'
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

        hql = """    -- 插入最新的游戏场名称
        insert into table analysis.bankrupt_partytype_dim
            select c.fname fsk, c.fname fname, c.fgamefsk, null fplatformfsk, null fversionfsk, null fterminalfsk
            from (
                select nvl(fplayground_title, '未定义') fname, fgamefsk
                from stage.user_bankrupt_stg a
                join analysis.bpid_platform_game_ver_map b
                    on a.fbpid = b.fbpid
                where a.dt="%(statdate)s"
                group by nvl(fplayground_title, '未定义'), fgamefsk
            ) c
            left outer join analysis.bankrupt_partytype_dim d
                on c.fname = d.fname and c.fgamefsk = d.fgamefsk
            where d.fname is null
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    --
        insert overwrite table analysis.user_bankrupt_pouring_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, pouring_num,
                sum(fbankruptusercnt) fbankruptusercnt,
                sum(fbankruptcnt) fbankruptcnt,
                sum(fbank_30min_payusernum) fbank_30min_payusernum,
                sum(fbank_30min_income) fbank_30min_income,
                sum(fbank_30min_paycnt) fbank_30min_paycnt
            from (
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    fuphill_pouring pouring_num, count(distinct fuid) fbankruptusercnt,
                    count(1) fbankruptcnt, null fbank_30min_payusernum, null fbank_30min_income,
                    null fbank_30min_paycnt
                from stage.user_bankrupt_stg a
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where a.dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fuphill_pouring
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    fuphill_pouring pouring_num, null fbankruptusercnt, null fbankruptcnt,
                    count(distinct c.fplatform_uid) fbank_30min_payusernum,
                    sum(nvl(round(c.fcoins_num * c.frate, 2), 0)) fbank_30min_income,
                    count(distinct forder_id) fbank_30min_paycnt
                from stage.user_bankrupt_stg a
                join stage.pay_user_mid b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid
                join stage.payment_stream_stg c
                    on b.fbpid = c.fbpid and b.fplatform_uid = c.fplatform_uid and c.dt="%(statdate)s"
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where a.dt="%(statdate)s" and unix_timestamp(c.fdate)-1800 <= unix_timestamp(a.frupt_at)
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fuphill_pouring
            ) src
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, pouring_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        insert overwrite table analysis.user_bankrupt_partytype_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                sum(fbankruptusercnt) fbankruptusercnt,
                sum(fbankruptcnt) fbankruptcnt,
                partytype,
                sum(fbank_30min_payusernum) fbank_30min_payusernum,
                sum(fbank_30min_income) fbank_30min_income,
                sum(fbank_30min_paycnt) fbank_30min_paycnt
            from (
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    count(distinct fuid) fbankruptusercnt, count(1) fbankruptcnt,
                    nvl(fplayground_title, '未定义') partytype,
                    null fbank_30min_payusernum,
                    null fbank_30min_income, null fbank_30min_paycnt
                from stage.user_bankrupt_stg a
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(fplayground_title, '未定义')
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    null fbankruptusercnt, null fbankruptcnt,
                    nvl(fplayground_title, '未定义') partytype,
                    count(distinct c.fplatform_uid) fbank_30min_payusernum,
                    sum(nvl(round(fcoins_num * frate, 2), 0)) fbank_30min_income,
                    count(distinct forder_id) fbank_30min_paycnt
                from stage.user_bankrupt_stg a
                join stage.pay_user_mid b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid
                join stage.payment_stream_stg c
                    on b.fbpid = c.fbpid and b.fplatform_uid = c.fplatform_uid and c.dt="%(statdate)s"
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where a.dt="%(statdate)s" and unix_timestamp(c.fdate)-1800 <= unix_timestamp(a.frupt_at)
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(fplayground_title, '未定义')
            ) src
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, partytype
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        insert overwrite table analysis.user_bankrupt_scene_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, bankruptscene,
                sum(fbankruptusercnt) fbankruptusercnt,
                sum(fbankruptcnt) fbankruptcnt,
                sum(fbank_30min_payusernum) fbank_30min_payusernum,
                sum(fbank_30min_income) fbank_30min_income,
                sum(fbank_30min_paycnt) fbank_30min_paycnt
            from (
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    count(distinct fuid) fbankruptusercnt, count(1) fbankruptcnt,
                    nvl(fscene, '未定义') bankruptscene,
                    null fbank_30min_payusernum,
                    null fbank_30min_income, null fbank_30min_paycnt
                from stage.user_bankrupt_stg a
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where dt="%(statdate)s"
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(fscene, '未定义')
                    union all
                select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                    null fbankruptusercnt, null fbankruptcnt,
                    nvl(fscene, '未定义') bankruptscene,
                    count(distinct c.fplatform_uid) fbank_30min_payusernum,
                    sum(nvl(round(fcoins_num * frate, 2), 0)) fbank_30min_income,
                    count(distinct forder_id) fbank_30min_paycnt
                from stage.user_bankrupt_stg a
                join stage.pay_user_mid b
                    on a.fbpid = b.fbpid and a.fuid = b.fuid
                join stage.payment_stream_stg c
                    on b.fbpid = c.fbpid and b.fplatform_uid = c.fplatform_uid and c.dt="%(statdate)s"
                join analysis.bpid_platform_game_ver_map bpm
                    on a.fbpid = bpm.fbpid
                where a.dt="%(statdate)s" and unix_timestamp(c.fdate)-1800 <= unix_timestamp(a.frupt_at)
                group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(fscene, '未定义')
            ) src
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, bankruptscene
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
a = agg_user_bankrupt_pouring_day(statDate, eid)
a()
