#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_bankrupt_num_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_bankrupt_num_fct
        (
          fdate            date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          bank_num         bigint,
          fbankruptusercnt bigint,
          frupt_actcnt     bigint,
          f1drupt_actcnt   bigint,
          f7drupt_actcnt   bigint,
          f30drupt_actcnt  bigint,
          frupt_dsucnt     bigint,
          f1drupt_dsucnt   bigint,
          f7drupt_dsucnt   bigint,
          f30drupt_dsucnt  bigint,
          frupt_paycnt     bigint,
          f1drupt_paycnt   bigint,
          f7drupt_paycnt   bigint,
          f30drupt_paycnt  bigint,
          frupt_paidcnt    bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_bankrupt_num_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        query = { 'statdate':statDate, "num_begin": statDate.replace('-', ''), 'ld_1dayago': PublicFunc.add_days(statDate, -1),
                'ld_7dayago': PublicFunc.add_days(statDate, -7), 'ld_30dayago': PublicFunc.add_days(statDate, -30)
            }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    -- 当日破产用户数
        drop table if exists stage.agg_user_bankrupt_num_data_%(num_begin)s;

        create table stage.agg_user_bankrupt_num_data_%(num_begin)s as
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk, a.bankrupt_num bank_num,
                count(a.fuid) fbankruptusercnt,
                0 frupt_actcnt, 0 f1drupt_actcnt, 0 f7drupt_actcnt, 0 f30drupt_actcnt,
                0 frupt_dsucnt, 0 f1drupt_dsucnt, 0 f7drupt_dsucnt, 0 f30drupt_dsucnt,
                0 frupt_paycnt, 0 f1drupt_paycnt, 0 f7drupt_paycnt, 0 f30drupt_paycnt,
                0 frupt_paidcnt
            from (
                select fbpid, fuid, count(1) bankrupt_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, a.bankrupt_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 1、7、30 日回头用户破产次数
        insert into table stage.agg_user_bankrupt_num_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk, bpm.fversionfsk fversionfsk,
                bpm.fterminalfsk fterminalfsk, a.bankrupt_num bank_num,
                0 fbankruptusercnt,
                count( case when b.dt = "%(statdate)s" then b.fuid end) frupt_actcnt,
                count( case when b.dt = "%(ld_1dayago)s" then b.fuid end) f1drupt_actcnt,
                count( case when b.dt = "%(ld_7dayago)s" then b.fuid end) f7drupt_actcnt,
                count( case when b.dt = "%(ld_30dayago)s" then b.fuid end) f30drupt_actcnt,
                0 frupt_dsucnt, 0 f1drupt_dsucnt, 0 f7drupt_dsucnt, 0 f30drupt_dsucnt,
                0 frupt_paycnt, 0 f1drupt_paycnt, 0 f7drupt_paycnt, 0 f30drupt_paycnt,
                0 frupt_paidcnt
            from (
                select fbpid, fuid, count(1) bankrupt_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join stage.active_user_mid b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
                and cast(b.dt as string) in ("%(statdate)s", "%(ld_1dayago)s", "%(ld_7dayago)s", "%(ld_30dayago)s")
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, a.bankrupt_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 1、7、30 日注册用户破产次数
        insert into table stage.agg_user_bankrupt_num_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk, bpm.fversionfsk fversionfsk,
                bpm.fterminalfsk fterminalfsk, a.bankrupt_num bank_num,
                0 fbankruptusercnt,
                0 frupt_actcnt, 0 f1drupt_actcnt, 0 f7drupt_actcnt, 0 f30drupt_actcnt,
                count(case when b.dt="%(statdate)s" then b.fuid end ) frupt_dsucnt,
                count(case when b.dt="%(ld_1dayago)s" then b.fuid end ) f1drupt_dsucnt,
                count(case when b.dt="%(ld_7dayago)s" then b.fuid end ) f7drupt_dsucnt,
                count(case when b.dt="%(ld_30dayago)s" then b.fuid end) f30drupt_dsucnt,
                0 frupt_paycnt, 0 f1drupt_paycnt, 0 f7drupt_paycnt, 0 f30drupt_paycnt,
                0 frupt_paidcnt
            from (
                select fbpid, fuid, count(1) bankrupt_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join stage.user_dim b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
                and cast(b.dt as string) in ("%(statdate)s", "%(ld_1dayago)s", "%(ld_7dayago)s", "%(ld_30dayago)s")
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, a.bankrupt_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 1、7、30 日付费用户破产次数
        insert into table stage.agg_user_bankrupt_num_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk, bpm.fversionfsk fversionfsk,
                bpm.fterminalfsk fterminalfsk, a.bankrupt_num bank_num,
                0 fbankruptusercnt,
                0 frupt_actcnt, 0 f1drupt_actcnt, 0 f7drupt_actcnt, 0 f30drupt_actcnt,
                0 frupt_dsucnt, 0 f1drupt_dsucnt, 0 f7drupt_dsucnt, 0 f30drupt_dsucnt,
                count( case when b.dt="%(statdate)s" then b.fuid end ) frupt_paycnt,
                count( case when b.dt="%(ld_1dayago)s" then b.fuid end ) f1drupt_paycnt,
                count( case when b.dt="%(ld_7dayago)s" then b.fuid end ) f7drupt_paycnt,
                count( case when b.dt="%(ld_30dayago)s" then b.fuid end) f30drupt_paycnt,
                0 frupt_paidcnt
            from (
                select fbpid, fuid, count(1) bankrupt_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join (
                select distinct d.fbpid, d.dt, c.fuid
                from stage.user_pay_info c
                join stage.payment_stream_stg d
                    on c.fbpid = d.fbpid and c.fplatform_uid = d.fplatform_uid
                    and cast(d.dt as string) in
                    ("%(statdate)s", "%(ld_1dayago)s", "%(ld_7dayago)s", "%(ld_30dayago)s")
            ) b
                on a.fbpid=b.fbpid and a.fuid=b.fuid
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, a.bankrupt_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 历史付费用户破产次数
        insert into table stage.agg_user_bankrupt_num_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk, bpm.fversionfsk fversionfsk,
                bpm.fterminalfsk fterminalfsk, a.bankrupt_num bank_num,
                0 fbankruptusercnt,
                0 frupt_actcnt, 0 f1drupt_actcnt, 0 f7drupt_actcnt, 0 f30drupt_actcnt,
                0 frupt_dsucnt, 0 f1drupt_dsucnt, 0 f7drupt_dsucnt, 0 f30drupt_dsucnt,
                0 frupt_paycnt, 0 f1drupt_paycnt, 0 f7drupt_paycnt, 0 f30drupt_paycnt,
                count( a.fuid ) frupt_paidcnt
            from (
                select fbpid, fuid, count(1) bankrupt_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join (
                select fbpid, fuid
                from stage.pay_user_mid
                where ffirst_pay_at<"%(statdate)s"
            ) b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk, a.bankrupt_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_bankrupt_num_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, bank_num,
                max(fbankruptusercnt) fbankruptusercnt,
                max(frupt_actcnt) frupt_actcnt,
                max(f1drupt_actcnt) f1drupt_actcnt,
                max(f7drupt_actcnt) f7drupt_actcnt,
                max(f30drupt_actcnt) f30drupt_actcnt,
                max(frupt_dsucnt) frupt_dsucnt,
                max(f1drupt_dsucnt) f1drupt_dsucnt,
                max(f7drupt_dsucnt) f7drupt_dsucnt,
                max(f30drupt_dsucnt) f30drupt_dsucnt,
                max(frupt_paycnt) frupt_paycnt,
                max(f1drupt_paycnt) f1drupt_paycnt,
                max(f7drupt_paycnt) f7drupt_paycnt,
                max(f30drupt_paycnt) f30drupt_paycnt,
                max(frupt_paidcnt) frupt_paidcnt
            from stage.agg_user_bankrupt_num_data_%(num_begin)s
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, bank_num
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_user_bankrupt_num_data_%(num_begin)s;
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
a = agg_user_bankrupt_num_data(statDate, eid)
a()
