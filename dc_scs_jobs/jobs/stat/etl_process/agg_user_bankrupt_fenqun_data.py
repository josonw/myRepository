#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_bankrupt_fenqun_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_bankrupt_fenqun_fct
        (
          fdate            date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          frupt_actuser    bigint,
          f1drupt_actuser  bigint,
          f7drupt_actuser  bigint,
          f30drupt_actuser bigint,
          frupt_payuser    bigint,
          f1drupt_payuser  bigint,
          f7drupt_payuser  bigint,
          f30drupt_payuser bigint,
          frupt_dsu        bigint,
          f1drupt_dsu      bigint,
          f7drupt_dsu      bigint,
          f30drupt_dsu     bigint,
          frupt_dsunum     bigint,
          f1drupt_dsunum   bigint,
          f7drupt_dsunum   bigint,
          f30drupt_dsunum  bigint,
          frupt_actnum     bigint,
          f1drupt_actnum   bigint,
          f7drupt_actnum   bigint,
          f30drupt_actnum  bigint,
          frupt_paynum     bigint,
          f1drupt_paynum   bigint,
          f7drupt_paynum   bigint,
          f30drupt_paynum  bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_bankrupt_fenqun_fct'
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

        hql = """    -- 破产-活跃
        drop table if exists stage.agg_user_bankrupt_fenqun_data_%(num_begin)s;

        create table stage.agg_user_bankrupt_fenqun_data_%(num_begin)s as
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                count( case when b.dt="%(statdate)s" then b.fuid end) frupt_actuser,
                count( case when b.dt="%(ld_1dayago)s" then b.fuid end) f1drupt_actuser,
                count( case when b.dt="%(ld_7dayago)s" then b.fuid end) f7drupt_actuser,
                count( case when b.dt="%(ld_30dayago)s" then b.fuid end) f30drupt_actuser,
                0 frupt_payuser, 0 f1drupt_payuser, 0 f7drupt_payuser, 0 f30drupt_payuser,
                0 frupt_dsu, 0 f1drupt_dsu, 0 f7drupt_dsu, 0 f30drupt_dsu,
                0 frupt_dsunum, 0 f1drupt_dsunum, 0 f7drupt_dsunum, 0 f30drupt_dsunum,
                sum( case when b.dt="%(statdate)s" then bank_num end ) frupt_actnum,
                sum( case when b.dt="%(ld_1dayago)s" then bank_num end ) f1drupt_actnum,
                sum( case when b.dt="%(ld_7dayago)s" then bank_num end ) f7drupt_actnum,
                sum( case when b.dt="%(ld_30dayago)s" then bank_num end ) f30drupt_actnum,
                0 frupt_paynum, 0 f1drupt_paynum, 0 f7drupt_paynum, 0 f30drupt_paynum
            from (
                select fbpid, fuid , count(1) bank_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join stage.active_user_mid b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
                and cast(b.dt as string) in ("%(statdate)s", "%(ld_1dayago)s", "%(ld_7dayago)s", "%(ld_30dayago)s")
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 破产-付费
        insert into table stage.agg_user_bankrupt_fenqun_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                0 frupt_actuser, 0 f1drupt_actuser, 0 f7drupt_actuser, 0 f30drupt_actuser,
                count( case when b.dt="%(statdate)s" then b.fuid end ) frupt_payuser,
                count( case when b.dt="%(ld_1dayago)s" then b.fuid end ) f1drupt_payuser,
                count( case when b.dt="%(ld_7dayago)s" then b.fuid end ) f7drupt_payuser,
                count( case when b.dt="%(ld_30dayago)s" then b.fuid end ) f30drupt_payuser,
                0 frupt_dsu, 0 f1drupt_dsu, 0 f7drupt_dsu, 0 f30drupt_dsu,
                0 frupt_dsunum, 0 f1drupt_dsunum, 0 f7drupt_dsunum, 0 f30drupt_dsunum,
                0 frupt_actnum, 0 f1drupt_actnum, 0 f7drupt_actnum, 0 f30drupt_actnum,
                sum( case when b.dt="%(statdate)s" then bank_num end ) frupt_paynum,
                sum( case when b.dt="%(ld_1dayago)s" then bank_num end ) f1drupt_paynum,
                sum( case when b.dt="%(ld_7dayago)s" then bank_num end ) f7drupt_paynum,
                sum( case when b.dt="%(ld_30dayago)s" then bank_num end ) f30drupt_paynum
            from (
                select fbpid, fuid , count(1) bank_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join (
                select distinct b.fbpid, b.dt, a.fuid
                from stage.user_pay_info a
                join stage.payment_stream_stg b
                    on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
                    and cast(b.dt as string) in ("%(statdate)s", "%(ld_1dayago)s", "%(ld_7dayago)s", "%(ld_30dayago)s")
            ) b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    --破产-新增
        insert into table stage.agg_user_bankrupt_fenqun_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                0 frupt_actuser, 0 f1drupt_actuser, 0 f7drupt_actuser, 0 f30drupt_actuser,
                0 frupt_payuser, 0 f1drupt_payuser, 0 f7drupt_payuser, 0 f30drupt_payuser,
                count(distinct case when b.dt="%(statdate)s" then b.fuid end ) frupt_dsu,
                count(distinct case when b.dt="%(ld_1dayago)s" then b.fuid end ) f1drupt_dsu,
                count(distinct case when b.dt="%(ld_7dayago)s" then b.fuid end ) f7drupt_dsu,
                count(distinct case when b.dt="%(ld_30dayago)s" then b.fuid end ) f30drupt_dsu,
                sum( case when b.dt="%(statdate)s" then bank_num end ) frupt_dsunum,
                sum( case when b.dt="%(ld_1dayago)s" then bank_num end ) f1drupt_dsunum,
                sum( case when b.dt="%(ld_7dayago)s" then bank_num end ) f7drupt_dsunum,
                sum( case when b.dt="%(ld_30dayago)s" then bank_num end ) f30drupt_dsunum,
                0 frupt_actnum, 0 f1drupt_actnum, 0 f7drupt_actnum, 0 f30drupt_actnum,
                0 frupt_paynum, 0 f1drupt_paynum, 0 f7drupt_paynum, 0 f30drupt_paynum
            from (
                select fbpid, fuid , count(1) bank_num
                from stage.user_bankrupt_stg
                where dt="%(statdate)s"
                group by fbpid, fuid
            ) a
            join stage.user_dim b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
                    and cast(b.dt as string) in ("%(statdate)s", "%(ld_1dayago)s", "%(ld_7dayago)s", "%(ld_30dayago)s")
            join analysis.bpid_platform_game_ver_map bpm
                on a.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_bankrupt_fenqun_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                sum(frupt_actuser) frupt_actuser,
                sum(f1drupt_actuser) f1drupt_actuser,
                sum(f7drupt_actuser) f7drupt_actuser,
                sum(f30drupt_actuser) f30drupt_actuser,
                sum(frupt_payuser) frupt_payuser,
                sum(f1drupt_payuser) f1drupt_payuser,
                sum(f7drupt_payuser) f7drupt_payuser,
                sum(f30drupt_payuser) f30drupt_payuser,
                sum(frupt_dsu) frupt_dsu,
                sum(f1drupt_dsu) f1drupt_dsu,
                sum(f7drupt_dsu) f7drupt_dsu,
                sum(f30drupt_dsu) f30drupt_dsu,
                sum(frupt_dsunum) frupt_dsunum,
                sum(f1drupt_dsunum) f1drupt_dsunum,
                sum(f7drupt_dsunum) f7drupt_dsunum,
                sum(f30drupt_dsunum) f30drupt_dsunum,
                sum(frupt_actnum) frupt_actnum,
                sum(f1drupt_actnum) f1drupt_actnum,
                sum(f7drupt_actnum) f7drupt_actnum,
                sum(f30drupt_actnum) f30drupt_actnum,
                sum(frupt_paynum) frupt_paynum,
                sum(f1drupt_paynum) f1drupt_paynum,
                sum(f7drupt_paynum) f7drupt_paynum,
                sum(f30drupt_paynum) f30drupt_paynum
            from stage.agg_user_bankrupt_fenqun_data_%(num_begin)s
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_user_bankrupt_fenqun_data_%(num_begin)s;
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
a = agg_user_bankrupt_fenqun_data(statDate, eid)
a()
