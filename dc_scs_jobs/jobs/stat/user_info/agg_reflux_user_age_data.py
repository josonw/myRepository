#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reflux_user_age_data(BaseStat):
    """回流用户, 游戏年龄数据
    fx 流失回流
    fr 回流用户
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_backflow_age_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fnotact_day bigint,
                fx1 bigint,
                fx2 bigint,
                fx3 bigint,
                fx4 bigint,
                fx5 bigint,
                fx6 bigint,
                fx7 bigint,
                fx8 bigint,
                fx9 bigint,
                fr1 bigint,
                fr2 bigint,
                fr3 bigint,
                fr4 bigint,
                fr5 bigint,
                fr6 bigint,
                fr7 bigint,
                fr8 bigint,
                fr9 bigint
                )
                partitioned by ( dt date )
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update(date)
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict;""" % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        set hive.auto.convert.join=false;
        insert overwrite table analysis.user_backflow_age_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk, c.fnotact_day,
            count(distinct case when c.reflux_type='day' and signup <= 1 then c.fuid end) fx1,
            count(distinct case when c.reflux_type='day' and signup > 1 and signup <= 3 then c.fuid end) fx2,
            count(distinct case when c.reflux_type='day' and signup > 3 and signup <= 7 then c.fuid end) fx3,
            count(distinct case when c.reflux_type='day' and signup > 7 and signup <= 14 then c.fuid end) fx4,
            count(distinct case when c.reflux_type='day' and signup > 14 and signup <= 30 then c.fuid end) fx5,
            count(distinct case when c.reflux_type='day' and signup > 30 and signup <= 90 then c.fuid end) fx6,
            count(distinct case when c.reflux_type='day' and signup > 90 and signup <= 180 then c.fuid end) fx7,
            count(distinct case when c.reflux_type='day' and signup > 180 and signup <= 365 then c.fuid end) fx8,
            count(distinct case when c.reflux_type='day' and signup > 365 and signup <= 3650 then c.fuid end) fx9,
            count(distinct case when c.reflux_type='cycle' and signup <= 1 then c.fuid end) fr1,
            count(distinct case when c.reflux_type='cycle' and signup > 1 and signup <= 3 then c.fuid end) fr2,
            count(distinct case when c.reflux_type='cycle' and signup > 3 and signup <= 7 then c.fuid end) fr3,
            count(distinct case when c.reflux_type='cycle' and signup > 7 and signup <= 14 then c.fuid end) fr4,
            count(distinct case when c.reflux_type='cycle' and signup > 14 and signup <= 30 then c.fuid end) fr5,
            count(distinct case when c.reflux_type='cycle' and signup > 30 and signup <= 90 then c.fuid end) fr6,
            count(distinct case when c.reflux_type='cycle' and signup > 90 and signup <= 180 then c.fuid end) fr7,
            count(distinct case when c.reflux_type='cycle' and signup > 180 and signup <= 365 then c.fuid end) fr8,
            count(distinct case when c.reflux_type='cycle' and signup > 365 and signup <= 3650 then c.fuid end) fr9
        from (select a.fbpid, a.fuid, a.reflux fnotact_day, a.reflux_type, datediff('%(ld_daybegin)s', to_date(nvl(b.fsignup_at,'1970-01-01'))) signup
                from stage.reflux_user_mid a
                left outer join stage.user_dim b
                on a.fbpid = b.fbpid
                and a.fuid = b.fuid
                where a.dt='%(ld_daybegin)s'
        ) c
        join analysis.bpid_platform_game_ver_map d
        on c.fbpid = d.fbpid
        group by d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk, c.fnotact_day;

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_reflux_user_age_data(statDate)
a()
