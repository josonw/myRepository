#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_avg_actdays_data(BaseStat):
    """活跃用户，平均活跃天数。
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_active_avg_days_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                f7dau_days bigint,
                f30dau_days bigint,
                f7dsu_days bigint,
                f30dsu_days bigint,
                f7dpu_days bigint,
                f30dpu_days bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        hql = """
        insert overwrite table analysis.user_active_avg_days_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 round(avg(if(a.f7dau_days > 0, a.f7dau_days, null)), 2) f7dau_days,
                 round(avg(if(a.f30dau_days > 0,a.f30dau_days,null)), 2) f30dau_days,
                 round(avg(if(a.f7dsu_days > 0 , a.f7dsu_days,null)), 2) f7dsu_days,
                 round(avg(if(a.f30dsu_days > 0, a.f30dsu_days, null)), 2) f30dsu_days,
                 round(avg(if(a.f7dpu_days > 0, a.f7dpu_days, null)), 2) f7dpu_days,
                 round(avg(if(a.f30dpu_days > 0, a.f30dpu_days, null)), 2) f30dpu_days
            from (select a.fbpid,
                         a.fuid,
                         nvl(count(a.fuid), 0) f30dau_days,
                         nvl(count(if(fdate >= '%(ld_6dayago)s' and fdate <= '%(ld_daybegin)s', a.fuid, null)), 0) f7dau_days,
                         nvl(count(b.fuid), 0) f30dsu_days,
                         nvl(count(if(fdate >= '%(ld_6dayago)s' and fdate <= '%(ld_daybegin)s', b.fuid, null)), 0) f7dsu_days,
                         nvl(count(pay.fuid), 0) f30dpu_days,
                         nvl(count(if(fdate >= '%(ld_6dayago)s' and fdate <= '%(ld_daybegin)s', pay.fuid, null)),0) f7dpu_days
                    from (select fbpid, fuid, fdate
                            from stage.active_user_mid
                           where dt >= '%(ld_29dayago)s'
                             and dt <= '%(ld_daybegin)s') a
                    left outer join (select fbpid, fuid
                                      from stage.user_dim
                                     where dt >= '%(ld_29dayago)s'
                                       and dt < '%(ld_dayend)s') b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                    left outer join (select distinct a.fbpid, b.fuid
                                      from stage.payment_stream_stg a
                                      join stage.pay_user_mid b
                                        on a.fbpid = b.fbpid
                                       and a.fplatform_uid = b.fplatform_uid
                                     where a.dt >= '%(ld_29dayago)s'
                                       and a.dt < '%(ld_dayend)s') pay
                      on a.fbpid = pay.fbpid
                     and a.fuid = pay.fuid
                   group by a.fbpid, a.fuid) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk;
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
a = agg_user_avg_actdays_data(statDate)
a()
