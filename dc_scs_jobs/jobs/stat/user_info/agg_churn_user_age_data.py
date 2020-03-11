#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_user_age_data(BaseStat):
    """流失用户，游戏年龄
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_lc_loss_age_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fx1 bigint,
                fx2 bigint,
                fx3 bigint,
                fx4 bigint,
                fx5 bigint,
                fx6 bigint,
                fx7 bigint,
                fx8 bigint,
                fx9 bigint,
                fflag bigint
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
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        insert overwrite table analysis.user_lc_loss_age_fct
        partition (dt = '%(ld_daybegin)s')
        select /*+ MAPJOIN(c) */ '%(ld_daybegin)s' fdate,
                   e.fgamefsk,
                   e.fplatformfsk,
                   e.fversionfsk,
                   e.fterminalfsk,
                   case when fflag = 30 then 3
                      when fflag = 14 then 2
                      when fflag = 7 then 1
                    else 0 end fflag,
                    count(distinct case when signup <= 1 then a.fuid end) x1,
                    count(distinct case when signup > 1 and signup <= 3 then a.fuid end) x2,
                    count(distinct case when signup > 3 and signup <= 7 then a.fuid end) x3,
                    count(distinct case when signup > 7 and signup <= 14 then a.fuid end) x4,
                    count(distinct case when signup > 14 and signup <= 30 then a.fuid end) x5,
                    count(distinct case when signup > 30 and signup <= 90 then a.fuid end) x6,
                    count(distinct case when signup > 90 and signup <= 180 then a.fuid end) x7,
                    count(distinct case when signup > 180 and signup <= 365 then a.fuid end) x8,
                    count(distinct case when signup > 365 and signup <= 3650 then a.fuid end) x9
              from (select c.fbpid, c.fuid, c.fflag, max(datediff(d.fdate, c.fsignup_at)) signup
                     from ( select a.fbpid, a.fuid, a.days fflag, b.fsignup_at
                              from stage.churn_user_mid a
                              join stage.user_dim b
                                on a.fbpid = b.fbpid
                               and a.fuid = b.fuid
                              where a.churn_type='cycle'
                               ) c
                              join stage.active_user_mid d
                                on c.fbpid=d.fbpid
                               and c.fuid=d.fuid
                               and d.fdate >= '%(ld_59dayago)s'
                               and d.fdate < '%(ld_dayend)s'
                          group by c.fbpid, c.fuid, c.fflag
                   ) a
              join analysis.bpid_platform_game_ver_map e
              on a.fbpid = e.fbpid
              group by e.fplatformfsk,
                        e.fgamefsk,
                        e.fversionfsk,
                        e.fterminalfsk,
                        case when fflag = 30 then 3
                              when fflag = 14 then 2
                              when fflag = 7 then 1
                            else 0 end ;
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
a = agg_churn_user_age_data(statDate)
a()
