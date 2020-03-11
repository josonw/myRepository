#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pay_churn_funnel_paid_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.paid_user_loss_funnel_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                f1dpusernum bigint,
                f2dpusernum bigint,
                f3dpusernum bigint,
                f4dpusernum bigint,
                f5dpusernum bigint,
                f6dpusernum bigint,
                f7dpusernum bigint,
                f14dpusernum bigint,
                f30dpusernum bigint,
                f1dausernum bigint,
                f2dausernum bigint,
                f3dausernum bigint,
                f4dausernum bigint,
                f5dausernum bigint,
                f6dausernum bigint,
                f7dausernum bigint,
                f14dausernum bigint,
                f30dausernum bigint
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

        res = self.hq.exe_sql("""use stage;
            """)
        if res != 0: return res

        hql = """
        drop table if exists stage.active_paid_user_tmp;
        create table stage.active_paid_user_tmp
        as
        select * from stage.active_paid_user_mid
        where dt >= '%(ld_89dayago)s'
        and  dt < '%(ld_dayend)s';

        insert overwrite table analysis.paid_user_loss_funnel_fct
        partition ( dt )
        select fdate fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               count(distinct if(fpay_at >= date_add(fdate, 1), fuid, null)) F1DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 2), fuid, null)) F2DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 3), fuid, null)) F3DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 4), fuid, null)) F4DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 5), fuid, null)) F5DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 6), fuid, null)) F6DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 7), fuid, null)) F7DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 14), fuid, null)) F14DPUSERNUM,
               count(distinct if(fpay_at >= date_add(fdate, 30), fuid, null)) F30DPUSERNUM,
               0 f1dausernum,
               0 f2dausernum,
               0 f3dausernum,
               0 f4dausernum,
               0 f5dausernum,
               0 f6dausernum,
               0 f7dausernum,
               0 f14dausernum,
               0 f30dausernum,
               fdate dt
          from (select a.fdate, a.fbpid,  a.fuid, b.fpay_at
                  from stage.active_paid_user_tmp a
                  join (
                      select fbpid, fuid, max(fpay_at) fpay_at
                       from stage.user_pay_info
                      where dt >= '%(ld_89dayago)s'
                      and dt < '%(ld_dayend)s'
                    group by fbpid, fuid
                     ) b
                    on a.fbpid = b.fbpid
                   and a.fuid = b.fuid
                  ) a
          join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdate;

        drop table if exists stage.pay_churn_funnel_paid_tmp;
        create table stage.pay_churn_funnel_paid_tmp
        as
        select  fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 0 f1dpusernum,
                 0 f2dpusernum,
                 0 f3dpusernum,
                 0 f4dpusernum,
                 0 f5dpusernum,
                 0 f6dpusernum,
                 0 f7dpusernum,
                 0 f14dpusernum,
                 0 f30dpusernum,
                 count(distinct if(actday >= date_add(fdate, 1), fuid, null)) f1dausernum,
                 count(distinct if(actday >= date_add(fdate, 2), fuid, null)) f2dausernum,
                 count(distinct if(actday >= date_add(fdate, 3), fuid, null)) f3dausernum,
                 count(distinct if(actday >= date_add(fdate, 4), fuid, null)) f4dausernum,
                 count(distinct if(actday >= date_add(fdate, 5), fuid, null)) f5dausernum,
                 count(distinct if(actday >= date_add(fdate, 6), fuid, null)) f6dausernum,
                 count(distinct if(actday >= date_add(fdate, 7), fuid, null)) f7dausernum,
                 count(distinct if(actday >= date_add(fdate, 14), fuid, null)) f14dausernum,
                 count(distinct if(actday >= date_add(fdate, 30), fuid, null)) f30dausernum,
                 fdate dt
            from (select a.fbpid, a.fdate, actday, a.fuid
                    from stage.active_paid_user_tmp a
                    join (select fbpid, fuid, max(fdate) actday
                            from stage.active_user_mid
                            where dt >= '%(ld_89dayago)s'
                            and dt < '%(ld_dayend)s'
                            group by fbpid, fuid
                        ) b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
           distribute by a.fbpid, a.fdate) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdate;

        insert overwrite table analysis.paid_user_loss_funnel_fct
        partition (dt)
          select fdate,
                 fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                 max(f1dpusernum) f1dpusernum,
                 max(f2dpusernum) f2dpusernum,
                 max(f3dpusernum) f3dpusernum,
                 max(f4dpusernum) f4dpusernum,
                 max(f5dpusernum) f5dpusernum,
                 max(f6dpusernum) f6dpusernum,
                 max(f7dpusernum) f7dpusernum,
                 max(f14dpusernum) f14dpusernum,
                 max(f30dpusernum) f30dpusernum,
                 max(f1dausernum) f1dausernum,
                 max(f2dausernum) f2dausernum,
                 max(f3dausernum) f3dausernum,
                 max(f4dausernum) f4dausernum,
                 max(f5dausernum) f5dausernum,
                 max(f6dausernum) f6dausernum,
                 max(f7dausernum) f7dausernum,
                 max(f14dausernum) f14dausernum,
                 max(f30dausernum) f30dausernum,
                 fdate dt
            from (select fdate,
                         fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         f1dpusernum,
                         f2dpusernum,
                         f3dpusernum,
                         f4dpusernum,
                         f5dpusernum,
                         f6dpusernum,
                         f7dpusernum,
                         f14dpusernum,
                         f30dpusernum,
                         f1dausernum,
                         f2dausernum,
                         f3dausernum,
                         f4dausernum,
                         f5dausernum,
                         f6dausernum,
                         f7dausernum,
                         f14dausernum,
                         f30dausernum,
                         fdate        dt
                    from analysis.paid_user_loss_funnel_fct
                   where dt >= '%(ld_89dayago)s'
                     and dt < '%(ld_dayend)s'
                  union all
                  select * from pay_churn_funnel_paid_tmp
                  ) tmp
           group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.paid_user_loss_funnel_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        f1dpusernum,
        f2dpusernum,
        f3dpusernum,
        f4dpusernum,
        f5dpusernum,
        f6dpusernum,
        f7dpusernum,
        f14dpusernum,
        f30dpusernum,
        f1dausernum,
        f2dausernum,
        f3dausernum,
        f4dausernum,
        f5dausernum,
        f6dausernum,
        f7dausernum,
        f14dausernum,
        f30dausernum
        from analysis.paid_user_loss_funnel_fct
         where dt >= '%(ld_89dayago)s'
          and dt < '%(ld_dayend)s'
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
a = agg_pay_churn_funnel_paid_data(statDate)
a()


