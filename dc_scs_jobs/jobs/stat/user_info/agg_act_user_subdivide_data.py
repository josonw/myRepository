#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_user_subdivide_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_active_subdivide_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                factcnt bigint,
                fretcnt bigint,
                f7dregcnt bigint,
                fdaypaycnt bigint,
                f1dfretcnt bigint,
                f1dnewcnt bigint,
                f1dcnt bigint
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
              set io.sort.mb=256;

        insert overwrite table analysis.user_active_subdivide_fct partition
          (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 count(distinct fuid) factcnt,
                 count(distinct reflux_uid) fretcnt,
                 count(distinct 7d_reg_uid) f7dregcnt,
                 count(distinct hpay_uid) fdaypaycnt,
                 count(distinct 1d_reflux_uid) f1dfretcnt,
                 0 f1dnewcnt,
                 0 f1dcnt
            from (select a.fbpid, a.fuid, b.fuid 1d_reflux_uid,  e.fuid reflux_uid,
                        c.fuid 7d_reg_uid, d.fuid hpay_uid
                  from stage.active_user_mid a
                  left outer join stage.reflux_user_mid b
                         on a.fbpid=b.fbpid
                        and a.fuid=b.fuid
                        and b.dt = '%(ld_1dayago)s' and b.reflux=7
                  left outer join stage.reflux_user_mid e
                       on a.fbpid=e.fbpid
                      and a.fuid=e.fuid
                      and e.dt = '%(ld_daybegin)s' and e.reflux = 7
                  left outer join stage.user_dim c
                       on a.fbpid = c.fbpid
                      and a.fuid = c.fuid
                      and c.dt >= '%(ld_7dayago)s'and c.dt < '%(ld_dayend)s'
                  left outer join active_paid_user_mid d
                         on a.fbpid=d.fbpid
                        and a.fuid=d.fuid
                        and d.dt = '%(ld_daybegin)s'
                      where a.dt = '%(ld_daybegin)s'
                  ) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;

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
a = agg_act_user_subdivide_data(statDate)
a()
