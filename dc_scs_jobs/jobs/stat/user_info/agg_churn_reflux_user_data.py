#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_reflux_user_data(BaseStat):
    """流失回流用户中间表，只存放一天
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_backflow_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                f2duserback bigint,
                f5duserback bigint,
                f7duserback bigint,
                f14duserback bigint,
                f30duserback bigint,
                f2dpayuserback bigint,
                f5dpayuserback bigint,
                f7dpayuserback bigint,
                f14dpayuserback bigint,
                f30dpayuserback bigint
                )
                partitioned by (dt date)
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
        insert overwrite table analysis.user_backflow_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' date,
                 c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
                 count( if(reflux = 2, a.fuid, null)) f2duserback,
                 count( if(reflux = 5, a.fuid, null)) f5duserback,
                 count( if(reflux = 7, a.fuid, null)) f7duserback,
                 count( if(reflux = 14, a.fuid, null)) f14duserback,
                 count( if(reflux = 30, a.fuid, null)) f30duserback,
                 count( if(reflux = 2, b.fuid, null)) f2dpayuserback,
                 count( if(reflux = 5, b.fuid, null)) f5dpayuserback,
                 count( if(reflux = 7, b.fuid, null)) f7dpayuserback,
                 count( if(reflux = 14, b.fuid, null)) f14dpayuserback,
                 count( if(reflux = 30, b.fuid, null)) f30dpayuserback
            from stage.reflux_user_mid a
            left outer join stage.pay_user_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
            where a.dt='%(ld_daybegin)s' and a.reflux_type='day'
           group by c.fplatformfsk, c.fgamefsk, c.fversionfsk, c.fterminalfsk;
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
a = agg_churn_reflux_user_data(statDate)
a()
