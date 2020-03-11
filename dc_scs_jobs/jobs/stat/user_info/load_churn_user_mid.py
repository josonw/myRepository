#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_churn_user_mid(BaseStat):
    """流失用户中间表，只存放一天
    """
    def create_tab(self):
        hql = """create external table if not exists stage.churn_user_mid
                (
                fbpid varchar(50),
                fuid bigint
                )
                partitioned by ( churn_type string, days int )
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
        drop table if exists stage.churn_uid_tmp;
        create table if not exists stage.churn_uid_tmp
        as
        select fbpid,
               fuid,
               max(dayago30_q) dayago30_q,
               max(dayago14_q) dayago14_q,
               max(dayago7_q) dayago7_q,
               max(dayago3_q) dayago3_q,
               max(dayago30_h) dayago30_h,
               max(dayago14_h) dayago14_h,
               max(dayago7_h) dayago7_h,
               max(dayago5_h) dayago5_h,
               max(dayago3_h) dayago3_h,
               max(dayago2_h) dayago2_h,
               max(dayago1) dayago1,
               max(daybegin) daybegin,
               max(dayago2) dayago2,
               max(dayago5) dayago5,
               max(dayago7) dayago7,
               max(dayago14) dayago14,
               max(dayago30) dayago30
        from (
          select fbpid, fuid,
                if(a.dt >= '%(ld_59dayago)s' and a.dt < '%(ld_29dayago)s' , 1, 0 ) dayago30_q,
                if(a.dt >= '%(ld_27dayago)s' and a.dt < '%(ld_13dayago)s' , 1, 0 ) dayago14_q,
                if(a.dt >= '%(ld_13dayago)s' and a.dt < '%(ld_6dayago)s' , 1, 0 ) dayago7_q,
                if(a.dt >= '%(ld_5dayago)s' and a.dt < '%(ld_2dayago)s' , 1, 0 ) dayago3_q,
                if(a.dt >= '%(ld_29dayago)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) dayago30_h,
                if(a.dt >= '%(ld_13dayago)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) dayago14_h,
                if(a.dt >= '%(ld_6dayago)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) dayago7_h,
                if(a.dt >= '%(ld_4dayago)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) dayago5_h,
                if(a.dt >= '%(ld_2dayago)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) dayago3_h,
                if(a.dt >= '%(ld_1dayago)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) dayago2_h,
                if(a.dt >= '%(ld_1dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 ) dayago1,
                if(a.dt >= '%(ld_daybegin)s' and a.dt < '%(ld_dayend)s' , 1, 0 ) daybegin,
                if(a.dt >= '%(ld_2dayago)s' and a.dt < '%(ld_1dayago)s' , 1, 0 ) dayago2,
                if(a.dt >= '%(ld_5dayago)s' and a.dt < '%(ld_4dayago)s' , 1, 0 ) dayago5,
                if(a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_6dayago)s' , 1, 0 ) dayago7,
                if(a.dt >= '%(ld_14dayago)s' and a.dt < '%(ld_13dayago)s' , 1, 0 ) dayago14,
                if(a.dt >= '%(ld_30dayago)s' and a.dt < '%(ld_29dayago)s' , 1, 0 ) dayago30
           from stage.active_user_mid a
          where dt >= '%(ld_59dayago)s' and dt < '%(ld_dayend)s'
        ) tmp group by fbpid, fuid;

        -- cycle 周期流失 , day 单日流失
        from stage.churn_uid_tmp
        insert overwrite table stage.churn_user_mid partition(churn_type='cycle', days=30)
        select fbpid, fuid where dayago30_q=1 and dayago30_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='cycle', days=14)
        select fbpid, fuid where dayago14_q=1 and dayago14_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='cycle', days=7)
        select fbpid, fuid where dayago7_q=1 and dayago7_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='cycle', days=3)
        select fbpid, fuid where dayago3_q=1 and dayago3_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='cycle', days=1)
        select fbpid, fuid where dayago1=1 and daybegin=0
        --现在进行单日流失插入
        insert overwrite table stage.churn_user_mid partition(churn_type='day', days=30)
        select fbpid, fuid where dayago30=1 and dayago30_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='day', days=14)
        select fbpid, fuid where dayago14=1 and dayago14_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='day', days=7)
        select fbpid, fuid where dayago7=1 and dayago7_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='day', days=5)
        select fbpid, fuid where dayago5=1 and dayago5_h=0
        insert overwrite table stage.churn_user_mid partition(churn_type='day', days=2)
        select fbpid, fuid where dayago2=1 and dayago2_h=0
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
a = load_churn_user_mid(statDate)
a()
