#-*- coding: UTF-8 -*-
# Author: AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_label_all(BaseStat):
    def create_tab(self):
        """
        建表，汇总各类标签
        """
        hql = """
          create table if not exists stage.user_label_all
          (
            fdate               date,
            fbpid               varchar(100),
            fuid                int,
            fis_churn_3day      tinyint,
            fis_churn_5day      tinyint,
            fis_churn_7day      tinyint,
            fis_churn_30day     tinyint,
            factivity_score     decimal(30, 4),
            factivity_rank      int,
            factivity_rate      decimal(30,10),
            fgameparty_score    decimal(30, 4),
            fgameparty_rank     int,
            fgameparty_rate     decimal(30,10),
            floyalty_score      decimal(30, 4),
            floyalty_rank       int,
            floyalty_rate       decimal(30,10),
            fwin_score          decimal(30, 4),
            fwin_rank           int,
            fwin_rate           decimal(30,10),
            fpayment_score      decimal(30, 4),
            fpayment_rank       int,
            fpayment_rate       decimal(30,10)
          )
          partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
             insert overwrite table stage.user_label_all
             partition(dt='%(ld_daybegin)s')
             select '%(ld_daybegin)s' fdate,
                    t1.fbpid,
                    t1.fuid,
                    case when datediff(flast_act_date, '%(ld_daybegin)s') >= 3 then 1 else 0 end fis_churn_3day,
                    case when datediff(flast_act_date, '%(ld_daybegin)s') >= 5 then 1 else 0 end fis_churn_5day,
                    case when datediff(flast_act_date, '%(ld_daybegin)s') >= 7 then 1 else 0 end fis_churn_7day,
                    case when datediff(flast_act_date, '%(ld_daybegin)s') >= 30 then 1 else 0 end fis_churn_30day,
                    t2.fscore factivity_score,
                    t2.frank  factivity_rank,
                    t2.frate  factivity_rate,
                    t3.fscore fgameparty_score,
                    t3.frank  fgameparty_rank,
                    t3.frate  fgameparty_rate,
                    t4.fscore floyalty_score,
                    t4.frank  floyalty_rank,
                    t4.frate  floyalty_rate,
                    t5.fscore fwin_score,
                    t5.frank  fwin_rank,
                    t5.frate  fwin_rate,
                    t6.fscore fpayment_score,
                    t6.frank  fpayment_rank,
                    t6.frate  fpayment_rate
               from (select fbpid,
                            fuid,
                            flast_act_date
                       from stage.user_info_all
                      where dt = '%(ld_daybegin)s'
                        and flast_act_date > '%(ld_60dayago)s'
                    ) t1
               left outer join stage.user_label_activity t2
                 on t1.fbpid = t2.fbpid
                and t1.fuid = t2.fuid
                and t2.dt = '%(ld_daybegin)s'
               left outer join stage.user_label_gameparty t3
                 on t1.fbpid = t3.fbpid
                and t1.fuid = t3.fuid
                and t3.dt = '%(ld_daybegin)s'
               left outer join stage.user_label_loyalty t4
                 on t1.fbpid = t4.fbpid
                and t1.fuid = t4.fuid
                and t4.dt = '%(ld_daybegin)s'
               left outer join stage.user_label_win t5
                 on t1.fbpid = t5.fbpid
                and t1.fuid = t5.fuid
                and t5.dt = '%(ld_daybegin)s'
               left outer join stage.user_label_payment t6
                 on t1.fbpid = t6.fbpid
                and t1.fuid = t6.fuid
                and t6.dt = '%(ld_daybegin)s'
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_label_all(statDate)
    a()
