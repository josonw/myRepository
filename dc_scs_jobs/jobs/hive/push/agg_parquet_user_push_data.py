# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class agg_parquet_user_push_data(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dim.parquet_user_push_gameparty (
            fdate              string,
            fbpid              varchar(50),
            fuid               bigint,
            fparty_num         bigint   comment '上一次玩牌局数',
            fparty_num_total   bigint   comment '累计玩牌局数',
            fparty_num_day     bigint   comment '累计玩牌天数',
            fy_party_num       bigint   comment '上一次约牌房玩牌局数',
            fy_party_num_total bigint   comment '累计约牌房玩牌局数',
            fy_party_num_day   bigint   comment '累计约牌房玩牌天数'
            )comment '推送数据前置牌局数据'
            partitioned by(dt string);

        create table if not exists dim.parquet_user_push_pay (
            fdate              string,
            fbpid              varchar(50),
            fuid               bigint,
            fpay_num           float    comment '上一次付费额度',
            fpay_num_total     float    comment '累计付费额度',
            fpay_num_day       bigint   comment '累计付费天数'
            )comment '推送数据前置付费数据'
            partitioned by(dt string);

        create table if not exists dim.parquet_user_push_rupt (
            fdate              string,
            fbpid              varchar(50),
            fuid               bigint,
            frupt_num          bigint   comment '上一次破产次数',
            frupt_num_total    bigint   comment '累计破产次数',
            frupt_num_day      bigint   comment '累计破产天数'
            )comment '推送数据前置破产数据'
            partitioned by(dt string);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
    insert overwrite table dim.parquet_user_push_gameparty partition(dt = '%(statdate)s')
         select t1.fdate
                ,t1.fbpid
                ,t1.fuid
                ,coalesce(t1.fparty_num, t2.fparty_num, 0) fparty_num
                ,coalesce(t1.fparty_num_total+t2.fparty_num_total, 0) fparty_num_total
                ,coalesce(t1.fparty_num_day+t2.fparty_num_day, 0) fparty_num_day
                ,coalesce(t1.fy_party_num, t2.fy_party_num, 0) fy_party_num
                ,coalesce(t1.fy_party_num_total+t2.fy_party_num_total, 0) fy_party_num_total
                ,coalesce(t1.fy_party_num_day+t2.fy_party_num_day, 0) fy_party_num_day
           from (select fdate
                        ,fbpid
                        ,fuid
                        ,fparty_num
                        ,fparty_num_total
                        ,fparty_num_day
                        ,fy_party_num
                        ,fy_party_num_total
                        ,fy_party_num_day
                        ,row_number() over(partition by fbpid, fuid order by fdate desc) row_num
                   from (select fdate
                                ,fbpid
                                ,fuid
                                ,fparty_num
                                ,0 fparty_num_total
                                ,0 fparty_num_day
                                ,fy_party_num
                                ,0 fy_party_num_total
                                ,0 fy_party_num_day
                           from dim.parquet_user_push_gameparty t
                          where dt = date_sub('%(statdate)s', 1)
                          union all
                         select '%(statdate)s' fdate
                                ,fbpid
                                ,fuid
                                ,sum(fparty_num) fparty_num
                                ,sum(fparty_num) fparty_num_total
                                ,case when fparty_num > 0 then 1 else 0 end fparty_num_day
                                ,sum(case when split(fsubname, '_')[1] like '%%100' then fparty_num end) fy_party_num
                                ,sum(case when split(fsubname, '_')[1] like '%%100' then fparty_num end) fy_party_num_total
                                ,case when fparty_num > 0 and split(fsubname, '_')[1] like '%%100' then 1 else 0 end fy_party_num_day
                           from dim.user_gameparty t
                          where dt = '%(statdate)s'
                          group by fbpid,
                                   fuid,
                                   case when fparty_num > 0 then 1 else 0 end,
                                   case when fparty_num > 0 and split(fsubname, '_')[1] like '%%100' then 1 else 0 end
                        ) t
                ) t1
           left join dim.parquet_user_push_gameparty t2
             on t1.fbpid = t2.fbpid
            and t1.fuid = t2.fuid
            and t2.dt = date_sub('%(statdate)s', 1)
          where row_num = 1;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
    insert overwrite table dim.parquet_user_push_pay partition(dt = '%(statdate)s')
         select t1.fdate
                ,t1.fbpid
                ,t1.fuid
                ,coalesce(t1.fpay_num, t2.fpay_num, 0) fpay_num
                ,coalesce(t1.fpay_num_total+t2.fpay_num_total, 0) fpay_num_total
                ,coalesce(t1.fpay_num_day+t2.fpay_num_day, 0) fpay_num_day
           from (select fdate
                        ,fbpid
                        ,fuid
                        ,fpay_num
                        ,fpay_num_total
                        ,fpay_num_day
                        ,row_number() over(partition by fbpid, fuid order by fdate desc) row_num
                   from (select fdate
                                ,fbpid
                                ,fuid
                                ,fpay_num
                                ,0 fpay_num_total
                                ,0 fpay_num_day
                           from dim.parquet_user_push_pay t
                          where dt = date_sub('%(statdate)s', 1)
                          union all
                         select '%(statdate)s' fdate
                                ,fbpid
                                ,fuid
                                ,sum(case when dt = '%(statdate)s' then ftotal_usd_amt end) fpay_num
                                ,sum(ftotal_usd_amt) fpay_num_total
                                ,count(distinct case when ftotal_usd_amt > 0 then dt end) fpay_num_day
                           from dim.user_pay_day t
                          where dt = '%(statdate)s'
                          group by fbpid,
                                   fuid
                        ) t
                ) t1
           left join dim.parquet_user_push_pay t2
             on t1.fbpid = t2.fbpid
            and t1.fuid = t2.fuid
            and t2.dt = date_sub('%(statdate)s', 1)
          where row_num = 1;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
    insert overwrite table dim.parquet_user_push_rupt partition(dt = '%(statdate)s')
         select t1.fdate
                ,t1.fbpid
                ,t1.fuid
                ,coalesce(t1.frupt_num, t2.frupt_num, 0)frupt_num
                ,coalesce(t1.frupt_num_total+t2.frupt_num_total, 0)frupt_num_total
                ,coalesce(t1.frupt_num_day+t2.frupt_num_day, 0)frupt_num_day
           from (select fdate
                        ,fbpid
                        ,fuid
                        ,frupt_num
                        ,frupt_num_total
                        ,frupt_num_day
                        ,row_number() over(partition by fbpid, fuid order by fdate desc) row_num
                   from (select fdate
                                ,fbpid
                                ,fuid
                                ,frupt_num
                                ,0 frupt_num_total
                                ,0 frupt_num_day
                           from dim.parquet_user_push_rupt t
                          where dt = date_sub('%(statdate)s', 1)
                          union all
                         select '%(statdate)s' fdate
                                ,fbpid
                                ,fuid
                                ,sum(case when dt = '%(statdate)s' then frupt_cnt end) frupt_num
                                ,sum(frupt_cnt) frupt_num_total
                                ,count(distinct case when frupt_cnt > 0 then dt end) frupt_num_day
                           from dim.user_bankrupt_relieve t
                          where dt = '%(statdate)s'
                          group by fbpid,
                                   fuid
                        ) t
                ) t1
           left join dim.parquet_user_push_rupt t2
             on t1.fbpid = t2.fbpid
            and t1.fuid = t2.fuid
            and t2.dt = date_sub('%(statdate)s', 1)
          where t1.row_num = 1;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_parquet_user_push_data(sys.argv[1:])
a()
