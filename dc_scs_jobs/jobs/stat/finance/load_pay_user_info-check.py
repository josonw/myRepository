#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_pay_user_info(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists stage.user_pay_info_check
        (
        fbpid            string,
        fuid             bigint,
        fpay_at          date,
        fplatform_uid    string,
        ffirstpay_at     string
        )
        partitioned by (dt date);

        create table if not exists stage.pay_user_mid_check
        (
        fbpid               string,
        fuid                bigint,
        fplatform_uid       string,
        ffirst_pay_at       date,
        ffirst_pay_income   decimal(20,2)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        insert overwrite table stage.user_pay_info_check partition
        (dt = "%(stat_date)s")
        select distinct a.fbpid,
               nvl(b.fuid, c.fuid) fuid,
               '%(stat_date)s' fpay_at,
               a.fplatform_uid,
               null
        from
        (select fbpid, fplatform_uid
                from stage.payment_stream_stg
               where dt = '%(stat_date)s'
               group by fbpid, fplatform_uid
        ) a
        left join
        (
            select fbpid, fuid, fplatform_uid
                     from stage.user_order_stg
                    where dt = '%(stat_date)s'
                      and fuid != 0
                    group by fbpid, fuid, fplatform_uid
        ) b
          on a.fplatform_uid = b.fplatform_uid
         and a.fbpid = b.fbpid
        left join stage.user_dim c
          on a.fbpid = c.fbpid
         and a.fplatform_uid = c.fplatform_uid
       where nvl(b.fuid, c.fuid) is not null
       """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table stage.pay_user_mid_check partition
        (dt = "%(stat_date)s")
           select a.fbpid, b.fuid, a.fplatform_uid, '%(stat_date)s' ffirst_pay_at, a.dip ffirst_pay_income
          from (
            select a.fbpid,
                       a.fplatform_uid,
                       sum(round(a.fcoins_num * a.frate, 2)) dip
                  from stage.payment_stream_stg a
                  left join stage.pay_user_mid b
                    on a.fbpid = b.fbpid
                   and a.fplatform_uid = b.fplatform_uid
                 where a.dt = '%(stat_date)s'
                   and b.fplatform_uid is null
                 group by a.fbpid, a.fplatform_uid
               ) a
          left join stage.user_pay_info b
            on a.fbpid = b.fbpid
           and a.fplatform_uid = b.fplatform_uid
           and b.dt = '%(stat_date)s'
      group by a.fbpid, b.fuid, a.fplatform_uid, a.dip
        """ % self.hql_dict
        hql_list.append( hql )

        # result = 0
        result = self.exe_hql_list(hql_list)
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = load_pay_user_info(stat_date)
    a()
