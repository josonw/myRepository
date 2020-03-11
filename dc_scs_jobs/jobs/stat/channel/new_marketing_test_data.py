#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class new_marketing_test_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.marketing_channel_test_data
        (
          fbpid          varchar(64),
          fchannel_id    varchar(64),
          fet_id         bigint,
          fudid          varchar(64),
          fuid           bigint,
          flts_at        string,
          fip            varchar(100),
          fnum           decimal(20,2),
          fis_new_device int
        )
        partitioned by (dt date);        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []
        hql = """
        set hive.mapred.mode = nonstrict;
        set hive.strict.checks.cartesian.product=false;
        insert overwrite table analysis.marketing_channel_test_data partition
          (dt = "%(ld_begin)s")
        select fbpid,
            fchannel_id,
            fet_id,
            fudid,
            fuid,
            flts_at,
            fip,
            fnum,
            fis_new_device
       from (select fbpid,
                    fchannel_id,
                    fet_id,
                    fudid,
                    fuid,
                    flts_at,
                    fip,
                    fnum,
                    fis_new_device,
                    row_number() OVER(PARTITION BY fbpid, fchannel_id, fet_id ORDER BY flts_at DESC) rn
               from (
                     select a.fbpid,
                             a.fchannel_id,
                             a.fet_id,
                             a.fudid,
                             max(a.fuid) fuid,
                             max(a.flts_at) flts_at,
                             max(a.fip) fip,
                             case  when a.fet_id = 5 then  sum(fpay_money * fpay_rate / coalesce(rate.rate, 0.157176) ) else  count(1)  end fnum,
                             case  when max(b.fudid) is not null then  1  else 0 end fis_new_device
                       from stage.fd_user_channel_stg a
                       left join stage.channel_market_new_reg_mid b
                         on a.fbpid = b.fbpid
                        and a.fudid = b.fudid
                        and b.dt = '%(ld_begin)s'
                        LEFT JOIN (
                            select * from stage.paycenter_rate rate
                            where
                                rate.dt='%(ld_begin)s' AND rate.unit='CNY'
                            limit 1
                        ) rate
                            ON rate.dt='%(ld_begin)s' AND rate.unit='CNY'
                      where a.dt = '%(ld_begin)s'
                      group by a.fbpid, a.fchannel_id, a.fet_id, a.fudid
                     ) c
             ) d
      where rn <= 50

        """ % self.hql_dict
        hql_list.append( hql )

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
    a = new_marketing_test_data(stat_date)
    a()
