#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_channel_payment_mid(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists stage.channel_market_payment_mid
        (
          fbpid           string,
          fuid            decimal(20),
          fplatform_uid   string,
          fchannel_id     string,
          fdate           string,
          fip             string,
          fcli_os         string,
          fudid           string,
          fpay_money      decimal(20,2),
          fpay_rate       decimal(25,10),
          fpay_mode       string,
          fnow_channel_id string,
          fchannel_id_bak string
        )
        partitioned by (dt date);

        create table if not exists analysis.user_channel_test_device
        (
        fudid     string
        );
        create table if not exists analysis.user_channel_coin_seller
        (
          fbpid string,
          fuid  decimal(20),
          fexpect_channel_id varchar(64)
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []
        hql = """
       insert overwrite table stage.channel_market_payment_mid partition
          (dt = "%(ld_begin)s")
       select a.fbpid,
                  a.fuid,
                  fplatform_uid,
                  ( case when c.fuid is not null then c.fexpect_channel_id else nvl(b.fchannel_id, a.fchannel_id) end ) fchannel_id,
                  '%(ld_begin)s' fdate,
                  fip,
                  fcli_os,
                  a.fudid,
                  fpay_money,
                  fpay_rate,
                  fpay_mode,
                  a.fchannel_id fnow_channel_id,
                  nvl(b.fchannel_id, a.fchannel_id) fchannel_id_bak
             from (
                  select a.*
                     from stage.fd_user_channel_stg a
                     left outer join analysis.user_channel_test_device b
                       on a.fudid = b.fudid
                    where a.fet_id = 5
                      and a.dt = '%(ld_begin)s'
                      and b.fudid is null
             ) a
             left join stage.channel_market_new_reg_mid b
               on a.fbpid = b.fbpid
              and a.fudid = b.fudid
              and b.dt < '%(ld_end)s'
             left join analysis.user_channel_coin_seller c
               on a.fbpid = c.fbpid
              and a.fuid = c.fuid
        """ % self.hql_dict
        hql_list.append( hql )

        # 过滤港云之前新增设备的后续关联付费
        hql = """
       insert overwrite table stage.channel_market_payment_mid partition (dt = "%(ld_begin)s")
       select
        a.fbpid,
        a.fuid,
        a.fplatform_uid,
        a.fchannel_id,
        a.fdate,
        a.fip,
        a.fcli_os,
        a.fudid,
        a.fpay_money,
        a.fpay_rate,
        a.fpay_mode,
        a.fnow_channel_id,
        a.fchannel_id_bak
        from stage.channel_market_payment_mid a
        left join stage.gangyu_delete_channel_id_list b
        on a.fchannel_id = b.fchannel_id
        where a.dt ="%(ld_begin)s"
        and b.fchannel_id is null
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
    a = load_channel_payment_mid(stat_date)
    a()