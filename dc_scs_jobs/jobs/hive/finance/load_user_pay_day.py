#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class load_user_pay_day(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_pay_day
        (
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户游戏ID
          ftotal_usd_amt       decimal(20,4),       --支付成功总金额，以美元为单位
          fpay_cnt             int,                 --支付成功次数
          fplatform_uid        varchar(50),         --平台uid
          ffirstpay_at         string,              --当天的首付时间
          fmax_usd_amt         decimal(20,4)        --当天最高的一笔单的总金额，以美元为单位 -- 20170728增加字段，为宽表服务
        )
        partitioned by(dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = {'statdate':self.stat_date,
          'null_int_report':sql_const.NULL_INT_REPORT}
        query.update(PublicFunc.date_define(self.stat_date))

        hql ="""
        drop table if exists work.user_stream_%(num_begin)s;
        create table if not exists work.user_stream_%(num_begin)s as
        select fbpid, fplatform_uid, max(fuid) fuid
        from
        (
            select fbpid, fuid, fplatform_uid
            from stage.user_order_stg
            where dt = '%(statdate)s'
                and fuid != 0 and fplatform_uid is not null

            union all

            select fbpid, fuid, fplatform_uid
            from stage.user_signup_stg
            where dt = '%(statdate)s'
                and fuid != 0 and fplatform_uid is not null

            union all

            select fbpid, fuid, fplatform_uid
            from dim.user_login_additional
            where dt = '%(statdate)s'
                and fuid != 0 and fplatform_uid is not null
        ) tt
        group by fbpid, fplatform_uid
        """% query
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        hql = """
        set hive.auto.convert.join = false; --to solve return code3
        insert overwrite table dim.user_pay_day
        partition( dt="%(statdate)s" )
        select
          ps.fbpid,
          coalesce(ugo.fgame_id,%(null_int_report)d) fgame_id,
          coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
          case when max(ps.fuid)>0 then max(ps.fuid) else max(us.fuid) end fuid,
          round(sum(ps.fcoins_num * ps.frate),6) ftotal_usd_amt,
          count(1) fpay_cnt,
          ps.fplatform_uid,
          min(fsucc_time) ffirstpay_at,
          round(max(ps.fcoins_num * ps.frate),6) ffirstpay_at
        from
          stage.payment_stream_stg ps
        left join
          (select
            forder_id,
            max(coalesce(fgame_id,cast (0 as bigint))) fgame_id
            from stage.user_generate_order_stg
            where dt='%(statdate)s' group by forder_id) ugo
        on ps.forder_id  = ugo.forder_id

        left join work.user_stream_%(num_begin)s us
        on ps.fbpid = us.fbpid and ps.fplatform_uid = us.fplatform_uid

        left join
          analysis.marketing_channel_pkg_info ci
        on ps.fchannel_id = ci.fid
        where ps.dt='%(statdate)s'
        group by
        ps.fbpid,
        ugo.fgame_id,
        ps.fplatform_uid;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_user_pay_day(statDate, eid)
a()
