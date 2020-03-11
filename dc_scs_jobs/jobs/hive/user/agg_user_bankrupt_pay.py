# -*- coding: UTF-8 -*-
#src     :stage.user_bankrupt_relieve_stg,analysis.grade_dim,dim.bpid_map,stage.user_bankrupt_stg,stage.payment_stream_stg,dim.user_pay
#dst     :dcnew.user_bankrupt_pay
#authot  :SimonRen
#date    :2016-09-05


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


class agg_user_bankrupt_pay(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_bankrupt_pay (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fbank_unum          bigint        comment '破产人数',
               fbankpay_unum       bigint        comment '破产付费人数',
               fbankpay_cnt        bigint        comment '破产付费次数',
               fstatus_pu          bigint        comment '下单人数',
               fstatus_pcnt        bigint        comment '下单次数',
               fstatus_income      decimal(20,2) comment '下单付费额度'
               )comment '破产用户付费'
               partitioned by(dt date)
        location '/dw/dcnew/user_bankrupt_pay'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--取出破产用户及破产付费用户
        drop table if exists work.user_bankrupt_paytmp_br_%(statdatenum)s;
        create table work.user_bankrupt_paytmp_br_%(statdatenum)s as
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                       coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                       a.fuid,
                       sum(fbank_cnt) fbank_cnt,
                       count(distinct pay_fuid) pay_unum,
                       sum(pay_cnt) pay_cnt,
                       sum(fstatus_cnt) fstatus_cnt,
                       sum(fstatus_income) fstatus_income
                  from (select a.fbpid,
                               a.fuid,
                               a.fgame_id,
                               a.fchannel_code,
                               count(1) fbank_cnt, --破产次数
                               case when b.fdate <= from_unixtime(unix_timestamp(a.frupt_at)+1800)
                               then b.fuid end pay_fuid, --破产付费人数
                               count(case when b.fdate <= from_unixtime(unix_timestamp(a.frupt_at)+1800)
                                     then b.fuid end ) pay_cnt,  --破产付费次数
                               0 fstatus_cnt,
                               0 fstatus_income
                          from (select fbpid, fuid, fgame_id, fchannel_code, frupt_at
                                  from stage.user_bankrupt_stg
                                 where dt = '%(statdate)s'
                               ) a
                          left join stage.payment_stream_stg b
                           on a.fbpid = b.fbpid
                          and a.fuid = b.fuid
                          and b.dt = '%(statdate)s'
                        group by a.fbpid,
                                 a.fuid,
                                 a.fgame_id,
                                 a.fchannel_code,
                                 case when b.fdate <= from_unixtime(unix_timestamp(a.frupt_at)+1800)
                               then b.fuid end
                         union all
                        select a.fbpid,
                               a.fuid,
                               a.fgame_id,
                               a.fchannel_code,
                               0 fbank_cnt,
                               null pay_fuid,
                               0 pay_cnt,
                               count(1) fstatus_cnt, --下单次数
                               sum(nvl(b.fcoins_num * b.frate, 0)) fstatus_income --下单付费额度
                          from stage.user_generate_order_stg a
                          join stage.payment_stream_all_stg b
                            on a.forder_id = b.forder_id
                           and b.dt = '%(statdate)s'
                         where a.dt = '%(statdate)s'
                           and a.fbankrupt = 1
                         group by a.fbpid, a.fuid, a.fgame_id, a.fchannel_code
                       ) a
                  join dim.bpid_map c
                    on a.fbpid = c.fbpid
                 group by c.fgamefsk, c.fplatformfsk, c.fhallfsk, c.fterminaltypefsk, c.fversionfsk, c.hallmode,
                          a.fgame_id, a.fchannel_code,a.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合破产用户及破产付费用户
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       count(distinct case when fbank_cnt >0 then gs.fuid end) fbank_unum,
                       count(distinct case when pay_unum >0 then gs.fuid end ) fbankpay_unum,
                       sum(pay_cnt) fbankpay_cnt,
                       count(distinct case when fstatus_cnt >0 then gs.fuid end )  fstatus_pu,
                       sum(fstatus_cnt) fstatus_cnt,
                       round(sum(fstatus_income), 2) fstatus_income
                  from work.user_bankrupt_paytmp_br_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code

        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.user_bankrupt_pay
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_bankrupt_paytmp_br_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_bankrupt_pay(sys.argv[1:])
a()
