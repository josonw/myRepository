#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel

""" 付费用户的渠道分布 各版本的渠道分布
    合并了老后台的两张表analysis.pay_channel_fct,analysis.pay_channel_ver_fct
"""


class agg_pay_user_product_channel_dis(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_product_channel_dis
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fm_fsk                      string,
            fversion_info               string,
            fpayuser_cnt                bigint,          --付费人数
            fpayment_cnt                decimal(20,2),   --付费金额
            fpay_times                  bigint,          --下单数
            fnewpayuser_cnt             bigint,          --首次付费用户付费人数
            fnewpayment_cnt             decimal(20,2),   --首次付费用户付费金额
            fnewpay_times               bigint,          --首次付费用户下单次数
            fordercnt                   bigint           --订单数
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fm_fsk','fversion_info'],
                        'groups':[[1,1],
                                  [1,0] ]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_product_channel_%(statdatenum)s;
        create table work.pay_user_product_channel_%(statdatenum)s as
            select a.fbpid, max(case when a.fuid>0 then a.fuid else c.fuid end),
                        a.forder_id,  a.fuid, a.fcoins_num, a.frate ,a.fm_id, a.pstatus,
                        coalesce(c.fgame_id,%(null_int_report)d) fgame_id,
                        coalesce(c.fchannel_code,%(null_int_report)d) fchannel_code,
                        coalesce(d.fversion_info,'%(null_str_report)s') fversion_info,
                        case when c.dt='%(statdate)s' then c.fuid else null end ffuid
                    FROM stage.payment_stream_all_stg a
                    left join dim.user_pay c
                    on a.fuid = c.fuid and a.fbpid=c.fbpid
                    and a.fbpid = c.fbpid
                    left join stage.user_generate_order_stg d
                    on a.forder_id = d.forder_id
                    WHERE a.dt = '%(statdate)s'
                group by a.fbpid, a.forder_id, a.fuid, a.fcoins_num, a.frate ,a.fm_id, a.pstatus,
                c.fgame_id, c.fchannel_code, coalesce(d.fversion_info,'%(null_str_report)s'),
                case when c.dt='%(statdate)s' then c.fuid else null end
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.pay_user_product_channel2_%(statdatenum)s;
        create table work.pay_user_product_channel2_%(statdatenum)s as
         select
            c.fgamefsk,
            c.fplatformfsk,
            c.fhallfsk,
            c.fterminaltypefsk,
            c.fversionfsk,
            c.hallmode,
            a.fgame_id,
            a.fchannel_code,
           coalesce(b.fm_fsk, cast(a.fm_id as bigint) ) fm_fsk,
           a.fversion_info,
           a.pstatus,
           a.fuid,
           a.ffuid,
           a.forder_id,
           a.fcoins_num * a.frate fpayment_cnt
         from work.pay_user_product_channel_%(statdatenum)s a
         left join analysis.payment_channel_dim b
           on a.fm_id = b.fm_id
         join dim.bpid_map c
         on a.fbpid = c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fm_fsk,
                coalesce(fversion_info,'%(null_str_group_rule)s') fversion_info,
                count(DISTINCT case when a.pstatus=2 then a.fuid else null end) fpayuser_cnt,
                sum(case when a.pstatus=2 then fpayment_cnt else 0 end) fpayment_cnt,
                count(DISTINCT case when a.pstatus=2 then a.forder_id else null end) fpay_times,
                count(DISTINCT case when a.pstatus=2 then a.ffuid else null end) fnewpayuser_cnt,
                sum(case when a.pstatus=2 and a.ffuid is not null then fpayment_cnt else 0 end) fnewpayment_cnt,
                count(DISTINCT case when a.pstatus=2 and a.ffuid is not null then a.forder_id else null end) fnewpay_times,
                count(DISTINCT a.forder_id) fordercnt
           from work.pay_user_product_channel2_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fm_fsk,fversion_info
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_user_product_channel_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_product_channel_%(statdatenum)s;
        drop table if exists work.pay_user_product_channel2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_product_channel_dis(sys.argv[1:])
a()
