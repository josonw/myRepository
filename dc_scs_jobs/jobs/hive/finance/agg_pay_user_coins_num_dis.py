#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel

""" 订单额度，分布
只在不同平台的情况下支付币种会不一样，可以考虑把面额，和金额的两个维度放到同一张表
合并了老后台的两张表analysis.pay_coins_num,analysis.user_pay_money_range_fct,老表的部分段已废弃
"""

class agg_pay_user_coins_num_dis(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_coins_num_dis
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fcoins_num              decimal(20,2),    --订单额度，分布
            fincome                 decimal(20,2),    --订单金额，分布
            fusercnt                bigint,
            fpaycnt                 bigint
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        extend_group = {'fields':['fcoins_num','fincome'],
                        'groups':[[0,1],[1,0] ]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_coins_num_%(statdatenum)s;
        create table work.pay_user_coins_num_%(statdatenum)s as
            select a.fbpid,
                   coalesce(c.fgame_id,%(null_int_report)d) fgame_id,
                   coalesce(c.fchannel_code,%(null_int_report)d) fchannel_code,
                   a.fuid,
                   a.fcoins_num,
                   round(c.ftotal_usd_amt,2) fincome,
                   d.fgamefsk,
                    d.fplatformfsk,
                    d.fhallfsk,
                    d.fterminaltypefsk,
                    d.fversionfsk,
                    d.hallmode,
                    count(1) fpaycnt
               from stage.payment_stream_stg a
               left join dim.user_pay_day c
                 on a.fuid = c.fuid
                and a.fbpid = c.fbpid
                and c.dt='%(statdate)s'
               join dim.bpid_map d
                 on a.fbpid = d.fbpid
              where a.dt = '%(statdate)s'
                group by a.fbpid, c.fgame_id, c.fchannel_code, a.fuid, a.fcoins_num, round(c.ftotal_usd_amt,2),
                       d.fgamefsk, d.fplatformfsk, d.fhallfsk, d.fterminaltypefsk, d.fversionfsk, d.hallmode
        """

        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
          select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                coalesce(fcoins_num,%(null_int_group_rule)d) fcoins_num,
                coalesce(fincome,%(null_int_group_rule)d) fincome,
                count(distinct fuid) fusercnt,
                sum(fpaycnt) fpaycnt
            from work.pay_user_coins_num_%(statdatenum)s a
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
                    fchannel_code,fcoins_num,fincome
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_user_coins_num_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_coins_num_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_coins_num_dis(sys.argv[1:])
a()
