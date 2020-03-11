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

class agg_pay_income(BaseStatModel):
    def create_tab(self):
        """每日付费各项指标统计"""

        hql = """create table if not exists dcnew.pay_income
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fgroupingid int,
            fdpayucnt bigint,
            fdincome decimal(38,2)
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.pay_income_tmp_b_%(statdatenum)s;
        create table work.pay_income_tmp_b_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               c.hallmode,
                               a.fgame_id,
                               a.fchannel_code,
                               a.fplatform_uid,
                               a.fuid,
                               a.fcoins_num,
                               a.frate
                          from (select fbpid,
                                       coalesce(ugo.fgame_id,%(null_int_report)d) fgame_id,
                                       coalesce(ftrader_id,%(null_int_report)d) fchannel_code,
                                       us.fplatform_uid,
                                       us.fuid,
                                       us.fcoins_num,
                                       us.frate
                                  from (select fbpid,
                                               fchannel_id fchannel_code,
                                               fplatform_uid,
                                               forder_id,
                                               fcoins_num,
                                               frate,
                                               fuid
                                          from stage.payment_stream_stg
                                         where dt='%(statdate)s'
                                       ) us
                                  left join analysis.marketing_channel_pkg_info mp
                                    on us.fchannel_code = mp.fid
                                  left join (select forder_id,
                                                    max(coalesce(fgame_id,cast (0 as bigint))) fgame_id
                                               from stage.user_generate_order_stg
                                              where dt='%(statdate)s'
                                              group by forder_id
                                            ) ugo
                                         on us.forder_id  = ugo.forder_id
                                   ) a
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       %(null_int_report)d fgroupingid,
                       count(distinct fuid) fdpayucnt,
                       round(sum(fcoins_num * frate),2) fdincome
                  from work.pay_income_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.pay_income
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.pay_income_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

#生成统计实例
a = agg_pay_income(sys.argv[1:])
a()
