#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_hour_dis(BaseStatModel):
    """ 付费用户的时段分布"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_hour_dis
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fhourfsk            bigint,      --小时
            fpay_unum            bigint,      --人数
            fpay_cnt              bigint,      --付费次数
            fincome               decimal(20,2),   --付费金额
            fpay_ucnt             bigint          --累计时段付费人数
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fhourfsk'],
                        'groups':[[1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_hour_dis_%(statdatenum)s;
        create table work.pay_user_hour_dis_%(statdatenum)s as
        select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               fbpid, fuid, fgame_id, cast(fchannel_code as bigint) fchannel_code, fhourfsk, fincome, fpaycnt,
               row_number() over(partition by fbpid, fuid order by fhourfsk) rown
          from  (
                select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                            a.fbpid, a.fuid, c.fgame_id, c.fchannel_code, hd.fhour+1 fhourfsk,
                            round(sum(a.fcoins_num * a.frate), 6) fincome,
                            count(1) fpaycnt
                            from stage.payment_stream_stg a
                            join stage.user_generate_order_stg c
                              on a.forder_id=c.forder_id and c.dt='%(statdate)s'
                            join dim.bpid_map b
                              on a.fbpid=b.fbpid
                            join analysis.hour_dim hd
                              on hour(a.fdate) = hd.fhourid
                     where a.dt = '%(statdate)s'
                     group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                            a.fbpid, a.fuid, c.fgame_id, c.fchannel_code, hd.fhour+1
                ) a
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fhourfsk,
                count(distinct a.fuid) fpayusercnt,
                sum(fpaycnt) fpaycnt,
                round(sum(fincome), 2) fincome,
                0 fcum_pun
           from work.pay_user_hour_dis_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fhourfsk
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        drop table if exists work.pay_user_hour_dis2_%(statdatenum)s;
        create table work.pay_user_hour_dis2_%(statdatenum)s as
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select
                fgamefsk,
                fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fhourfsk,
                0 fpayusercnt,
                0 fpaycnt,
                0 fincome,
                count(distinct a.fuid) fcum_pun
           from work.pay_user_hour_dis_%(statdatenum)s a
          where hallmode = %(hallmode)s and rown=1
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fhourfsk
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        insert into table work.pay_user_hour_dis2_%(statdatenum)s
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql="""
        drop table if exists work.pay_user_hour_dis3_%(statdatenum)s;
        create table work.pay_user_hour_dis3_%(statdatenum)s as
         select '%(statdate)s' fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fhourfsk,
                max(fpayusercnt) fpayusercnt,
                max(fpaycnt) fpaycnt,
                max(fincome) fincome,
                0 fcum_pun
           from work.pay_user_hour_dis2_%(statdatenum)s a
          group by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,fhourfsk
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql="""
        insert into table work.pay_user_hour_dis3_%(statdatenum)s
        select '%(statdate)s' fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fhourfsk,
                0 fpayusercnt,
                0 fpaycnt,
                0 fincome,
                sum(fcum_pun) over(partition by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode
                    order by fhourfsk rows between unbounded preceding and current row) fcum_pun
           from work.pay_user_hour_dis2_%(statdatenum)s a
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.pay_user_hour_dis
        partition (dt = "%(statdate)s")
         select '%(statdate)s' fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fhourfsk,
                max(fpayusercnt) fpayusercnt,
                max(fpaycnt) fpaycnt,
                max(fincome) fincome,
                max(fcum_pun) fcum_pun
           from work.pay_user_hour_dis3_%(statdatenum)s a
          group by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,fhourfsk
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_hour_dis_%(statdatenum)s;
        drop table if exists work.pay_user_hour_dis2_%(statdatenum)s;
        drop table if exists work.pay_user_hour_dis3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_hour_dis(sys.argv[1:])
a()
