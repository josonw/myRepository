#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_generate_order(BaseStatModel):
    """ 付费用户订单多类分布汇总表"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_generate_order
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fpname                      string,
            fsubname                    string,
            fante                       string,
            fpay_scene                  string,
            fis_bankrupt                int,
            fuser_type                  int,
            fpm_name                    string,
            forder_cnt                  bigint,
            forder_unum                 bigint,
            fpay_cnt                    bigint,
            fpay_unum                   bigint,
            fincome                     decimal(20,2)
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fpname','fsubname','fante','fpay_scene', 'fis_bankrupt', 'fpm_name'],
                        'groups':[[1,1,0,0,0,0],
                                  [0,0,0,1,0,0],
                                  [0,0,0,1,0,1],
                                  [0,0,0,0,0,1],
                                  [1,1,1,1,1,1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_generate_order_%(statdatenum)s;
        create table work.pay_user_generate_order_%(statdatenum)s as
            select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fgame_id, a.fchannel_code, a.fgameparty_pname fpname,a.forder_id,
                    a.fgameparty_subname fsubname, a.fgameparty_anto fante, a.fpay_scene, fbankrupt fis_bankrupt,
                    a.fpm_name, a.fuid, coalesce(a.fincome,0) fincome, c.fuid fruid, d.fuid ffuid
                from dim.user_pay_payscene a
                left join dim.reg_user_main_additional c
                  on a.fbpid = c.fbpid
                 and a.fuid = c.fuid
                 and c.dt = '%(ld_begin)s'
                left join dim.user_pay d
                  on a.fbpid = d.fbpid
                 and a.fuid = d.fuid
                 and d.dt = '%(ld_begin)s'
                join dim.bpid_map b
                  on a.fbpid=b.fbpid
               where a.dt = '%(statdate)s'
         group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,a.forder_id,
                    a.fbpid, a.fgame_id, a.fchannel_code, a.fgameparty_pname, a.fgameparty_subname, a.fgameparty_anto, a.fpay_scene,
                    a.fbankrupt, a.fpm_name, a.fuid, a.fincome, c.fuid, d.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
         select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                coalesce(fpname, '%(null_str_group_rule)s') fpname,
                coalesce(fsubname, '%(null_str_group_rule)s') fsubname,
                coalesce(fante, %(null_int_group_rule)d) fante,
                coalesce(fpay_scene, '%(null_str_group_rule)s') fpay_scene,
                coalesce(fis_bankrupt, %(null_int_group_rule)d) fis_bankrupt,
                1 fuser_type,
                coalesce(fpm_name, '%(null_str_group_rule)s') fpm_name,
                count(distinct a.forder_id) forder_cnt,
                count(distinct a.fuid) forder_unum,
                count(distinct case when fincome > 0 then a.forder_id end) fpay_cnt,
                count(distinct case when fincome > 0 then a.fuid end) fpay_unum,
                round(sum(fincome), 2) fincome
           from work.pay_user_generate_order_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                   fpname,fsubname,fante,fpay_scene,fis_bankrupt,fpm_name
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_user_generate_order
        partition (dt = "%(statdate)s")
        %(sql_template)s
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
                coalesce(fpname, '%(null_str_group_rule)s') fpname,
                coalesce(fsubname, '%(null_str_group_rule)s') fsubname,
                coalesce(fante, %(null_int_group_rule)d) fante,
                coalesce(fpay_scene, '%(null_str_group_rule)s') fpay_scene,
                coalesce(fis_bankrupt, %(null_int_group_rule)d) fis_bankrupt,
                2 fuser_type,
                coalesce(fpm_name, '%(null_str_group_rule)s') fpm_name,
                count(distinct case when a.fruid is not null then a.forder_id end) forder_cnt,
                count(distinct case when a.fruid is not null then  a.fuid end)  forder_unum,
                count(distinct case when a.fruid is not null and fincome > 0 then a.forder_id end) fpay_cnt,
                count(distinct case when a.fruid is not null and fincome > 0 then  a.fuid end) fpay_unum,
                round(sum(case when a.fruid is not null then fincome end), 2) fincome
           from work.pay_user_generate_order_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                   fpname,fsubname,fante,fpay_scene,fis_bankrupt,fpm_name
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table dcnew.pay_user_generate_order
        partition (dt = "%(statdate)s")
        %(sql_template)s
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
                coalesce(fpname, '%(null_str_group_rule)s') fpname,
                coalesce(fsubname, '%(null_str_group_rule)s') fsubname,
                coalesce(fante, %(null_int_group_rule)d) fante,
                coalesce(fpay_scene, '%(null_str_group_rule)s') fpay_scene,
                coalesce(fis_bankrupt, %(null_int_group_rule)d) fis_bankrupt,
                3 fuser_type,
                coalesce(fpm_name, '%(null_str_group_rule)s') fpm_name,
                count(distinct case when a.ffuid is not null then a.forder_id end) forder_cnt,
                count(distinct case when a.ffuid is not null then  a.fuid end)  forder_unum,
                count(distinct case when a.ffuid is not null and fincome > 0 then a.forder_id end) fpay_cnt,
                count(distinct case when a.ffuid is not null and fincome > 0 then a.fuid end) fpay_unum,
                round(sum(case when a.ffuid is not null then fincome end), 2) fincome
           from work.pay_user_generate_order_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                    fpname,fsubname,fante,fpay_scene,fis_bankrupt,fpm_name
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table dcnew.pay_user_generate_order
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_generate_order_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_generate_order(sys.argv[1:])
a()
