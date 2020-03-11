#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_prod_channel_dis(BaseStatModel):
    """ 付费用户的产品渠道分布"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_prod_channel_dis
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fp_fsk          string,
            fm_fsk          string,
            fpayusercnt    bigint,
            fpaycnt       bigint,           --付费次数  总即成功订单次数
            fincome      decimal(20,2),
            fpay_scene    string,           --付费场景  产品后期新加的统计维度
            forder_cnt    bigint            --订单次数  总次数 包含成功与未成功
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fp_fsk','fm_fsk','fpay_scene'],
                        'groups':[[0,0,0],
                                  [1,0,0],
                                  [1,1,0],
                                  [1,0,1],
                                  [1,1,1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_prod_channel_dis_%(statdatenum)s;
        create table work.pay_user_prod_channel_dis_%(statdatenum)s as
            select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid,
                    coalesce(c.fgame_id,%(null_int_report)d) fgame_id,
                    coalesce(c.fchannel_code,%(null_int_report)d) fchannel_code,
                    case when coalesce(a.fproduct_id,'0') = '0' then a.fp_id else a.fproduct_id end fp_fsk,
                    coalesce(a.fm_id,'%(null_str_report)s') fm_fsk,
                    coalesce(e.fpay_scene,'%(null_str_report)s') fpay_scene,
                    sum(a.fcoins_num * a.frate) fincome,
                    count(distinct a.forder_id) fpaycnt
              from stage.payment_stream_stg a
              join dim.user_pay_day c
                on a.fbpid = c.fbpid and a.fuid = c.fuid and c.dt = '%(statdate)s'
              left join dim.user_pay_payscene e
                on a.fuid = e.fuid and a.fbpid=e.fbpid and a.forder_id = e.forder_id
              join dim.bpid_map b
                on a.fbpid=b.fbpid
             where a.dt = '%(statdate)s'
             group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, c.fgame_id, c.fchannel_code,
                    case when coalesce(a.fproduct_id,'0') = '0' then a.fp_id else a.fproduct_id end,
                    coalesce(e.fpay_scene,'%(null_str_report)s') ,
                    a.fm_id
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                coalesce(fp_fsk,'%(null_str_group_rule)s') fp_fsk,
                coalesce(fm_fsk,'%(null_str_group_rule)s') fm_fsk,
                count(DISTINCT a.fuid) fpayusercnt,
                sum(fpaycnt) fpaycnt,
                round(sum(fincome),2) fincome,
                coalesce(fpay_scene,'%(null_str_group_rule)s') fpay_scene,
                0 forder_cnt
           from work.pay_user_prod_channel_dis_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                fp_fsk,fm_fsk,fpay_scene
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.pay_user_prod_c_%(statdatenum)s;
        create table work.pay_user_prod_c_%(statdatenum)s as
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_prod_channel_d_%(statdatenum)s;
        create table work.pay_user_prod_channel_d_%(statdatenum)s as
            select c.fgamefsk, c.fplatformfsk, c.fhallfsk, c.fterminaltypefsk, c.fversionfsk,c.hallmode,
                   a.fbpid, a.fuid,
                   coalesce(b.fgame_id,cast (0 as bigint)) fgame_id,
                   coalesce(cast (a.fchannel_id as bigint),%(null_int_report)d) fchannel_code,
                   case when d.forder_id is not null and coalesce(d.fproduct_id,'0') = '0' then d.fp_id
                        when d.forder_id is not null then d.fproduct_id
                        when d.forder_id is null and coalesce(a.fproduct_id,'0') = '0' then a.fp_id
                   else a.fproduct_id end fp_fsk,
                   coalesce(d.fm_id,cast (a.fpmode as string),'%(null_str_report)s') fm_fsk,
                   coalesce(b.fpay_scene,'%(null_str_report)s') fpay_scene,
                   a.forder_id
              from stage.payment_stream_all_stg a
              left join stage.user_generate_order_stg b
                on a.forder_id = b.forder_id
               and b.dt = '%(statdate)s'
              left join stage.payment_stream_stg d
                on a.forder_id = d.forder_id
               and d.dt = '%(statdate)s'
              join dim.bpid_map c
                on a.fbpid=c.fbpid
             where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                coalesce(fp_fsk,'%(null_str_group_rule)s') fp_fsk,
                coalesce(fm_fsk,'%(null_str_group_rule)s') fm_fsk,
                0 fpayusercnt,
                0 fpaycnt,
                0 fincome,
                coalesce(fpay_scene,'%(null_str_group_rule)s') fpay_scene,
                count(distinct forder_id) forder_cnt
           from work.pay_user_prod_channel_d_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                fp_fsk,fm_fsk,fpay_scene
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table work.pay_user_prod_c_%(statdatenum)s
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.pay_user_prod_channel_dis
        partition (dt = "%(statdate)s")
         select '%(statdate)s' fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fp_fsk,
                fm_fsk,
                max(fpayusercnt) fpayusercnt,
                max(fpaycnt) fpaycnt,
                max(fincome) fincome,
                fpay_scene,
                max(forder_cnt) forder_cnt
           from work.pay_user_prod_c_%(statdatenum)s a
          group by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                fp_fsk,fm_fsk,fpay_scene
        """

        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.pay_user_prod_channel_dis_%(statdatenum)s;
        drop table if exists work.pay_user_prod_c_%(statdatenum)s;
        drop table if exists work.pay_user_prod_channel_d_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_prod_channel_dis(sys.argv[1:])
a()
