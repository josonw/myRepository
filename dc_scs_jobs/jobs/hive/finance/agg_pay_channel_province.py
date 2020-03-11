#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_channel_province(BaseStatModel):
    """ 新后台支付通道监控"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_channel_province
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fm_id                       string,
            fcountry                    string,
            fprovince                   string,
            fpayuser_cnt                bigint,
            fpayuser_num                bigint,
            fmoney                      decimal(20,2),
            forder                      bigint
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.user_location_temp_%(statdatenum)s;
        create table work.user_location_temp_%(statdatenum)s
          as
        select b.key.bpid fbpid,
                cast(b.key.uid as bigint) fuid,
                b.country,b.province
                from hbase.user_location_all b
        group by b.key.bpid,b.key.uid,b.country,b.province;

        drop table if exists work.pay_channel_province_%(statdatenum)s;
        create table work.pay_channel_province_%(statdatenum)s as
            select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fplatform_uid, round(a.fcoins_num * a.frate, 6) fincome,
                    a.fm_id,a.pstatus,a.forder_id,
                    coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                    coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                    coalesce(ula.country,'%(null_str_report)s') country,
                    coalesce(ula.province,'%(null_str_report)s') province
            from (
                select a.fbpid, max(case when a.fuid>0 then a.fuid else c.fuid end) fuid,
                        b.fgame_id, b.fchannel_code,
                        a.forder_id,  a.fplatform_uid, a.fcoins_num, a.frate ,a.fm_id, a.pstatus
                    FROM stage.payment_stream_all_stg a
                    LEFT JOIN dim.user_pay_day b
                    on a.fbpid = b.fbpid
                    and a.fplatform_uid = b.fplatform_uid
                    and b.dt='%(statdate)s'
                    LEFT JOIN dim.reg_user_main_additional c
                    on a.fbpid = c.fbpid
                    and a.fuid = c.fuid
                    WHERE a.dt = '%(statdate)s'
                group by a.fbpid, a.forder_id, a.fplatform_uid, a.fcoins_num, a.frate ,a.fm_id, a.pstatus,
                         b.fgame_id, b.fchannel_code
            ) a
            LEFT JOIN work.user_location_temp_%(statdatenum)s ula
            ON ula.fbpid=a.fbpid AND ula.fuid=a.fuid
            JOIN dim.bpid_map b ON a.fbpid = b.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        extend_group = {'fields':['fm_id', 'country', 'province'],
                        'groups':[[1, 1, 1]]
                       }

        hql = """
         select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fm_id,
                country,
                province,
               count(DISTINCT case when a.pstatus=2 then a.forder_id else null end) fpayuser_cnt,
               count(DISTINCT case when a.pstatus=2 then a.fplatform_uid else null end) fpayuser_num,
               sum(case when a.pstatus=2 then fincome else 0 end) fmoney,
               count(*) fordercnt
           from work.pay_channel_province_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                fm_id, country, province
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_channel_province
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.pay_channel_province partition(dt = "%(statdate)s")
        select
            fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannelcode,
            fm_id,
            a.fcountry  fcountry,
            case when (a.fcountry='中国' and b.fprovince is null) then '未知' else a.fprovince end fprovince,
            sum(fpayuser_cnt) fpayuser_cnt,
            sum(fpayuser_num) fpayuser_num,
            sum(fmoney)       fmoney      ,
            sum(forder)       forder
        from dcnew.pay_channel_province a
        left join analysis.geography_info_dim b
        on a.fcountry = b.fcountry
        and a.fprovince = b.fprovince
        where dt = "%(statdate)s"
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannelcode,
            fm_id,
            a.fcountry,
            case when (a.fcountry='中国' and b.fprovince is null) then '未知' else a.fprovince end;

        drop table if exists work.pay_channel_province_%(statdatenum)s;
        drop table if exists work.user_location_temp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_channel_province(sys.argv[1:])
a()
