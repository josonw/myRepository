#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


""" 新后台地图的用户位置分布
     三种组合，fip_country,fip_countrycode
               fip_country,fip_countrycode,fip_province
               fip_country,fip_countrycode,fip_province,fip_city
    省去了agg_user_location_pg_day.py 的任务
"""


class agg_user_position_info(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.user_position_info
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fip_country      varchar(128),
            fip_province     varchar(128),
            fip_city         varchar(128),
            fip_countrycode  varchar(32),
            fregusercnt      bigint,
            factusercnt      bigint,
            fpayusercnt      bigint,
            fincome          decimal(38,2)
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result



    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fip_country','fip_province','fip_city','fip_countrycode'],
                        'groups':[[1,1,1,1],
                                  [0,1,0,1],
                                  [1,0,0,1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """
        drop table if exists work.user_position_info_%(statdatenum)s;
        create table work.user_position_info_%(statdatenum)s as
            select fbpid,fuid,fgame_id,
                   max(fplatform_uid) fplatform_uid, max(fchannel_code) fchannel_code,
                max(fincome) fincome, max(is_act) is_act, max(is_reg) is_reg, max(is_pay) is_pay
              from
              (
                select fbpid,fuid,null fplatform_uid,fgame_id,fchannel_code,0 fincome, 1 is_act, 0 is_reg,0 is_pay
                  from dim.user_act
                 where dt='%(statdate)s'
                union all

                select fbpid,fuid, null fplatform_uid, %(null_int_report)d fgame_id,fchannel_code,0 fincome, 0 is_act, 1 is_reg,0 is_pay
                  from dim.reg_user_main_additional
                 where dt='%(statdate)s'
                union all

                select fbpid,fuid,fplatform_uid,fgame_id,fchannel_code, ftotal_usd_amt fincome, 0 is_act, 0 is_reg,1 is_pay
                  from dim.user_pay_day
                 where dt='%(statdate)s'
                ) t
             group by fbpid,fuid,fgame_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql ="""
        drop table if exists work.user_position_info2_%(statdatenum)s;
        create table work.user_position_info2_%(statdatenum)s as
        select fbpid,fuid,fip_country,fip_province,fip_city,fip,fip_countrycode
                    from
                         (SELECT fbpid,fuid,
                          fip_country,
                          case when fip_province='中国' then '%(null_str_report)s' else fip_province end fip_province,
                          fip_city,
                          fip,
                          case when fip_countrycode IN ('TW','HK','MO') then 'CN' else fip_countrycode end fip_countrycode,
                          row_number() over(partition by fbpid,fuid order by flogin_at desc) as rn
                          FROM dim.user_login_additional a
                          where dt='%(statdate)s'
                         ) c
                    where rn=1
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql="""
        drop table if exists work.user_position_info3_%(statdatenum)s;
        create table work.user_position_info3_%(statdatenum)s as
        select a.fbpid,a.fuid,a.fgame_id,a.fchannel_code, a.fincome,
               c.fip_country, c.fip_province, c.fip_city, c.fip, c.fip_countrycode,a.fplatform_uid,
               b.fgamefsk,b.fplatformfsk,b.fhallfsk,b.fterminaltypefsk,b.fversionfsk,b.hallmode,
               a.is_reg,a.is_act,a.is_pay
          from work.user_position_info_%(statdatenum)s a
          left join work.user_position_info2_%(statdatenum)s c
            on a.fbpid=c.fbpid and a.fuid=c.fuid
          join dim.bpid_map b
            on a.fbpid=b.fbpid
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
                coalesce(fip_country ,'%(null_str_group_rule)s') fip_country,
                coalesce(fip_province ,'%(null_str_group_rule)s') fip_province,
                coalesce(fip_city ,'%(null_str_group_rule)s') fip_city,
                fip_countrycode,
                count(DISTINCT CASE WHEN is_reg =1 THEN fuid ELSE NULL END) AS fregusercnt,
                count(DISTINCT CASE WHEN is_act =1 THEN fuid ELSE NULL END) AS factusercnt,
                count(DISTINCT CASE WHEN is_pay =1 THEN fplatform_uid ELSE NULL END) AS fpayusercnt,
                sum(fincome) AS fincome
           from work.user_position_info3_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fip_country,fip_province,fip_city,fip_countrycode
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.user_position_info
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.user_position_info_%(statdatenum)s;
        drop table if exists work.user_position_info2_%(statdatenum)s;
        drop table if exists work.user_position_info3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_user_position_info(sys.argv[1:])
a()
