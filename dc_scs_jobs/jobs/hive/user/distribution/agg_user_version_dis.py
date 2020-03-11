#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class agg_user_version_dis(BaseStatModel):
    """ 用户版本信息分布 """
    def create_tab(self):

        hql = """create table if not exists dcnew.user_version_dis
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fdimension varchar(32),
            fvername varchar(32),
            fverucnt bigint,
            fverdcnt bigint
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:return res


    def stat(self):

        extend_group = {
                     'fields':[ 'fversion_info'],
                     'groups':[ [1]]
                     }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res
        hql = """
        drop table if exists work.user_version_dis1_%(statdatenum)s;
        create table work.user_version_dis1_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               ua.fgame_id, ua.fchannel_code, ua.fuid,
               coalesce(ul.fversion_info,'0') fversion_info,
               coalesce(ul.fm_imei, '0') fm_imei
        from
            dim.user_act ua
        left join (
            select fbpid, fuid, fversion_info, fm_imei
              from dim.user_login_additional
             where dt= '%(statdate)s'
            union all
            select fbpid, fuid, fversion_info, fm_imei
              from dim.reg_user_main_additional
             where dt= '%(statdate)s'
        ) ul
            on ua.fbpid = ul.fbpid
            and ua.fuid = ul.fuid
        join dim.bpid_map bm
        on ua.fbpid = bm.fbpid
        where ua.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
            select
                a.fgamefsk,
                coalesce(a.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(a.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(a.fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(a.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(a.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(a.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                coalesce(fversion_info,'%(null_str_group_rule)s') fvername,
                count(distinct fuid) fverucnt,
                count(distinct fm_imei) fverdcnt
            from
                work.user_version_dis1_%(statdatenum)s a
            where hallmode = %(hallmode)s
            group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
            fchannel_code,fversion_info
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.user_version_dis2_%(statdatenum)s;
        create table work.user_version_dis2_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.user_version_dis
        partition(dt = '%(statdate)s')
        select
            '%(statdate)s' fdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            'register' fdimension,
            coalesce(un.fversion_info,'%(null_str_report)s') fvername,
            count(distinct un.fuid) fverucnt,
            count(distinct un.fm_imei) fverdcnt
        from
            dim.reg_user_array un
        where
            un.dt = '%(statdate)s'
        group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,un.fversionfsk,un.fchannel_code, un.fversion_info

        union all

        select
            '%(statdate)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code fchannelcode,
            'active' fdimension,
            fvername,
            fverucnt,
            fverdcnt
        from
            work.user_version_dis2_%(statdatenum)s src
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.user_version_dis1_%(statdatenum)s;
        drop table if exists work.user_version_dis2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

#愉快的统计要开始啦
#生成统计实例
a = agg_user_version_dis(sys.argv[1:])
a()
