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

class agg_user_gender_dis(BaseStatModel):

    def create_tab(self):

        hql = """create table if not exists dcnew.user_gender_dis
            (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fdsucnt bigint,
                fdsmaleucnt bigint,
                fdsfemaleucnt bigint,
                fdaucnt bigint,
                fdamaleucnt bigint,
                fdafemaleucnt bigint
            )
            partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:return res


    def stat(self):

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res
        hql = """
        drop table if exists work.user_gender_dis1_%(statdatenum)s;
        create table work.user_gender_dis1_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               ua.fgame_id, ua.fchannel_code, ua.fuid,
               coalesce(un.fgender,'%(null_str_report)s') fgender
        from
            dim.user_act ua
        left join
            (select
                fbpid,fuid,max(fgender) fgender
            from dim.reg_user_main_additional
            where dt <= '%(statdate)s'
            group by fbpid,fuid) un
        on ua.fbpid = un.fbpid
            and ua.fuid = un.fuid
        join
            dim.bpid_map bm
        on ua.fbpid = bm.fbpid
        where dt='%(statdate)s'
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
                count(distinct a.fuid)  fdaucnt,
                count(distinct case when a.fgender = 1 then a.fuid else null end) fdamaleucnt,
                count(distinct case when a.fgender = 0 then a.fuid else null end) fdafemaleucnt
            from
                work.user_gender_dis1_%(statdatenum)s a
            where hallmode = %(hallmode)s
            group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
            fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        drop table if exists work.user_gender_dis2_%(statdatenum)s;
        create table work.user_gender_dis2_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert overwrite table dcnew.user_gender_dis
        partition(dt = '%(statdate)s')
        select
            '%(statdate)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code fchannelcode,
            max(fdsucnt) fdsucnt,
            max(fdsmaleucnt) fdsmaleucnt,
            max(fdsfemaleucnt) fdsfemaleucnt,
            max(fdaucnt) fdaucnt,
            max(fdamaleucnt) fdamaleucnt,
            max(fdafemaleucnt) fdafemaleucnt
        from
            (select
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fgame_id,
                fterminaltypefsk,
                fversionfsk,
                fchannel_code,
                count(distinct un.fuid) fdsucnt,
                count(distinct case when un.fgender = 1 then un.fuid else null end) fdsmaleucnt,
                count(distinct case when un.fgender = 0 then un.fuid else null end) fdsfemaleucnt,
                0 fdaucnt,
                0 fdamaleucnt,
                0 fdafemaleucnt
            from
                dim.reg_user_array un
            where
                un.dt = '%(statdate)s'
            group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
            un.fversionfsk,un.fchannel_code

            union all

            select
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fgame_id,
                fterminaltypefsk,
                fversionfsk,
                fchannel_code,
                0 fdsucnt,
                0 fdsmaleucnt,
                0 fdsfemaleucnt,
                fdaucnt,
                fdamaleucnt,
                fdafemaleucnt
            from work.user_gender_dis2_%(statdatenum)s
        ) src
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
            fversionfsk,fchannel_code;
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.user_gender_dis1_%(statdatenum)s;
        drop table if exists work.user_gender_dis2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

#愉快的统计要开始啦
#生成统计实例
a = agg_user_gender_dis(sys.argv[1:])
a()
