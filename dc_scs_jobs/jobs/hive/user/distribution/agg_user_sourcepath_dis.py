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

class agg_user_sourcepath_dis(BaseStatModel):

    def create_tab(self):
        hql = """create table if not exists dcnew.user_sourcepath_dis
            (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fsourcepath varchar(100),
                fdsucnt bigint,
                fdaucnt bigint
            )
            partitioned by (dt date);
        """
        res = self.sql_exe(hql)


    def stat(self):
        extend_group = {
                     'fields':[ 'fsource_path'],
                     'groups':[ [1]]
                     }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """
        drop table if exists work.user_sourcepath_dis1_%(statdatenum)s;
        create table work.user_sourcepath_dis1_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               ua.fgame_id, ua.fchannel_code, ua.fuid,
               coalesce(ul.fsource_path,'%(null_str_report)s') fsource_path, ul.fm_imei
        from
            dim.user_act ua
        join
            dim.user_login_additional ul
        on ua.fbpid = ul.fbpid
            and ua.fuid = ul.fuid and ul.dt='%(statdate)s'
        join dim.bpid_map bm
        on ua.fbpid = bm.fbpid
        where ua.dt= '%(statdate)s'
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
                    coalesce(a.fsource_path,'%(null_str_group_rule)s') fsource_path,
                    count(distinct a.fuid) fdaucnt
                from
                    work.user_sourcepath_dis1_%(statdatenum)s a
                where hallmode = %(hallmode)s
                group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
                fchannel_code,fsource_path
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.user_sourcepath_dis2_%(statdatenum)s;
        create table work.user_sourcepath_dis2_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert overwrite table dcnew.user_sourcepath_dis
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
            fsource_path fsourcepath,
            max(fdsucnt) fdsucnt,
            max(fdaucnt) fdaucnt
        from
            (select
                un.fgamefsk,
                un.fplatformfsk,
                un.fhallfsk,
                un.fgame_id,
                un.fterminaltypefsk,
                un.fversionfsk,
                un.fchannel_code,
                un.fsource_path,
                count(distinct un.fuid) fdsucnt,
                0 fdaucnt
            from
                dim.reg_user_array un
            where
                un.dt = '%(statdate)s'
            group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
            un.fversionfsk,un.fchannel_code,un.fsource_path

            union all

            select
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d),
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d),
                coalesce(fsource_path,'%(null_str_report)s') fsource_path,
                0 fdsucnt,
                fdaucnt
            from
                work.user_sourcepath_dis2_%(statdatenum)s) src
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code,fsource_path;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.user_sourcepath_dis1_%(statdatenum)s;
        drop table if exists work.user_sourcepath_dis2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

#愉快的统计要开始啦
a = agg_user_sourcepath_dis(sys.argv[1:])
a()
