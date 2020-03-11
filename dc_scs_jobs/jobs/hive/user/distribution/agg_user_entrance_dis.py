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

"""
用户入口分布统计
"""
class agg_user_entrance_dis(BaseStatModel):

    def create_tab(self):

        hql = """create table if not exists dcnew.user_entrance_dis
            (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fentranceid bigint,
                fdsucnt bigint,
                fdsdcnt bigint,
                fdaucnt bigint,
                fdadcnt bigint
            )
            partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:return res


    def stat(self):
        extend_group = {
                     'fields':[ 'fentrance_id'],
                     'groups':[ [1]]
                     }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """
        drop table if exists work.user_entrance_dis1_%(statdatenum)s;
        create table work.user_entrance_dis1_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               ua.fgame_id, ua.fchannel_code, ua.fuid,
               coalesce(ul.fentrance_id,%(null_int_report)d) fentrance_id, ul.fm_imei
        from
            dim.user_act ua
        join (select fbpid,
                      fuid,
                      max(fentrance_id) fentrance_id,
                      max(fm_imei) fm_imei
                 from user_login_stg
                where dt = '%(statdate)s'
                group by fbpid, fuid
              ) ul
        on ua.fbpid = ul.fbpid
            and ua.fuid = ul.fuid
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
                coalesce(a.fentrance_id,%(null_int_group_rule)d) fentrance_id,
                0 fdsucnt,
                0 fdsdcnt,
                count(distinct a.fuid) fdaucnt,
                count(distinct a.fm_imei) fdadcnt
            from
                work.user_entrance_dis1_%(statdatenum)s a
            where hallmode = %(hallmode)s
            group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
            fchannel_code,fentrance_id
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.user_entrance_dis2_%(statdatenum)s;
        create table work.user_entrance_dis2_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res



        hql = """
        insert overwrite table dcnew.user_entrance_dis
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
                fentrance_id fentranceid,
                max(fdsucnt) fdsucnt,
                max(fdsdcnt) fdsdcnt,
                max(fdaucnt) fdaucnt,
                max(fdadcnt) fdadcnt
        from
            (select
                un.fgamefsk,
                un.fplatformfsk,
                un.fhallfsk,
                un.fgame_id,
                un.fterminaltypefsk,
                un.fversionfsk,
                un.fchannel_code,
                un.fentrance_id,
                count(distinct un.fuid) fdsucnt,
                count(distinct un.fm_imei) fdsdcnt,
                0 fdaucnt,
                0 fdadcnt
            from
                dim.reg_user_array un
            where
                un.dt = '%(statdate)s'
            group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
            un.fversionfsk,un.fchannel_code,un.fentrance_id

            union all

            select
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fgame_id,
                fterminaltypefsk,
                fversionfsk,
                fchannel_code,
                fentrance_id,
                0 fdsucnt,
                0 fdsdcnt,
                fdaucnt,
                fdadcnt
            from
                work.user_entrance_dis2_%(statdatenum)s ) src
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code,fentrance_id;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.user_entrance_dis1_%(statdatenum)s;
        drop table if exists work.user_entrance_dis2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

#愉快的统计要开始啦
a = agg_user_entrance_dis(sys.argv[1:])
a()
