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

class agg_user_source_dis(BaseStatModel):

    def create_tab(self):

        hql = """create table if not exists dcnew.user_source_dis
            (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fdsucnt bigint,               --fother_cnt,新增用户数
                fdsfeeducnt bigint,           --ffeed_cnt
                fdspushucnt bigint,           --push_cnt
                fdsaducnt bigint,             --fad_cnt
                fdaucnt bigint,
                fdafeeducnt bigint,
                fdapushucnt bigint,
                fdaaducnt bigint
            )
            partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:return res


    def stat(self):
        extend_group = {
                     'fields':[ 'fuid'],
                     'groups':[ [1]]
                     }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.user_source_dis11_%(statdatenum)s;
        create table work.user_source_dis11_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               %(null_int_report)d fgame_id, %(null_int_report)d fchannel_code, fc.fuid
        from
            stage.feed_clicked_stg fc
        join
            dim.bpid_map bm
        on
            fc.fbpid = bm.fbpid
        where dt = '%(statdate)s' and ffeed_as = 2
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
            fuid
        from
            work.user_source_dis11_%(statdatenum)s a
        where hallmode = %(hallmode)s
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.user_source_dis12_%(statdatenum)s;
        create table work.user_source_dis12_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.user_source_dis21_%(statdatenum)s;
        create table work.user_source_dis21_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               %(null_int_report)d fgame_id, %(null_int_report)d fchannel_code, ps.fuid
        from
            stage.push_send_stg ps
        join
            dim.bpid_map bm
        on
            ps.fbpid = bm.fbpid
        where dt >= '%(ld_29day_ago)s'
           and dt <= '%(statdate)s'
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
            fuid
        from
            work.user_source_dis21_%(statdatenum)s a
        where hallmode = %(hallmode)s
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.user_source_dis22_%(statdatenum)s;
        create table work.user_source_dis22_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.user_source_dis31_%(statdatenum)s;
        create table work.user_source_dis31_%(statdatenum)s as
        select bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
               ua.fgame_id, ua.fchannel_code, coalesce(ul.fad_code,'%(null_str_report)s') fad_code,
               ua.fuid afuid, fc.fuid cfuid, ps.fuid sfuid,  ul.fuid lfuid
        from
            dim.user_act ua
        left join
            (select fbpid,fuid from stage.feed_clicked_stg
            where dt = '%(statdate)s' and ffeed_as = 1) fc
        on ua.fbpid = fc.fbpid
            and ua.fuid = fc.fuid
        left join
            (select fbpid,fuid from stage.push_send_stg where dt = '%(statdate)s') ps
        on ua.fbpid = ps.fbpid
            and ua.fuid = ps.fuid
        left join
            (select fbpid,fuid,fad_code
                from dim.user_login_additional where dt = '%(statdate)s') ul
        on ua.fbpid = ul.fbpid
            and ua.fuid = ul.fuid
        left join dim.bpid_map bm
        on ua.fbpid = bm.fbpid

        where ua.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        select
            fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            count(distinct afuid) fdaucnt,
            count(distinct cfuid) fdafeeducnt,
            count(distinct sfuid) fdapushucnt,
            count(distinct case when fad_code is not null and fad_code <> '' and fad_code <> '%(null_str_report)s' then lfuid else null end) fdaaducnt
        from work.user_source_dis31_%(statdatenum)s
        where hallmode = %(hallmode)s
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        drop table if exists work.user_source_dis32_%(statdatenum)s;
        create table work.user_source_dis32_%(statdatenum)s as
         %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.user_source_dis
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
            max(fdsfeeducnt) fdsfeeducnt,
            max(fdspushucnt) fdspushucnt,
            max(fdsaducnt) fdsaducnt,
            max(fdaucnt) fdaucnt,
            max(fdafeeducnt) fdafeeducnt,
            max(fdapushucnt) fdapushucnt,
            max(fdaaducnt) fdaaducnt
        from
            (select
                un.fgamefsk,
                un.fplatformfsk,
                un.fhallfsk,
                un.fgame_id,
                un.fterminaltypefsk,
                un.fversionfsk,
                un.fchannel_code,
                count(distinct un.fuid) fdsucnt,
                count(distinct fcu.fuid) fdsfeeducnt,
                count(distinct psu.fuid) fdspushucnt,
                count(distinct case when un.fad_code is not null and un.fad_code <> '' and un.fad_code <> '%(null_str_report)s' then un.fuid else null end) fdsaducnt,
                0 fdaucnt,
                0 fdafeeducnt,
                0 fdapushucnt,
                0 fdaaducnt
            from
                dim.reg_user_array un

            left join work.user_source_dis12_%(statdatenum)s fcu
              on un.fuid = fcu.fuid
             and un.fgamefsk = fcu.fgamefsk
             and un.fplatformfsk = fcu.fplatformfsk
             and un.fhallfsk = fcu.fhallfsk
             and un.fterminaltypefsk = fcu.fterminaltypefsk
             and un.fversionfsk = fcu.fversionfsk
             and fcu.fgame_id = %(null_int_group_rule)d
             and fcu.fchannel_code = %(null_int_group_rule)d

            left join work.user_source_dis22_%(statdatenum)s psu
              on un.fuid = psu.fuid
             and un.fgamefsk = psu.fgamefsk
             and un.fplatformfsk = psu.fplatformfsk
             and un.fhallfsk = psu.fhallfsk
             and un.fterminaltypefsk = psu.fterminaltypefsk
             and un.fversionfsk = psu.fversionfsk
             and psu.fgame_id = %(null_int_group_rule)d
             and psu.fchannel_code = %(null_int_group_rule)d

           where un.dt = '%(statdate)s'
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
               0 fdsfeeducnt,
               0 fdspushucnt,
               0 fdsaducnt,
               fdaucnt,
               fdafeeducnt,
               fdapushucnt,
               fdaaducnt
           from work.user_source_dis32_%(statdatenum)s
            ) src
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
        fchannel_code;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.user_source_dis11_%(statdatenum)s;
        drop table if exists work.user_source_dis12_%(statdatenum)s;
        drop table if exists work.user_source_dis21_%(statdatenum)s;
        drop table if exists work.user_source_dis22_%(statdatenum)s;
        drop table if exists work.user_source_dis31_%(statdatenum)s;
        drop table if exists work.user_source_dis32_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

#生成统计实例
a = agg_user_source_dis(sys.argv[1:])
a()
