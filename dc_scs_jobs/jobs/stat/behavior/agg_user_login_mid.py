#-*- coding: UTF-8 -*-
# Author： AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_login_mid(BaseStat):
    """
    每日登录中间表
    ffirst_date          首次登录日期,
    fgamecoins_num       首次登录携带金币,
    flogin_num           登录次数,
    fip_num              登录不同IP数,
    fimei_num            登录不同imei数,
    fcountry_num         登录不同国家数,
    fprovince_num        登录不同省份数,
    fcity_num            登录不同城市数,
    fm_dtype             登录设备,
    fm_pixel             登录设备分辨率,
    fm_imei              登录设备IMEI号,
    fip_country          最后一次登录IP国家,
    fip_province         最后一次登录IP省份,
    fip_city             最后一次登录IP城市
    """
    def create_tab(self):
        hql = """
        create table if not exists stage.user_login_mid
        (
            fdate                date,
            fbpid                varchar(50),
            fuid                 bigint,
            ffirst_date          date,
            fgamecoins_num       bigint,
            flogin_num           bigint,
            fip_num              int,
            fimei_num            int,
            fcountry_num         int,
            fprovince_num        int,
            fcity_num            int,
            fm_dtype             varchar(50),
            fm_pixel             varchar(50),
            fm_imei              varchar(50),
            fip_country          varchar(50),
            fip_province         varchar(50),
            fip_city             varchar(50)
        )
        partitioned by (dt date) """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
            drop table if exists stage.user_login_mid_tmp_01_%(num_begin)s;
            create table stage.user_login_mid_tmp_01_%(num_begin)s as
            select fbpid,
                   fuid,
                   to_date(ffirst_at) ffirst_date,
                   null fgamecoins_num,
                   count(distinct flogin_at) flogin_num,
                   count(distinct fip) fip_num,
                   count(distinct fm_dtype) fimei_num,
                   count(distinct fip_country) fcountry_num,
                   count(distinct fip_province) fprovince_num,
                   count(distinct fip_city) fcity_num,
                   null fm_dtype,
                   null fm_pixel,
                   null fm_imei,
                   null fip_country,
                   null fip_province,
                   null fip_city
            from stage.user_login_stg
            where dt = '%(ld_daybegin)s'
            group by fbpid, fuid, ffirst_at
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            drop table if exists stage.user_login_mid_tmp_02_%(num_begin)s;
            create table stage.user_login_mid_tmp_02_%(num_begin)s as
            select fbpid,
                   fuid,
                   ffirst_date,
                   fgamecoins_num,
                   null flogin_num,
                   null fip_num,
                   null fimei_num,
                   null fcountry_num,
                   null fprovince_num,
                   null fcity_num,
                   null fm_dtype,
                   null fm_pixel,
                   null fm_imei,
                   null fip_country,
                   null fip_province,
                   null fip_city
            from(
                    select fbpid,
                           fuid,
                           to_date(ffirst_at) ffirst_date,
                           user_gamecoins fgamecoins_num,
                           row_number() over(partition by fbpid, fuid, ffirst_at order by flogin_at asc) nrow
                      from stage.user_login_stg
                     where dt = '%(ld_daybegin)s'
                       and flogin_at > '1970-01-01'
                )t
            where nrow = 1
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            drop table if exists stage.user_login_mid_tmp_03_%(num_begin)s;
            create table stage.user_login_mid_tmp_03_%(num_begin)s as
            select fbpid,
                   fuid,
                   ffirst_date,
                   null fgamecoins_num,
                   null flogin_num,
                   null fip_num,
                   null fimei_num,
                   null fcountry_num,
                   null fprovince_num,
                   null fcity_num,
                   fm_dtype,
                   fm_pixel,
                   fm_imei,
                   fip_country,
                   fip_province,
                   fip_city
            from(
                    select fbpid,
                           fuid,
                           to_date(ffirst_at) ffirst_date,
                           fm_dtype,
                           fm_pixel,
                           fm_imei,
                           fip_country,
                           fip_province,
                           fip_city,
                           row_number() over(partition by fbpid, fuid, ffirst_at order by flogin_at desc) nrow
                      from stage.user_login_stg
                     where dt = '%(ld_daybegin)s'
                )t
            where nrow = 1
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 日登录中间表
        hql ="""
            insert overwrite table stage.user_login_mid
            partition (dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                    fbpid,
                    fuid,
                    ffirst_date,
                    min(fgamecoins_num) fgamecoins_num,
                    max(flogin_num)     flogin_num,
                    max(fip_num)        fip_num,
                    max(fimei_num)      fimei_num,
                    max(fcountry_num)   fcountry_num,
                    max(fprovince_num)  fprovince_num,
                    max(fcity_num)      fcity_num,
                    max(fm_dtype)       fm_dtype,
                    max(fm_pixel)       fm_pixel,
                    max(fm_imei)        fm_imei,
                    max(fip_country)    fip_country,
                    max(fip_province)   fip_province,
                    max(fip_city)       fip_city
             from
             (
                select *
                  from stage.user_login_mid_tmp_01_%(num_begin)s

                 union all

                 select *
                   from stage.user_login_mid_tmp_02_%(num_begin)s

                 union all

                 select *
                   from stage.user_login_mid_tmp_03_%(num_begin)s
             ) t
            group by fbpid, fuid, ffirst_date
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 干掉临时工
        hql = """
            drop table if exists stage.user_login_mid_tmp_01_%(num_begin)s;
            drop table if exists stage.user_login_mid_tmp_02_%(num_begin)s;
            drop table if exists stage.user_login_mid_tmp_03_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statdate = get_stat_date()

    #生成统计实例
    a = agg_user_login_mid(statdate)
    a()
