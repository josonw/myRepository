#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_device_type_data(BaseStat):
    """广告用户数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_device_type
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fm_dtype varchar(100),
                fregcnt bigint,
                factcnt bigint,
                fpaycnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.user_device_pixel
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fm_pixel varchar(100),
                fregcnt bigint,
                factcnt bigint,
                fpaycnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.user_device_os
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fm_os varchar(100),
                fregcnt bigint,
                factcnt bigint,
                fpaycnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.user_device_network
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fm_network varchar(100),
                fregcnt bigint,
                factcnt bigint,
                fpaycnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.user_device_operator
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fm_operator varchar(100),
                fregcnt bigint,
                factcnt bigint,
                fpaycnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update(date)
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        --用户设备类型
        insert overwrite table analysis.user_device_type
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fm_dtype,
                max(fregcnt) fregcnt,
                max(factcnt) factcnt,
                max(fpaycnt) fpaycnt
        from (
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
             nvl(fm_dtype, '其他') fm_dtype,
             count(distinct fuid) fregcnt,
             0 factcnt, 0 fpaycnt
        from stage.user_dim a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, nvl(fm_dtype, '其他')
          union all
          select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
               nvl(fm_dtype, '其他') fm_dtype,
               0 fregcnt,
               count(distinct a.fuid) factcnt,
             count(distinct b.fuid) fpaycnt
             from stage.user_login_stg a
             left outer join stage.user_pay_info b
             on a.fbpid =b.fbpid
             and a.fuid=b.fuid
             and b.dt = '%(ld_daybegin)s'
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
            group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, nvl(fm_dtype, '其他')
        ) tmp group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fm_dtype;


        --设备分辨率fm_pixel
        insert overwrite table analysis.user_device_pixel
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fm_pixel,
                max(fregcnt) fregcnt,
                max(factcnt) factcnt,
                max(fpaycnt) fpaycnt
        from (
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
             nvl(fm_pixel, '其他') fm_pixel,
             count(distinct fuid) fregcnt,
             0 factcnt, 0 fpaycnt
        from stage.user_dim a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, nvl(fm_pixel, '其他')
          union all
          select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
               nvl(fm_pixel, '其他') fm_pixel,
               0 fregcnt,
               count(distinct a.fuid) factcnt,
             count(distinct b.fuid) fpaycnt
             from stage.user_login_stg a
             left outer join stage.user_pay_info b
             on a.fbpid =b.fbpid
             and a.fuid=b.fuid
             and b.dt = '%(ld_daybegin)s'
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
            group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, nvl(fm_pixel, '其他')
        ) tmp group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fm_pixel;



        --设备操作系统 m_os
        insert overwrite table analysis.user_device_os
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fm_os,
                max(fregcnt) fregcnt,
                max(factcnt) factcnt,
                max(fpaycnt) fpaycnt
        from (
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
             nvl(fm_os, '其他') fm_os,
             count(distinct fuid) fregcnt,
             0 factcnt, 0 fpaycnt
        from stage.user_dim a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, nvl(fm_os, '其他')
          union all
          select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
               nvl(fm_os, '其他') fm_os,
               0 fregcnt,
               count(distinct a.fuid) factcnt,
             count(distinct b.fuid) fpaycnt
             from stage.user_login_stg a
             left outer join stage.user_pay_info b
             on a.fbpid =b.fbpid
             and a.fuid=b.fuid
             and b.dt = '%(ld_daybegin)s'
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
            group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, nvl(fm_os, '其他')
        ) tmp group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fm_os;


        --设备接入方式m_network
        insert overwrite table analysis.user_device_network
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fm_network,
                max(fregcnt) fregcnt,
                max(factcnt) factcnt,
                max(fpaycnt) fpaycnt
        from (
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
             nvl(fm_network, '其他') fm_network,
             count(distinct fuid) fregcnt,
             0 factcnt, 0 fpaycnt
        from stage.user_dim a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, nvl(fm_network, '其他')
          union all
          select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
               nvl(fm_network, '其他') fm_network,
               0 fregcnt,
               count(distinct a.fuid) factcnt,
             count(distinct b.fuid) fpaycnt
             from stage.user_login_stg a
             left outer join stage.user_pay_info b
             on a.fbpid =b.fbpid
             and a.fuid=b.fuid
             and b.dt = '%(ld_daybegin)s'
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
            group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, nvl(fm_network, '其他')
        ) tmp group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fm_network;


        --设备运营商m_operator
        insert overwrite table analysis.user_device_operator
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fm_operator,
                max(fregcnt) fregcnt,
                max(factcnt) factcnt,
                max(fpaycnt) fpaycnt
        from (
        select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
             nvl(fm_operator, '其他') fm_operator,
             count(distinct fuid) fregcnt,
             0 factcnt, 0 fpaycnt
        from stage.user_dim a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, nvl(fm_operator, '其他')
          union all
          select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
               nvl(fm_operator, '其他') fm_operator,
               0 fregcnt,
               count(distinct a.fuid) factcnt,
             count(distinct b.fuid) fpaycnt
             from stage.user_login_stg a
             left outer join stage.user_pay_info b
             on a.fbpid =b.fbpid
             and a.fuid=b.fuid
             and b.dt = '%(ld_daybegin)s'
             join analysis.bpid_platform_game_ver_map c
               on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
            group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk, nvl(fm_operator, '其他')
        ) tmp group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fm_operator;

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_user_device_type_data(statDate)
a()
