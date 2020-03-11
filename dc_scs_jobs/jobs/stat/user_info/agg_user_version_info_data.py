#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_version_info_data(BaseStat):
    """建立版本信息数据
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_game_version_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fversion_info varchar(50),
                factive bigint,
                fregister bigint,
                freg_payuser bigint,
                fincome decimal(20,2),
                fpayuser bigint,
                ffpayuser bigint,
                fplayuser bigint,
                fbankruptun bigint,
                fbankruptcnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        hql = """
        drop table if exists user_game_version_tmp_%(num_begin)s;
        create table if not exists user_game_version_tmp_%(num_begin)s
        as
        select fbpid, fuid, max(fversion_info) fversion_info
        from (
            select fbpid, fuid, fversion_info
              from stage.user_login_stg
             where dt= '%(ld_daybegin)s'
            union all
            select fbpid, fuid, fversion_info
              from stage.pb_gamecoins_stream_stg
             where dt= '%(ld_daybegin)s'
            union all
            select fbpid, fuid, fversion_info
              from stage.user_gameparty_stg
             where dt= '%(ld_daybegin)s'
            union all
            select fbpid, fuid, fversion_info
              from stage.user_dim
             where dt= '%(ld_daybegin)s'
        ) tmp
        group by fbpid, fuid;

        insert overwrite table analysis.user_game_version_fct
        partition (dt='%(ld_daybegin)s')
        select  '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                COALESCE(fversion_info, '0') fversion_info,
                max(factive) factive,
                max(fregister) fregister,
                max(freg_payuser) freg_payuser,
                max(fincome) fincome,
                max(fpayuser) fpayuser,
                max(ffpayuser) ffpayuser,
                max(fplayuser) fplayuser,
                max(fbankruptun) fbankruptun,
                max(fbankruptcnt) fbankruptcnt
        from (
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
               fversion_info fversion_info,
               count(1) factive,
               0 fregister,
               0 freg_payuser,
               0 fincome,
               0 fpayuser,
               0 ffpayuser,
               0 fplayuser,
               0 fbankruptun,
               0 fbankruptcnt
          from stage.active_user_mid a
          left outer join user_game_version_tmp_%(num_begin)s b
            on a.fbpid = b.fbpid
           and a.fuid = b.fuid
          join analysis.bpid_platform_game_ver_map c
            on a.fbpid = c.fbpid
         where a.dt = '%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fversion_info
        union all
        select  b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, fversion_info fversion_info,
                0 factive,
                count(distinct a.fuid) fregister,
                count(distinct p.fplatform_uid) freg_payuser,
                0 fincome,
                0 fpayuser,
                0 ffpayuser,
                0 fplayuser,
                0 fbankruptun,
                0 fbankruptcnt
           from stage.user_dim a
           left outer join stage.payment_stream_stg p
             on a.fbpid = p.fbpid
            and a.fplatform_uid = p.fplatform_uid
            and p.dt = '%(ld_daybegin)s'
           join analysis.bpid_platform_game_ver_map b
             on a.fbpid = b.fbpid
          where a.dt = '%(ld_daybegin)s'
          group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, fversion_info
        union all
        select  fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                fversion_info fversion_info,
                0 factive,
                0 fregister,
                0 freg_payuser,
                sum(round(fcoins_num * frate, 2)) fincome,
                count(distinct a.fplatform_uid) fpayuser,
                count(distinct if(p.dt='%(ld_daybegin)s', a.fplatform_uid, null)) ffpayuser,
                0 fplayuser,
                0 fbankruptun,
                0 fbankruptcnt
           from stage.payment_stream_stg a
           left outer join stage.pay_user_mid p
             on a.fbpid = p.fbpid
            and a.fplatform_uid = p.fplatform_uid
           left outer join user_game_version_tmp_%(num_begin)s b
             on p.fbpid = b.fbpid
            and p.fuid = b.fuid
           join analysis.bpid_platform_game_ver_map c
             on a.fbpid = c.fbpid
          where a.dt = '%(ld_daybegin)s'
          group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fversion_info
        union all
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fversion_info fversion_info,
            0 factive,
            0 fregister,
            0 freg_payuser,
            0 fincome,
            0 fpayuser,
            0 ffpayuser,
            count(distinct a.fuid) fplayuser,
            0 fbankruptun,
            0 fbankruptcnt
        from stage.user_gameparty_info_mid a
        left outer join user_game_version_tmp_%(num_begin)s b
         on a.fbpid = b.fbpid
        and a.fuid = b.fuid
        join analysis.bpid_platform_game_ver_map c
         on a.fbpid = c.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fversion_info
        union all
        select  fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                b.fversion_info fversion_info,
                0 factive,
                0 fregister,
                0 freg_payuser,
                0 fincome,
                0 fpayuser,
                0 ffpayuser,
                0 fplayuser,
                count(distinct a.fuid) fbankruptun,
                count(1) fbankruptcnt
           from stage.user_bankrupt_stg a
           left outer join user_game_version_tmp_%(num_begin)s b
             on a.fbpid = b.fbpid
            and a.fuid = b.fuid
           join analysis.bpid_platform_game_ver_map c
             on a.fbpid = c.fbpid
          where a.dt = '%(ld_daybegin)s'
          group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, b.fversion_info
        ) tmp group by  fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk,
                        COALESCE(fversion_info, '0');
        drop table if exists user_game_version_tmp_%(num_begin)s;
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
a = agg_user_version_info_data(statDate)
a()
