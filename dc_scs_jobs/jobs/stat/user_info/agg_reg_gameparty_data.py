#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

"""
对应的oracle存储过程是  pkg_user_info.agg_newuser_partynum_data
"""

class agg_reg_gameparty_data(BaseStat):
    """新增用户,牌局数据
    """
    def create_tab(self):
        hql = """
        create table if not exists analysis.user_new_pay_party_fct
        (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fpayusernum bigint,
            fpartynum bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        create table if not exists analysis.user_register_logincnt_fct
        (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fusernum bigint,
            flogincnt bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage;""")
        if res != 0: return res

        hql = """
        set hive.auto.convert.join=false;

        insert overwrite table analysis.user_new_pay_party_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            max(fpayusernum) fpayusernum,
            max(fpartynum) fpartynum
        from
        (
            select c.fplatformfsk, c.fgamefsk, c.fversionfsk, c.fterminalfsk,
                count(distinct a.fuid) fpayusernum, null fpartynum
            from stage.user_pay_info a
            join stage.user_dim b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s' and b.dt = '%(ld_daybegin)s'
            group by c.fplatformfsk, c.fgamefsk, c.fversionfsk, c.fterminalfsk

            union all

            select c.fplatformfsk, c.fgamefsk, c.fversionfsk, c.fterminalfsk,
                null fpayusernum, sum(fparty_num) fpartynum
            from stage.user_gameparty_info_mid b
            join stage.user_dim a
                on a.fbpid = b.fbpid and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s' and b.dt = '%(ld_daybegin)s'
            group by c.fplatformfsk, c.fgamefsk, c.fversionfsk, c.fterminalfsk
        ) tmp
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;


        insert overwrite table analysis.user_register_logincnt_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 count(a.fuid) fusernum,
                 logincnt

            from (select a.fbpid, a.fuid, count(1) logincnt
                    from stage.user_dim a
                    join stage.user_login_stg b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and b.dt = '%(ld_daybegin)s'
                    where a.dt = '%(ld_daybegin)s'
                   group by a.fbpid, a.fuid) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, logincnt;
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
a = agg_reg_gameparty_data(statDate)
a()
