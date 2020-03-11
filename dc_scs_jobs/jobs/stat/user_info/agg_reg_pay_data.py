#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reg_pay_data(BaseStat):
    """新增用户,付费，破产数据
    """

    def create_tab(self):
        hql = """create table if not exists analysis.user_new_pay_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fusernum bigint,
                fpaycnt bigint,
                fincome decimal(20,4)
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """create table if not exists analysis.user_new_rupt_fct
                (
                fdate date,
                fplatformfsk bigint,
                fgamefsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fusernum bigint,
                fruptcnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)
        res = self.hq.exe_sql(
            """use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.user_new_pay_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 count(distinct fuid) usernum,
                 paycnt,
                 paynum
            from (select a.fbpid,
                         a.fuid,
                         count(b.fplatform_uid) paycnt,
                         sum(b.fcoins_num * b.frate) paynum
                    from stage.user_dim a
                    join stage.payment_stream_stg b
                      on a.fbpid = b.fbpid
                     and a.fplatform_uid = b.fplatform_uid
                     and b.dt = '%(ld_daybegin)s'
                    where a.dt = '%(ld_daybegin)s'
                   group by a.fbpid, a.fuid) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by b.fgamefsk,
                    b.fplatformfsk,
                    b.fversionfsk,
                    b.fterminalfsk,
                    paycnt,
                    paynum;


        insert overwrite table analysis.user_new_rupt_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 b.fplatformfsk,
                 b.fgamefsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 count(distinct fuid) usernum,
                 ruptcnt
            from (select a.fbpid, a.fuid, count(b.fuid) ruptcnt
                    from stage.user_dim a
                    join stage.user_bankrupt_stg b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                    where a.dt = '%(ld_daybegin)s'
                      and b.dt = '%(ld_daybegin)s'
                   group by a.fbpid, a.fuid) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by b.fplatformfsk,
                    b.fgamefsk,
                    b.fversionfsk,
                    b.fterminalfsk,
                    ruptcnt;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


# 愉快的统计要开始啦
global statDate
if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(
        datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
else:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
# 生成统计实例
a = agg_reg_pay_data(statDate)
a()
