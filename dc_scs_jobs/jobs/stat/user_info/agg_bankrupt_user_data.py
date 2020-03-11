#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_bankrupt_user_data(BaseStat):
    """破产用户，数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_bankrupt_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fgradefsk bigint,
                fbankruptusercnt bigint,
                fbankruptcnt bigint,
                freliefusercnt bigint,
                freliefcnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            use stage;
            drop table if exists stage.user_bankrupt_fct_tmp_%(num_begin)s;
            create table if not exists stage.user_bankrupt_fct_tmp_%(num_begin)s
            (
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fgradefsk bigint,
            fbankruptusercnt bigint,
            fbankruptcnt bigint,
            freliefusercnt bigint,
            freliefcnt bigint
            )
            """ % self.hql_dict
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.user_bankrupt_pay_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fbankusercnt bigint,
                fbankpayusercnt bigint,
                fstatus_pu bigint,
                fstatus_income decimal(20,2),
                fstatus_pcnt bigint
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
        insert into table stage.user_bankrupt_fct_tmp_%(num_begin)s
        select bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk,
            nvl(gd.fsk, 1) fgradefsk,
            count(distinct bs.fuid) fbankruptusercnt,
            sum(bankcnt) fbankruptcnt,
            0 freliefusercnt, 0 freliefcnt
        from (
            select fbpid, fuid, max(fuser_grade) fgarde, count(1) bankcnt
            from stage.user_bankrupt_stg
            where dt = '%(ld_daybegin)s'
            group by fbpid, fuid
        ) bs
        left join analysis.grade_dim gd
           on bs.fgarde = gd.flevel
        join analysis.bpid_platform_game_ver_map bpm
           on bs.fbpid = bpm.fbpid
        group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, nvl(gd.fsk, 1)
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert into table stage.user_bankrupt_fct_tmp_%(num_begin)s
        select bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk,
            nvl(gd.fsk, 1) fgradefsk,
            0 fbankruptusercnt,
            0 fbankruptcnt,
            count(bs.fuid) freliefusercnt,
            sum(cnt) freliefcnt
        from (
            select fbpid, fuid, count(1) cnt, max(fuser_grade) fgarde
            from stage.user_bankrupt_relieve_stg
            where dt = '%(ld_daybegin)s'
            group by fbpid, fuid
        ) bs
         left join analysis.grade_dim gd
               on bs.fgarde = gd.flevel
           join analysis.bpid_platform_game_ver_map bpm
               on bs.fbpid = bpm.fbpid
        group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk, nvl(gd.fsk, 1)
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.user_bankrupt_fct
        partition (dt = '%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fgradefsk,
                sum(fbankruptusercnt) fbankruptusercnt,
                sum(fbankruptcnt) fbankruptcnt,
                sum(freliefusercnt) freliefusercnt,
                sum(freliefcnt) freliefcnt
            from stage.user_bankrupt_fct_tmp_%(num_begin)s
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fgradefsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_bankrupt_pay_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fgamefsk, fplatformfsk, fversionfsk,
             fterminalfsk, sum(fbankusercnt) fbankusercnt,
             sum(fbankpayusercnt) fbankpayusercnt, sum(fstatus_pu) fstatus_pu,
             sum(fstatus_income) fstatus_income, sum(fstatus_pcnt) fstatus_pcnt
        from (select bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk,
                      bpm.fterminalfsk, count(distinct a.fuid) fbankusercnt,
                      count(distinct a.pay_fuid) fbankpayusercnt,
                      0 fstatus_pcnt,
                      0 fstatus_pu,
                      0 fstatus_income
                 from  (select distinct a.fbpid, a.fuid fuid,
                         case when b.fdate <= from_unixtime(unix_timestamp(a.frupt_at)+1800)
                         then b.fuid end pay_fuid
                          from (select fbpid, fuid, frupt_at
                                   from stage.user_bankrupt_stg
                                  where dt = '%(ld_daybegin)s') a
                          left outer join (select b.fbpid, a.fuid, fdate
                                            from stage.payment_stream_stg b
                                            join stage.pay_user_mid a
                                              on a.fbpid = b.fbpid
                                             and a.fplatform_uid = b.fplatform_uid
                                           where b.dt = '%(ld_daybegin)s') b
                            on a.fbpid = b.fbpid
                           and a.fuid = b.fuid
                        ) a
                 join analysis.bpid_platform_game_ver_map bpm
                   on a.fbpid = bpm.fbpid
                group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk,
                         bpm.fterminalfsk
               union all
               select c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk,
                      0 fbankusercnt, 0 fbankpayusercnt,
                      count(a.fuid) fstatus_pcnt,
                      count(distinct a.fuid) fstatus_pu,
                      round(sum(nvl(b.fcoins_num * b.frate, 0)), 2) fstatus_income
                 from stage.user_generate_order_stg a
                 join stage.payment_stream_stg b
                   on a.fbpid = b.fbpid
                  and a.fplatform_uid = b.fplatform_uid
                  and a.forder_id = b.forder_id
                 join analysis.bpid_platform_game_ver_map c
                   on a.fbpid = c.fbpid
                where a.dt = '%(ld_daybegin)s'
                  and a.fbankrupt = 1
                group by c.FGAMEFSK, c.fplatformfsk, c.fversionfsk, c.FTERMINALFSK) tmp
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;

        drop table if exists stage.user_bankrupt_fct_tmp_%(num_begin)s;
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
a = agg_bankrupt_user_data(statDate)
a()
