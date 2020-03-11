#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_payment_new(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        hql = """
        drop table if exists analysis.user_payment_fct_new_pay_part_%(num_begin)s;
        create table if not exists analysis.user_payment_fct_new_pay_part_%(num_begin)s
            (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            f1daynewpaycnt              bigint,
            f1daynewpayincome           decimal(38,2),
            f2daynewpaycnt              bigint,
            f3daynewpaycnt              bigint,
            f4daynewpaycnt              bigint,
            f5daynewpaycnt              bigint,
            f6daynewpaycnt              bigint,
            f7daynewpaycnt              bigint,
            f7daynewpayincome           decimal(38,2),
            f14daynewpaycnt             bigint,
            f30daynewpaycnt             bigint,
            f30daynewpayincome          decimal(38,2),
            fregpayusernum              bigint
            );
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
          insert into table analysis.user_payment_fct_new_pay_part_%(num_begin)s
        -- 首付用户数和首付金额
        select '%(stat_date)s' fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            count(case when npt.dt >= date_add('%(ld_end)s',-1) and npt.dt < '%(ld_end)s' then 1 end) f1daynewpaycnt,
            sum(case when npt.dt   >= date_add('%(ld_end)s',-1) and npt.dt < '%(ld_end)s' then nvl(ffirst_pay_income,0) end) f1daynewpayincome,
            count(case when npt.dt >= date_add('%(ld_end)s',-2) and npt.dt < '%(ld_end)s' then 1 end) f2daynewpaycnt,
            count(case when npt.dt >= date_add('%(ld_end)s',-3) and npt.dt < '%(ld_end)s' then 1 end) f3daynewpaycnt,
            count(case when npt.dt >= date_add('%(ld_end)s',-4) and npt.dt < '%(ld_end)s' then 1 end) f4daynewpaycnt,
            count(case when npt.dt >= date_add('%(ld_end)s',-5) and npt.dt < '%(ld_end)s' then 1 end) f5daynewpaycnt,
            count(case when npt.dt >= date_add('%(ld_end)s',-6) and npt.dt < '%(ld_end)s' then 1 end) f6daynewpaycnt,
            count(case when npt.dt >= date_add('%(ld_end)s',-7) and npt.dt < '%(ld_end)s' then 1 end) f7daynewpaycnt,
            sum(case when npt.dt   >= date_add('%(ld_end)s',-7) and npt.dt < '%(ld_end)s' then nvl(ffirst_pay_income,0) end) f7daynewpayincome,
            count(case when npt.dt >= date_add('%(ld_end)s',-14) and npt.dt < '%(ld_end)s' then 1 end) f14daynewpaycnt,
            count(case when npt.dt >= date_add('%(ld_end)s',-30) and npt.dt < '%(ld_end)s' then 1 end) f30daynewpaycnt,
            sum(case when npt.dt   >= date_add('%(ld_end)s',-30) and npt.dt < '%(ld_end)s' then nvl(ffirst_pay_income,0) end) f30daynewpayincome,
            0 fregpayusernum
        from stage.pay_user_mid npt
        join analysis.bpid_platform_game_ver_map b
          on npt.fbpid = b.fbpid
        where npt.dt >= date_add('%(ld_end)s',-30)
        AND npt.dt < '%(ld_end)s'
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_payment_fct_new_pay_part_%(num_begin)s
        select  '%(stat_date)s' fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            0 f1daynewpaycnt,
            0 f1daynewpayincome,
            0 f2daynewpaycnt,
            0 f3daynewpaycnt,
            0 f4daynewpaycnt,
            0 f5daynewpaycnt,
            0 f6daynewpaycnt,
            0 f7daynewpaycnt,
            0 f7daynewpayincome,
            0 f14daynewpaycnt,
            0 f30daynewpaycnt,
            0 f30daynewpayincome,
            count(distinct a.fuid) fregpayusernum
        from stage.user_dim a
        join stage.user_pay_info b
        on a.fbpid = b.fbpid
        and a.fuid = b.fuid
        and b.dt >= '%(stat_date)s' and  b.dt < '%(ld_end)s'
        join analysis.bpid_platform_game_ver_map c
          on a.fbpid = c.fbpid
        where a.dt >= '%(stat_date)s' and  a.dt < '%(ld_end)s'
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_payment_fct_new_pay_part_%(num_begin)s
        select
            fdate,
            fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(f1daynewpaycnt)         f1daynewpaycnt,
            max(f1daynewpayincome)      f1daynewpayincome,
            max(f2daynewpaycnt)         f2daynewpaycnt,
            max(f3daynewpaycnt)         f3daynewpaycnt,
            max(f4daynewpaycnt)         f4daynewpaycnt,
            max(f5daynewpaycnt)         f5daynewpaycnt,
            max(f6daynewpaycnt)         f6daynewpaycnt,
            max(f7daynewpaycnt)         f7daynewpaycnt,
            max(f7daynewpayincome)      f7daynewpayincome,
            max(f14daynewpaycnt)        f14daynewpaycnt,
            max(f30daynewpaycnt)        f30daynewpaycnt,
            max(f30daynewpayincome)     f30daynewpayincome,
            max(fregpayusernum)         fregpayusernum
        from analysis.user_payment_fct_new_pay_part_%(num_begin)s
        group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_user_payment_new(stat_date)
    a()
