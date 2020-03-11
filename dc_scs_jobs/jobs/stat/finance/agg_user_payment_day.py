#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_payment_day(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        hql = """
        drop table if exists analysis.user_payment_fct_day_part_%(num_begin)s;
        create table if not exists analysis.user_payment_fct_day_part_%(num_begin)s
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fpayusercnt                 bigint,
            fpaycnt                     bigint,
            fincome                     decimal(38,2),
            f3dpayusercnt               bigint,
            f3dpaycnt                   bigint,
            f3dincome                   decimal(38,2),
            f7dpayusercnt               bigint,
            f7dpaycnt                   bigint,
            f7dincome                   decimal(38,2),
            f30dpayusercnt              bigint,
            f30dpaycnt                  bigint,
            f30dincome                  decimal(38,2),
            fweekpayusercnt             bigint,
            fweekpaycnt                 bigint,
            fweekincome                 decimal(38,2),
            fmonthpayusercnt            bigint,
            fmonthpaycnt                bigint,
            fmonthincome                decimal(38,2),
            fmonthincome_newpu          decimal(20,2),
            fincome_fpu                 decimal(20,2)
        );
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """
        insert into table analysis.user_payment_fct_day_part_%(num_begin)s
        select  '%(stat_date)s' fdate,
             fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
             count(distinct case
                     when pss.dt >= '%(stat_date)s' and pss.dt < '%(ld_end)s' then
                      pss.fplatform_uid
                     else
                      null
                   end) fpayusercnt,
             count(case
                     when pss.dt >= '%(stat_date)s' and pss.dt < '%(ld_end)s' then
                      1
                     else
                      null
                   end) fpaycnt,
             round(sum(case
                   when pss.dt >= '%(stat_date)s' and pss.dt < '%(ld_end)s' then
                    pss.fcoins_num * pss.frate
                   else
                    0
                 end), 2) fincome,
             count(distinct case
                     when pss.dt >= date_add('%(ld_end)s',-3) and
                          pss.dt < '%(ld_end)s' then
                      pss.fplatform_uid
                     else
                      null
                   end) f3dpayusercnt,
             count(case
                     when pss.dt >= date_add('%(ld_end)s',-3) and
                          pss.dt < '%(ld_end)s' then
                      1
                     else
                      null
                   end) f3dpaycnt,
             round(sum(case
                   when pss.dt >= date_add('%(ld_end)s',-3) and
                        pss.dt < '%(ld_end)s' then
                    pss.fcoins_num * pss.frate
                   else
                    0
                 end), 2) f3dincome,
             count(distinct case
                     when pss.dt >= date_add('%(ld_end)s',-7) and
                          pss.dt < '%(ld_end)s' then
                      pss.fplatform_uid
                     else
                      null
                   end) f7dpayusercnt,
             count(case
                     when pss.dt >= date_add('%(ld_end)s',-7) and
                          pss.dt < '%(ld_end)s' then
                      1
                     else
                      null
                   end) f7dpaycnt,
             round(sum(case
                   when pss.dt >= date_add('%(ld_end)s',-7) and
                        pss.dt < '%(ld_end)s' then
                    pss.fcoins_num * pss.frate
                   else
                    0
                 end), 2) f7dincome,
             count(distinct case
                     when pss.dt >= date_add('%(ld_end)s',-30) and
                          pss.dt < '%(ld_end)s' then
                      pss.fplatform_uid
                     else
                      null
                   end) f30dpayusercnt,
             count(case
                     when pss.dt >= date_add('%(ld_end)s',-30) and
                          pss.dt < '%(ld_end)s' then
                      1
                     else
                      null
                   end) f30dpaycnt,
             round(sum(case
                   when pss.dt >= date_add('%(ld_end)s',-30) and
                        pss.dt < '%(ld_end)s' then
                    pss.fcoins_num * pss.frate
                   else
                    0
                 end), 2) f30dincome,
             count(distinct case
                     when pss.dt >= '%(ld_week_begin)s' and
                          pss.dt < '%(ld_end)s' then
                      pss.fplatform_uid
                     else
                      null
                   end) fweekpayusercnt,
             count(case
                     when pss.dt >= '%(ld_week_begin)s' and
                          pss.dt < '%(ld_end)s' then
                      1
                     else
                      null
                   end) fweekpaycnt,
             round(sum(case
                   when pss.dt >= '%(ld_week_begin)s' and
                        pss.dt < '%(ld_end)s' then
                    pss.fcoins_num * pss.frate
                   else
                    0
                 end), 2) fweekincome,
             count(distinct case
                     when pss.dt >= '%(ld_month_begin)s' and
                          pss.dt < '%(ld_end)s' then
                      pss.fplatform_uid
                     else
                      null
                   end) fmonthpayusercnt,
             count(case
                     when pss.dt >= '%(ld_month_begin)s' and
                          pss.dt < '%(ld_end)s' then
                      1
                     else
                      null
                   end) fmonthpaycnt,
             round(sum(case
                   when pss.dt >= '%(ld_month_begin)s' and
                        pss.dt < '%(ld_end)s' then
                    pss.fcoins_num * pss.frate
                   else
                    0
                 end), 2) fmonthincome,
            0 fmonthincome_newpu,
            0 fincome_fpu
        from stage.payment_stream_stg pss
        join analysis.bpid_platform_game_ver_map b
         on pss.fbpid = b.fbpid
       where pss.dt >= date_add('%(ld_end)s',-30)
         and pss.dt < '%(ld_end)s'
       group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        set io.sort.mb=256;
        insert into table analysis.user_payment_fct_day_part_%(num_begin)s
        select '%(stat_date)s' fdate,
                fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                0 fpayusercnt,
                0 fpaycnt,
                0 fincome,
                0 f3dpayusercnt,
                0 f3dpaycnt,
                0 f3dincome,
                0 f7dpayusercnt,
                0 f7dpaycnt,
                0 f7dincome,
                0 f30dpayusercnt,
                0 f30dpaycnt,
                0 f30dincome,
                0 fweekpayusercnt,
                0 fweekpaycnt,
                0 fweekincome,
                0 fmonthpayusercnt,
                0 fmonthpaycnt,
                0 fmonthincome,
                round(sum(fcoins_num*frate),2) fmonthincome_newpu,
                0 fincome_fpu
            from stage.payment_stream_stg npt
            join stage.pay_user_mid b
              on npt.fplatform_uid=b.fplatform_uid
             and npt.fbpid=b.fbpid
             and b.dt >='%(ld_month_begin)s'
             and b.dt < '%(ld_end)s'
            join analysis.bpid_platform_game_ver_map c
              on npt.fbpid = c.fbpid
           where npt.dt >= '%(ld_month_begin)s'
             and npt.dt < '%(ld_end)s'
           group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_payment_fct_day_part_%(num_begin)s
        select '%(stat_date)s' fdate,
                fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                0 fpayusercnt,
                0 fpaycnt,
                0 fincome,
                0 f3dpayusercnt,
                0 f3dpaycnt,
                0 f3dincome,
                0 f7dpayusercnt,
                0 f7dpaycnt,
                0 f7dincome,
                0 f30dpayusercnt,
                0 f30dpaycnt,
                0 f30dincome,
                0 fweekpayusercnt,
                0 fweekpaycnt,
                0 fweekincome,
                0 fmonthpayusercnt,
                0 fmonthpaycnt,
                0 fmonthincome,
                0 fmonthincome_newpu,
                round(sum(npt.ffirst_pay_income)) fincome_fpu
            from stage.pay_user_mid npt
            join analysis.bpid_platform_game_ver_map b
              on npt.fbpid = b.fbpid
           where npt.dt >=date_add('%(ld_end)s',-30)
             and npt.dt < '%(ld_end)s'
           group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_payment_fct_day_part_%(num_begin)s
        select fdate,
            fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(fpayusercnt)            fpayusercnt,
            max(fpaycnt)                fpaycnt,
            max(fincome)                fincome,
            max(f3dpayusercnt)          f3dpayusercnt,
            max(f3dpaycnt)              f3dpaycnt,
            max(f3dincome)              f3dincome,
            max(f7dpayusercnt)          f7dpayusercnt,
            max(f7dpaycnt)              f7dpaycnt,
            max(f7dincome)              f7dincome,
            max(f30dpayusercnt)         f30dpayusercnt,
            max(f30dpaycnt)             f30dpaycnt,
            max(f30dincome)             f30dincome,
            max(fweekpayusercnt)        fweekpayusercnt,
            max(fweekpaycnt)            fweekpaycnt,
            max(fweekincome)            fweekincome,
            max(fmonthpayusercnt)       fmonthpayusercnt,
            max(fmonthpaycnt)           fmonthpaycnt,
            max(fmonthincome)           fmonthincome,
            max(fmonthincome_newpu)     fmonthincome_newpu,
            max(fincome_fpu)            fincome_fpu
        from analysis.user_payment_fct_day_part_%(num_begin)s
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
    a = agg_user_payment_day(stat_date)
    a()
