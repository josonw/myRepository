#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class agg_pay_user_income(BaseStat):
    """付费用户的多天统计"""

    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_income
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
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
        )
        partitioned by (dt date)"""
        result = self.exe_hql(hql)
        if result != 0:return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}
        numdate = self.stat_date.replace("-", "")
        query = sql_const.query_list(self.stat_date, alias_dic, None)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        for i in range(2):
            hql = """
            select  '%(ld_daybegin)s' fdate,
                  %(select_field_str)s,
                 count(distinct case
                         when a.dt >= '%(ld_daybegin)s' and a.dt < '%(ld_dayend)s' then
                          a.fuid
                         else
                          null
                       end) fpayusercnt,
                 sum(case
                         when a.dt >= '%(ld_daybegin)s' and a.dt < '%(ld_dayend)s' then
                          a.fpay_cnt
                         else
                          0
                       end) fpaycnt,
                 round(sum(case
                       when a.dt >= '%(ld_daybegin)s' and a.dt < '%(ld_dayend)s' then
                        a.ftotal_usd_amt
                       else
                        0
                     end), 2) fincome,
                 count(distinct case
                         when a.dt >= date_add('%(ld_dayend)s',-3) and
                              a.dt < '%(ld_dayend)s' then
                          a.fuid
                         else
                          null
                       end) f3dpayusercnt,
                 sum(case
                         when a.dt >= date_add('%(ld_dayend)s',-3) and
                              a.dt < '%(ld_dayend)s' then
                          a.fpay_cnt
                         else
                          0
                       end) f3dpaycnt,
                 round(sum(case
                       when a.dt >= date_add('%(ld_dayend)s',-3) and
                            a.dt < '%(ld_dayend)s' then
                        a.ftotal_usd_amt
                       else
                        0
                     end), 2) f3dincome,
                 count(distinct case
                         when a.dt >= date_add('%(ld_dayend)s',-7) and
                              a.dt < '%(ld_dayend)s' then
                          a.fuid
                         else
                          null
                       end) f7dpayusercnt,
                 sum(case
                         when a.dt >= date_add('%(ld_dayend)s',-7) and
                              a.dt < '%(ld_dayend)s' then
                          a.fpay_cnt
                         else
                          0
                       end) f7dpaycnt,
                 round(sum(case
                       when a.dt >= date_add('%(ld_dayend)s',-7) and
                            a.dt < '%(ld_dayend)s' then
                        a.ftotal_usd_amt
                       else
                        0
                     end), 2) f7dincome,
                 count(distinct case
                         when a.dt >= date_add('%(ld_dayend)s',-30) and
                              a.dt < '%(ld_dayend)s' then
                          a.fuid
                         else
                          null
                       end) f30dpayusercnt,
                 sum(case
                         when a.dt >= date_add('%(ld_dayend)s',-30) and
                              a.dt < '%(ld_dayend)s' then
                          a.fpay_cnt
                         else
                          0
                       end) f30dpaycnt,
                 round(sum(case
                       when a.dt >= date_add('%(ld_dayend)s',-30) and
                            a.dt < '%(ld_dayend)s' then
                        a.ftotal_usd_amt
                       else
                        0
                     end), 2) f30dincome,
                 count(distinct case
                         when a.dt >= '%(ld_weekbegin)s' and
                              a.dt < '%(ld_dayend)s' then
                          a.fuid
                         else
                          null
                       end) fweekpayusercnt,
                 sum(case
                         when a.dt >= '%(ld_weekbegin)s' and
                              a.dt < '%(ld_dayend)s' then
                          a.fpay_cnt
                         else
                          0
                       end) fweekpaycnt,
                 round(sum(case
                       when a.dt >= '%(ld_weekbegin)s' and
                            a.dt < '%(ld_dayend)s' then
                        a.ftotal_usd_amt
                       else
                        0
                     end), 2) fweekincome,
                 count(distinct case
                         when a.dt >= '%(ld_monthbegin)s' and
                              a.dt < '%(ld_dayend)s' then
                          a.fuid
                         else
                          null
                       end) fmonthpayusercnt,
                 sum(case
                         when a.dt >= '%(ld_monthbegin)s' and
                              a.dt < '%(ld_dayend)s' then
                          a.fpay_cnt
                         else
                          0
                       end) fmonthpaycnt,
                 round(sum(case
                       when a.dt >= '%(ld_monthbegin)s' and
                            a.dt < '%(ld_dayend)s' then
                        a.ftotal_usd_amt
                       else
                        0
                     end), 2) fmonthincome,
                0 fmonthincome_newpu,
                0 fincome_fpu
            from dim.user_pay_day a
            join dim.bpid_map b
              on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
           where a.dt >= add_months(trunc('%(ld_dayend)s','MM'),-1)
             and a.dt < '%(ld_dayend)s'
           %(group_by)s
            """ % query[i]
            hql_list.append(hql)

        hql = """
        drop table if exists work.pay_user_income_temp_%s;
        create table work.pay_user_income_temp_%s as
        %s;
        insert into table work.pay_user_income_temp_%s
        %s
              """%(numdate, numdate, hql_list[0], numdate, hql_list[1])

        res = self.hq.exe_sql(hql)
        if res != 0:return res


        for i in range(2):
            hql = """
            set io.sort.mb=256;
            insert into table work.pay_user_income_temp_%(num_begin)s
            select '%(ld_daybegin)s' fdate,
                    %(select_field_str)s,
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
                    round(sum(ftotal_usd_amt),2) fmonthincome_newpu,
                    0 fincome_fpu
                from dim.user_pay_day a
                join dim.user_pay c
                  on a.fuid=c.fuid
                 and a.fbpid=c.fbpid
                 and c.dt >='%(ld_monthbegin)s'
                 and c.dt < '%(ld_dayend)s'
                join dim.bpid_map b
                  on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
               where a.dt >= '%(ld_monthbegin)s'
                 and a.dt < '%(ld_dayend)s'
               %(group_by)s
            """ % query[i]
            res = self.hq.exe_sql(hql)
            if res != 0:return res

        for i in range(2):
            hql = """
            insert into table work.pay_user_income_temp_%(num_begin)s
            select '%(ld_daybegin)s' fdate,
                    %(select_field_str)s,
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
                    round(sum(a.ffirst_pay_income)) fincome_fpu
                from dim.user_pay a
                join dim.bpid_map b
                  on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
               where a.dt >= add_months('%(ld_dayend)s',-1)
                 and a.dt < '%(ld_dayend)s'
               %(group_by)s
            """ % query[i]
            res = self.hq.exe_sql(hql)
            if res != 0:return res



        hql = """
        insert overwrite table dcnew.pay_user_income
        partition( dt="%(ld_daybegin)s" )
        select  fdate,
            fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
            sum(fpayusercnt)            fpayusercnt,
            sum(fpaycnt)                fpaycnt,
            sum(fincome)                fincome,
            sum(f3dpayusercnt)          f3dpayusercnt,
            sum(f3dpaycnt)              f3dpaycnt,
            sum(f3dincome)              f3dincome,
            sum(f7dpayusercnt)          f7dpayusercnt,
            sum(f7dpaycnt)              f7dpaycnt,
            sum(f7dincome)              f7dincome,
            sum(f30dpayusercnt)         f30dpayusercnt,
            sum(f30dpaycnt)             f30dpaycnt,
            sum(f30dincome)             f30dincome,
            sum(fweekpayusercnt)        fweekpayusercnt,
            sum(fweekpaycnt)            fweekpaycnt,
            sum(fweekincome)            fweekincome,
            sum(fmonthpayusercnt)       fmonthpayusercnt,
            sum(fmonthpaycnt)           fmonthpaycnt,
            sum(fmonthincome)           fmonthincome,
            sum(fmonthincome_newpu)     fmonthincome_newpu,
            sum(fincome_fpu)            fincome_fpu
        from work.pay_user_income_temp_%(num_begin)s
        group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """ % query[0]

        res = self.hq.exe_sql(hql)
        if res != 0:return res

        res = self.hq.exe_sql("""drop table if exists work.pay_user_income_temp_%(num_begin)s;"""% query[0])
        if res != 0:return res

        return res


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
    a = agg_pay_user_income(stat_date)
    a()
