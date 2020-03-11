#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_payment_income_day(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_payment_income_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fmaxpayusercnt              bigint,
            fminpayusercnt              bigint,
            favgpayusercnt              bigint,
            fmaxincome                  decimal(20,2),
            fminincome                  decimal(20,2),
            favgincome                  decimal(20,2)
        )
        partitioned by (dt date);
        create table if not exists analysis.minute_dim
        (
            fsk                     bigint,
            fminute                 int,
            f5minute                int,
            fname                   varchar(20),
            fcate                   varchar(20)
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        hql = """
        insert overwrite table analysis.user_payment_income_fct partition
        (dt = "%(stat_date)s")

        select '%(stat_date)s' fdate,
             bpm.fgamefsk,
             bpm.fplatformfsk,
             bpm.fversionfsk,
             bpm.fterminalfsk,
             max(a.fpayusercnt)              fmaxpayusercnt,
             min(a.fpayusercnt)              fminpayusercnt,
             floor(avg(a.fpayusercnt))       favgpayusercnt,
             max(a.fincome)                  fmaxincome,
             min(a.fincome)                  fminincome,
             round(avg(a.fincome), 2)        favgincome
        from (
            select pss.fbpid,
                     count(distinct pss.fplatform_uid)          fpayusercnt,
                     round(sum(pss.fcoins_num * pss.frate), 2)  fincome
                from stage.payment_stream_stg pss
                join analysis.minute_dim md
                  on minute(pss.fdate) = md.fminute
               where pss.dt = '%(stat_date)s'
               group by pss.fbpid,
                        hour(pss.fdate),
                        md.f5minute
            ) a
        join analysis.bpid_platform_game_ver_map bpm
          on a.fbpid = bpm.fbpid
       group by bpm.fgamefsk,
                bpm.fplatformfsk,
                bpm.fversionfsk,
                bpm.fterminalfsk
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
    a = agg_user_payment_income_day(stat_date)
    a()
