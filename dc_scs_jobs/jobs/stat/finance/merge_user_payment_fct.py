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
依赖的任务
agg_lastmonth_pu_retained.py
    payment_stream_stg  上个月1号开始要数据完整
    pay_user_mid        全量
    active_user_mid     上个月1号开始要数据完整

agg_user_generate_order.py
    analysis.paycenter_apps_dim
    stage.user_payscene_mid
        agg_user_payscene_proc
            analysis.paycenter_chanel_dim

agg_user_payment_continued_day.py
    payment_stream_stg   全部流水

agg_user_payment_day.py
agg_user_payment_new.py
agg_user_payment_qy_data.py

"""

class merge_user_payment_fct(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_payment_fct
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
            f1daynewpaycnt              bigint,
            f1daynewpayincome           decimal(20,2),
            f2daynewpaycnt              bigint,
            f3daynewpaycnt              bigint,
            f4daynewpaycnt              bigint,
            f5daynewpaycnt              bigint,
            f6daynewpaycnt              bigint,
            f7daynewpaycnt              bigint,
            f7daynewpayincome           decimal(20,2),
            f14daynewpaycnt             bigint,
            f30daynewpaycnt             bigint,
            f30daynewpayincome          decimal(20,2),
            favgconpayday               bigint,
            fmaxconpayday               bigint,
            f1daymaxpaycnt              bigint,
            f1dayminpaycnt              bigint,
            f3daymaxpaycnt              bigint,
            f3dayminpaycnt              bigint,
            f7daymaxpaycnt              bigint,
            f7dayminpaycnt              bigint,
            f30daymaxpaycnt             bigint,
            f30dayminpaycnt             bigint,
            flastmonthppr               bigint,
            flastmonthpar               bigint,
            fregpayusernum              bigint,
            fquarterpu                  bigint,
            fhyearpu                    bigint,
            fmonthincome_newpu          decimal(20,2),
            fincome_fpu                 decimal(20,2),
            fordercnt                   bigint,
            forderusercnt               bigint,
            fbankruptincome             decimal(38,2),
            fbankruptusercnt            bigint
        )
        partitioned by (dt date)
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        self.hql_dict['num_30dayago'] = self.hql_dict.get('ld_30dayago').replace('-', '')

        hql = """
        drop table if exists analysis.user_payment_fct_final_tmp_%(num_begin)s;

        create table analysis.user_payment_fct_final_tmp_%(num_begin)s as
        select * from (
            select
            fdate,
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
            0 favgconpayday,
            0 fmaxconpayday,
            0 f1daymaxpaycnt,
            0 f1dayminpaycnt,
            0 f3daymaxpaycnt,
            0 f3dayminpaycnt,
            0 f7daymaxpaycnt,
            0 f7dayminpaycnt,
            0 f30daymaxpaycnt,
            0 f30dayminpaycnt,
            flastmonthppr,
            flastmonthpar,
            0 fregpayusernum,
            0 fquarterpu,
            0 fhyearpu,
            0 fmonthincome_newpu,
            0 fincome_fpu,
            0 fordercnt,
            0 forderusercnt,
            0 fbankruptincome,
            0 fbankruptusercnt
        from analysis.user_payment_fct_lastmonth_part_%(num_begin)s

        union all

        select
            fdate,
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
            0 favgconpayday,
            0 fmaxconpayday,
            0 f1daymaxpaycnt,
            0 f1dayminpaycnt,
            0 f3daymaxpaycnt,
            0 f3dayminpaycnt,
            0 f7daymaxpaycnt,
            0 f7dayminpaycnt,
            0 f30daymaxpaycnt,
            0 f30dayminpaycnt,
            0 flastmonthppr,
            0 flastmonthpar,
            0 fregpayusernum,
            0 fquarterpu,
            0 fhyearpu,
            0 fmonthincome_newpu,
            0 fincome_fpu,
            fordercnt,
            forderusercnt,
            fbankruptincome,
            fbankruptusercnt
        from analysis.user_payment_fct_order_bankrupt_part_%(num_begin)s

        union all

        select
            fdate,
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
            favgconpayday,
            fmaxconpayday,
            f1daymaxpaycnt,
            f1dayminpaycnt,
            f3daymaxpaycnt,
            f3dayminpaycnt,
            f7daymaxpaycnt,
            f7dayminpaycnt,
            f30daymaxpaycnt,
            f30dayminpaycnt,
            0 flastmonthppr,
            0 flastmonthpar,
            0 fregpayusernum,
            0 fquarterpu,
            0 fhyearpu,
            0 fmonthincome_newpu,
            0 fincome_fpu,
            0 fordercnt,
            0 forderusercnt,
            0 fbankruptincome,
            0 fbankruptusercnt
        from analysis.user_payment_fct_continue_part_%(num_begin)s

        union all

        select
            fdate,
            fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            fpayusercnt,
            fpaycnt,
            fincome,
            f3dpayusercnt,
            f3dpaycnt,
            f3dincome,
            f7dpayusercnt,
            f7dpaycnt,
            f7dincome,
            f30dpayusercnt,
            f30dpaycnt,
            f30dincome,
            fweekpayusercnt,
            fweekpaycnt,
            fweekincome,
            fmonthpayusercnt,
            fmonthpaycnt,
            fmonthincome,
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
            0 favgconpayday,
            0 fmaxconpayday,
            0 f1daymaxpaycnt,
            0 f1dayminpaycnt,
            0 f3daymaxpaycnt,
            0 f3dayminpaycnt,
            0 f7daymaxpaycnt,
            0 f7dayminpaycnt,
            0 f30daymaxpaycnt,
            0 f30dayminpaycnt,
            0 flastmonthppr,
            0 flastmonthpar,
            0 fregpayusernum,
            0 fquarterpu,
            0 fhyearpu,
            fmonthincome_newpu,
            fincome_fpu,
            0 fordercnt,
            0 forderusercnt,
            0 fbankruptincome,
            0 fbankruptusercnt
        from analysis.user_payment_fct_day_part_%(num_begin)s

        union all

        select
            fdate,
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
            f1daynewpaycnt,
            f1daynewpayincome,
            f2daynewpaycnt,
            f3daynewpaycnt,
            f4daynewpaycnt,
            f5daynewpaycnt,
            f6daynewpaycnt,
            f7daynewpaycnt,
            f7daynewpayincome,
            f14daynewpaycnt,
            f30daynewpaycnt,
            f30daynewpayincome,
            0 favgconpayday,
            0 fmaxconpayday,
            0 f1daymaxpaycnt,
            0 f1dayminpaycnt,
            0 f3daymaxpaycnt,
            0 f3dayminpaycnt,
            0 f7daymaxpaycnt,
            0 f7dayminpaycnt,
            0 f30daymaxpaycnt,
            0 f30dayminpaycnt,
            0 flastmonthppr,
            0 flastmonthpar,
            fregpayusernum,
            0 fquarterpu,
            0 fhyearpu,
            0 fmonthincome_newpu,
            0 fincome_fpu,
            0 fordercnt,
            0 forderusercnt,
            0 fbankruptincome,
            0 fbankruptusercnt
        from analysis.user_payment_fct_new_pay_part_%(num_begin)s

        union all

        select
            fdate,
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
            0 favgconpayday,
            0 fmaxconpayday,
            0 f1daymaxpaycnt,
            0 f1dayminpaycnt,
            0 f3daymaxpaycnt,
            0 f3dayminpaycnt,
            0 f7daymaxpaycnt,
            0 f7dayminpaycnt,
            0 f30daymaxpaycnt,
            0 f30dayminpaycnt,
            0 flastmonthppr,
            0 flastmonthpar,
            0 fregpayusernum,
            fquarterpu,
            fhyearpu,
            0 fmonthincome_newpu,
            0 fincome_fpu,
            0 fordercnt,
            0 forderusercnt,
            0 fbankruptincome,
            0 fbankruptusercnt
        from analysis.user_payment_fct_year_season_part_%(num_begin)s
    ) a
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_payment_fct partition
        (dt = "%(stat_date)s")
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(fpayusercnt)                fpayusercnt,
            max(fpaycnt)                    fpaycnt,
            max(fincome)                    fincome,
            max(f3dpayusercnt)              f3dpayusercnt,
            max(f3dpaycnt)                  f3dpaycnt,
            max(f3dincome)                  f3dincome,
            max(f7dpayusercnt)              f7dpayusercnt,
            max(f7dpaycnt)                  f7dpaycnt,
            max(f7dincome)                  f7dincome,
            max(f30dpayusercnt)             f30dpayusercnt,
            max(f30dpaycnt)                 f30dpaycnt,
            max(f30dincome)                 f30dincome,
            max(fweekpayusercnt)            fweekpayusercnt,
            max(fweekpaycnt)                fweekpaycnt,
            max(fweekincome)                fweekincome,
            max(fmonthpayusercnt)           fmonthpayusercnt,
            max(fmonthpaycnt)               fmonthpaycnt,
            max(fmonthincome)               fmonthincome,
            max(f1daynewpaycnt)             f1daynewpaycnt,
            max(f1daynewpayincome)          f1daynewpayincome,
            max(f2daynewpaycnt)             f2daynewpaycnt,
            max(f3daynewpaycnt)             f3daynewpaycnt,
            max(f4daynewpaycnt)             f4daynewpaycnt,
            max(f5daynewpaycnt)             f5daynewpaycnt,
            max(f6daynewpaycnt)             f6daynewpaycnt,
            max(f7daynewpaycnt)             f7daynewpaycnt,
            max(f7daynewpayincome)          f7daynewpayincome,
            max(f14daynewpaycnt)            f14daynewpaycnt,
            max(f30daynewpaycnt)            f30daynewpaycnt,
            max(f30daynewpayincome)         f30daynewpayincome,
            max(favgconpayday)              favgconpayday,
            max(fmaxconpayday)              fmaxconpayday,
            max(f1daymaxpaycnt)             f1daymaxpaycnt,
            max(f1dayminpaycnt)             f1dayminpaycnt,
            max(f3daymaxpaycnt)             f3daymaxpaycnt,
            max(f3dayminpaycnt)             f3dayminpaycnt,
            max(f7daymaxpaycnt)             f7daymaxpaycnt,
            max(f7dayminpaycnt)             f7dayminpaycnt,
            max(f30daymaxpaycnt)            f30daymaxpaycnt,
            max(f30dayminpaycnt)            f30dayminpaycnt,
            max(flastmonthppr)              flastmonthppr,
            max(flastmonthpar)              flastmonthpar,
            max(fregpayusernum)             fregpayusernum,
            max(fquarterpu)                 fquarterpu,
            max(fhyearpu)                   fhyearpu,
            max(fmonthincome_newpu)         fmonthincome_newpu,
            max(fincome_fpu)                fincome_fpu,
            max(fordercnt)                  fordercnt,
            max(forderusercnt)              forderusercnt,
            max(fbankruptincome)            fbankruptincome,
            max(fbankruptusercnt)           fbankruptusercnt
        from analysis.user_payment_fct_final_tmp_%(num_begin)s a
        group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_payment_fct_final_tmp_%(num_30dayago)s;
        drop table if exists analysis.user_payment_fct_lastmonth_part_%(num_30dayago)s;
        drop table if exists analysis.user_payment_fct_order_bankrupt_part_%(num_30dayago)s;
        drop table if exists analysis.user_payment_fct_continue_part_%(num_30dayago)s;
        drop table if exists analysis.user_payment_fct_day_part_%(num_30dayago)s;
        drop table if exists analysis.user_payment_fct_new_pay_part_%(num_30dayago)s;
        drop table if exists analysis.user_payment_fct_year_season_part_%(num_30dayago)s;
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
    a = merge_user_payment_fct(stat_date)
    a()
