#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_change_data(BaseStat):

    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0


        hql = """

create table analysis.top100_insert_data_20161221_bak like analysis.top100_insert_data;
insert overwrite table analysis.top100_insert_data_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fplatform_uid, a.fuid, a.fpaycnt, a.dip, a.dt from analysis.top100_insert_data a ;

insert overwrite table analysis.top100_insert_data partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fplatform_uid, a.fuid, a.fpaycnt, a.dip, a.dt from analysis.top100_insert_data a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_gameparty_retained_fct_20161221_bak like analysis.user_gameparty_retained_fct;
insert overwrite table analysis.user_gameparty_retained_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.f60daycnt, a.f90daycnt, a.dt from analysis.user_gameparty_retained_fct a ;

insert overwrite table analysis.user_gameparty_retained_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.f60daycnt, a.f90daycnt, a.dt from analysis.user_gameparty_retained_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_bs_sub_retained_fct_20161221_bak like analysis.user_bs_sub_retained_fct;
insert overwrite table analysis.user_bs_sub_retained_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fpname, a.fsubname, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.dt from analysis.user_bs_sub_retained_fct a ;

insert overwrite table analysis.user_bs_sub_retained_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fpname, a.fsubname, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.dt from analysis.user_bs_sub_retained_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_bs_retained_fct_20161221_bak like analysis.user_bs_retained_fct;
insert overwrite table analysis.user_bs_retained_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.dt from analysis.user_bs_retained_fct a ;

insert overwrite table analysis.user_bs_retained_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.dt from analysis.user_bs_retained_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_back_pname_fct_20161221_bak like analysis.user_back_pname_fct;
insert overwrite table analysis.user_back_pname_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fpname, a.total_user, a.f1dayregbackcnt, a.f2dayregbackcnt, a.f3dayregbackcnt, a.f4dayregbackcnt, a.f5dayregbackcnt, a.f6dayregbackcnt, a.f7dayregbackcnt, a.f14dayregbackcnt, a.f30dayregbackcnt, a.dt from analysis.user_back_pname_fct a ;

insert overwrite table analysis.user_back_pname_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fpname, a.total_user, a.f1dayregbackcnt, a.f2dayregbackcnt, a.f3dayregbackcnt, a.f4dayregbackcnt, a.f5dayregbackcnt, a.f6dayregbackcnt, a.f7dayregbackcnt, a.f14dayregbackcnt, a.f30dayregbackcnt, a.dt from analysis.user_back_pname_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_retained_pname_fct_20161221_bak like analysis.user_retained_pname_fct;
insert overwrite table analysis.user_retained_pname_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fpname, a.total_user, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.f60daycnt, a.dt from analysis.user_retained_pname_fct a ;

insert overwrite table analysis.user_retained_pname_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fpname, a.total_user, a.f1daycnt, a.f2daycnt, a.f3daycnt, a.f4daycnt, a.f5daycnt, a.f6daycnt, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.f60daycnt, a.dt from analysis.user_retained_pname_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_month_retained_au_fct_20161221_bak like analysis.user_month_retained_au_fct;
insert overwrite table analysis.user_month_retained_au_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fmonthaucnt, a.f1monthcnt, a.f2monthcnt, a.f3monthcnt, a.dt from analysis.user_month_retained_au_fct a ;

insert overwrite table analysis.user_month_retained_au_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fmonthaucnt, a.f1monthcnt, a.f2monthcnt, a.f3monthcnt, a.dt from analysis.user_month_retained_au_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_week_retained_au_fct_20161221_bak like analysis.user_week_retained_au_fct;
insert overwrite table analysis.user_week_retained_au_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fweekaucnt, a.f1weekcnt, a.f2weekcnt, a.f3weekcnt, a.f4weekcnt, a.f5weekcnt, a.f6weekcnt, a.f7weekcnt, a.f8weekcnt, a.dt from analysis.user_week_retained_au_fct a ;

insert overwrite table analysis.user_week_retained_au_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.fweekaucnt, a.f1weekcnt, a.f2weekcnt, a.f3weekcnt, a.f4weekcnt, a.f5weekcnt, a.f6weekcnt, a.f7weekcnt, a.f8weekcnt, a.dt from analysis.user_week_retained_au_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

create table analysis.user_reg_retain_period_fct_20161221_bak like analysis.user_reg_retain_period_fct;
insert overwrite table analysis.user_reg_retain_period_fct_20161221_bak partition(dt)  select a.fdate, a.fgamefsk, a.fplatformfsk, a.fversionfsk, a.fterminalfsk, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.dt from analysis.user_reg_retain_period_fct a ;

insert overwrite table analysis.user_reg_retain_period_fct partition(dt)  select a.fdate, a.fgamefsk, (case when a.fgamefsk = 4132314431 then coalesce(b.fplatformfsk_new,-13658) else a.fplatformfsk end) fplatformfsk, a.fversionfsk, a.fterminalfsk, a.f7daycnt, a.f14daycnt, a.f30daycnt, a.dt from analysis.user_reg_retain_period_fct a  left join analysis.bpid_map_hall_platform_rel b on a.fgamefsk = b.fgamefsk and a.fplatformfsk = b.fplatformfsk and a.fversionfsk = b.fversionfsk and a.fterminalfsk = b.fterminalfsk ;

        """

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = agg_change_data()
    a()
