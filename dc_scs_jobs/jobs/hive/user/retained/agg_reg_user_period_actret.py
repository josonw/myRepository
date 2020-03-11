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



GROUPSET1 = sql_const.GROUPSET_FUID
alias_dic = {'bpid_tbl_alias':'bgm.','src_tbl_alias':'uai.'}


GROUPSET2 = {'alias':['src_tbl_alias'],
             'field':['dt'],
             'comb_value':[[1] ]}


class agg_reg_user_period_actret(BaseStat):
    """新增用户，在其后一段时间内的留存
    """
    def create_tab(self):
        hql = """create table if not exists dcnew.reg_user_period_actret
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,

                f7daycnt bigint,
                f14daycnt bigint,
                f30daycnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res



    def stat(self):
        query = { 'statdate':self.stat_date,"num_date": self.stat_date.replace("-", ""),
            'ld_30daylater':(datetime.datetime.strptime(self.stat_date, "%Y-%m-%d") + datetime.timedelta(days=30)).strftime('%Y-%m-%d'),
            'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
            'group_by':sql_const.extend_groupset(GROUPSET1, GROUPSET2)% alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)



        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        res = self.hq.exe_sql("""drop table if exists work.user_retained_indays_temp_%(num_date)s"""% query)
        if res != 0: return res


        hql = """
        create table work.user_retained_indays_temp_%(num_date)s
        as
            SELECT /*+ mapjoin(c)*/
            un.dt fdate,
            ua.dt bfdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            un.fuid,
            datediff(ua.dt, un.dt) retday
        from dim.reg_user_array un
        join (
            select
                dt,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fuid
            from dim.user_act_array
            where dt >= '%(ld_30dayago)s' AND dt <= '%(ld_30daylater)s'
            ) ua
         on un.fuid = ua.fuid
            and un.fgamefsk = ua.fgamefsk
            and un.fplatformfsk = ua.fplatformfsk
            and un.fhallfsk = ua.fhallfsk
            and un.fgame_id = ua.fsubgamefsk
            and un.fterminaltypefsk = ua.fterminaltypefsk
            and un.fversionfsk = ua.fversionfsk
            and un.fchannel_code = ua.fchannelcode
        where un.dt >= '%(ld_30dayago)s'
          and un.dt < '%(ld_dayend)s'
          and un.dt < ua.dt

        group by un.dt, ua.dt,
        un.fgamefsk, un.fplatformfsk, un.fhallfsk, un.fgame_id , un.fterminaltypefsk, un.fversionfsk, un.fchannel_code,
        un.fuid, datediff(ua.dt, un.dt)
        """% query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.reg_user_period_actret
        partition( dt )
        SELECT fdate,
               fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
               sum(f7daycnt) f7daycnt,
               sum(f14daycnt) f14daycnt,
               sum(f30daycnt) f30daycnt,
               fdate
        FROM
          ( SELECT m.fdate,
                   m.fgamefsk,
                   m.fplatformfsk,
                   m.fhallfsk,
                   m.fsubgamefsk,
                   m.fterminaltypefsk,
                   m.fversionfsk,
                   m.fchannelcode,
                   count(DISTINCT m.fuid) f7daycnt,
                   0 f14daycnt,
                   0 f30daycnt
           FROM work.user_retained_indays_temp_%(num_date)s m
           where retday>=1 AND retday<=7
           group by m.fdate,
           m.fgamefsk, m.fplatformfsk, m.fhallfsk, m.fsubgamefsk , m.fterminaltypefsk, m.fversionfsk, m.fchannelcode

           UNION ALL

           SELECT m.fdate,
                  m.fgamefsk,
                  m.fplatformfsk,
                  m.fhallfsk,
                  m.fsubgamefsk,
                  m.fterminaltypefsk,
                  m.fversionfsk,
                  m.fchannelcode,
                   0 f7daycnt,
                   count(DISTINCT m.fuid) f14daycnt,
                   0 f30daycnt
           FROM work.user_retained_indays_temp_%(num_date)s m
           where retday>=1 AND retday<=14
           group by m.fdate,
           m.fgamefsk, m.fplatformfsk, m.fhallfsk, m.fsubgamefsk , m.fterminaltypefsk, m.fversionfsk, m.fchannelcode

           UNION ALL

           SELECT m.fdate,
                  m.fgamefsk,
                  m.fplatformfsk,
                  m.fhallfsk,
                  m.fsubgamefsk,
                  m.fterminaltypefsk,
                  m.fversionfsk,
                  m.fchannelcode,
                   0 f7daycnt,
                   0 f14daycnt,
                   count(DISTINCT m.fuid) f30daycnt
           FROM work.user_retained_indays_temp_%(num_date)s m
           where retday>=1 AND retday<=30
           group by m.fdate,
           m.fgamefsk, m.fplatformfsk, m.fhallfsk, m.fsubgamefsk , m.fterminaltypefsk, m.fversionfsk, m.fchannelcode

           ) foo
           group by   fdate,
                      fgamefsk,
                      fplatformfsk,
                      fhallfsk,
                      fsubgamefsk,
                      fterminaltypefsk,
                      fversionfsk,
                      fchannelcode

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.reg_user_period_actret partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fhallfsk,
        fsubgamefsk,
        fterminaltypefsk,
        fversionfsk,
        fchannelcode,
        f7daycnt,
        f14daycnt,
        f30daycnt
        from dcnew.reg_user_period_actret
        where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        res = self.hq.exe_sql("""drop table if exists work.user_retained_indays_temp_%(num_date)s"""% query)
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
a = agg_reg_user_period_actret(statDate)
a()
