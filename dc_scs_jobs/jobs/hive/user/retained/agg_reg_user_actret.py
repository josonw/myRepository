#! /usr/local/python272/bin/python
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

"""
多周留存用户统计
"""


GROUPSET1 = sql_const.GROUPSET_FUID
alias_dic = {'bpid_tbl_alias':'bgm.','src_tbl_alias':'uai.'}




class agg_reg_user_actret(BaseStat):

    def create_tab(self):
        hql = """create table if not exists dcnew.reg_user_actret
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            f1daycnt bigint,
            f2daycnt bigint,
            f3daycnt bigint,
            f4daycnt bigint,
            f5daycnt bigint,
            f6daycnt bigint,
            f7daycnt bigint,
            f14daycnt bigint,
            f30daycnt bigint,
            f60daycnt bigint,
            f90daycnt bigint
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)

        hql = """create table if not exists dcnew.reg_user_actback
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                f1dayregbackcnt bigint comment '1天前注册今天活跃用户人数',
                f3dayregbackcnt bigint comment '3天前注册今天活跃用户人数',
                f7dayregbackcnt bigint comment '7天前注册今天活跃用户人数',
                f14dayregbackcnt bigint comment '14天前注册今天活跃用户人数',
                f30dayregbackcnt bigint comment '30天前注册今天活跃用户人数'
            )
            partitioned by (dt date);
        """

        res = self.hq.exe_sql(hql)



    def stat(self):

        query = { 'statdate':self.stat_date,"num_date": self.stat_date.replace("-", ""),
            'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
            'group_by':sql_const.extend_groupset(GROUPSET1)% alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        res = self.hq.exe_sql("""drop table if exists work.reg_user_actret_temp_%(num_date)s"""% query)
        if res != 0: return res


        hql = """
        create table work.reg_user_actret_temp_%(num_date)s
        as
            select
            un.dt fdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            datediff('%(ld_daybegin)s', un.dt) retday,
            count(distinct un.fuid) fdregrtducnt
        from
            dim.reg_user_array un
        join dim.user_act_array ua
             on un.fuid = ua.fuid
            and un.fgamefsk = ua.fgamefsk
            and un.fplatformfsk = ua.fplatformfsk
            and un.fhallfsk = ua.fhallfsk
            and un.fgame_id = ua.fsubgamefsk
            and un.fterminaltypefsk = ua.fterminaltypefsk
            and un.fversionfsk = ua.fversionfsk
            and un.fchannel_code = ua.fchannelcode
            and ua.dt='%(ld_daybegin)s'
        where un.dt >= '%(ld_90dayago)s'
          and un.dt < '%(ld_dayend)s'

        group by un.dt, un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
        un.fversionfsk,un.fchannel_code, datediff('%(ld_daybegin)s', un.dt);

        """% query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.reg_user_actret
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
            max(f1daycnt) f1daycnt,
            max(f2daycnt) f2daycnt,
            max(f3daycnt) f3daycnt,
            max(f4daycnt) f4daycnt,
            max(f5daycnt) f5daycnt,
            max(f6daycnt) f6daycnt,
            max(f7daycnt) f7daycnt,
            max(f14daycnt) f14daycnt,
            max(f30daycnt) f30daycnt,
            max(f60daycnt) f60daycnt,
            max(f90daycnt) f90daycnt,
            fdate dt
        from (
            select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                if( retday=1, fdregrtducnt, 0 ) f1daycnt,
                if( retday=2, fdregrtducnt, 0 ) f2daycnt,
                if( retday=3, fdregrtducnt, 0 ) f3daycnt,
                if( retday=4, fdregrtducnt, 0 ) f4daycnt,
                if( retday=5, fdregrtducnt, 0 ) f5daycnt,
                if( retday=6, fdregrtducnt, 0 ) f6daycnt,
                if( retday=7, fdregrtducnt, 0 ) f7daycnt,
                if( retday=14, fdregrtducnt, 0 ) f14daycnt,
                if( retday=30, fdregrtducnt, 0 ) f30daycnt,
                if( retday=60, fdregrtducnt, 0 ) f60daycnt,
                if( retday=90, fdregrtducnt, 0 ) f90daycnt
            from  work.reg_user_actret_temp_%(num_date)s
            union all
            SELECT fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                   f1daycnt,
                   f2daycnt,
                   f3daycnt,
                   f4daycnt,
                   f5daycnt,
                   f6daycnt,
                   f7daycnt,
                   f14daycnt,
                   f30daycnt,
                   f60daycnt,
                   f90daycnt
            FROM dcnew.reg_user_actret
            WHERE dt >= '%(ld_90dayago)s'
              AND dt < '%(ld_dayend)s'
                    ) tmp group by fdate, fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table dcnew.reg_user_actback
        partition( dt ='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannelcode,
            sum(if(retday = 1, fdregrtducnt, 0)) f1dayregbackcnt,
            sum(if(retday = 3, fdregrtducnt, 0)) f3dayregbackcnt,
            sum(if(retday = 7, fdregrtducnt, 0)) f7dayregbackcnt,
            sum(if(retday = 14, fdregrtducnt, 0)) f14dayregbackcnt,
            sum(if(retday = 30, fdregrtducnt, 0)) f30dayregbackcnt
        from work.reg_user_actret_temp_%(num_date)s
        group by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,
        fversionfsk,fchannelcode
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.reg_user_actret partition(dt='3000-01-01')
        select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
               f1daycnt,
               f2daycnt,
               f3daycnt,
               f4daycnt,
               f5daycnt,
               f6daycnt,
               f7daycnt,
               f14daycnt,
               f30daycnt,
               f60daycnt,
               f90daycnt
        from dcnew.reg_user_actret
        where dt >= '%(ld_90dayago)s'
        and dt < '%(ld_dayend)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        res = self.hq.exe_sql("""drop table if exists work.reg_user_actret_temp_%(num_date)s"""% query)
        if res != 0: return res
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_reg_user_actret(statDate)
a()
