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
将日数据与月数据分开，日数据先算，月数据后算
每日活跃用户统计：
活跃的来源有：登录、金流及玩牌
统计的指标有：每日活跃、每日登录用户数目及次数、金流和玩牌用户数目
"""
class agg_act_user_day(BaseStat):

    """
    创建每日活跃用户统计结果表
    """
    def create_tab(self):

        hql = """create table if not exists dcnew.act_user
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fdactucnt bigint comment '每日活跃用户数目',
                fdlgnucnt bigint comment '每日登录用户数目',
                fdlgncnt bigint comment '每日登录次数',
                fdgsucnt bigint comment '每日金流用户数目',
                fdgpucnt bigint comment '每日玩牌用户数目',
                fwaucnt bigint comment '周活跃人数',
                fmaucnt bigint comment '月活跃人数',
                f7dlgnucnt bigint comment '7日登录用户人数',
                f30dlgnucnt bigint comment '30日登录用户人数',
                f7dactucnt bigint comment '7日活跃用户人数',
                f30dactucnt bigint comment '30日活跃用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'group_by':sql_const.HQL_GROUP_BY_ALL % alias_dic,
            'group_by_no_sub_game':sql_const.HQL_GROUP_BY_NO_SUB_GAME % alias_dic,
            'group_by_include_sub_game':sql_const.HQL_GROUP_BY_INCLUDE_SUB_GAME % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        date = PublicFunc.date_define(self.stat_date)
        query.update(date)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        """
        测试发现Map执行慢，故增加Map数目
        """
        self.hq.exe_sql("set mapreduce.input.fileinputformat.split.maxsize=1280000;set hive.auto.convert.join=false;")
        # res = self.hq.exe_sql("""set mapreduce.job.name= %s; """%self.__class__.__name__)
        # if res != 0: return res

        hql = """
        insert overwrite table dcnew.act_user
        partition(dt = '%(statdate)s' )
        select
                '%(statdate)s' fdate,
                ua.fgamefsk,
                ua.fplatformfsk,
                ua.fhallfsk,
                ua.fsubgamefsk,
                ua.fterminaltypefsk,
                ua.fversionfsk,
                ua.fchannelcode,
                count(distinct ua.fuid) fdactucnt,
                count(distinct case when ua.flogin_cnt > 0 then ua.fuid else null end) fdlgnucnt,
                sum(ua.flogin_cnt) fdlgncnt,
                count(distinct case when ua.fis_change_gamecoins = 1 then ua.fuid else null end) fdgsucnt,
                count(distinct case when ua.fparty_num > 0 then ua.fuid else null end) fdgpucnt,
                0 fwaucnt,
                0 fmaucnt,
                0 f7dlgnucnt,
                0 f30dlgnucnt,
                0 f7dactucnt,
                0 f30dactucnt
            from
                dim.user_act_array ua
            where
                ua.dt = '%(statdate)s' and  fsubgamefsk <> %(null_int_report)d
            group by ua.fgamefsk,ua.fplatformfsk,ua.fhallfsk,ua.fsubgamefsk,ua.fterminaltypefsk,ua.fversionfsk,ua.fchannelcode
        """ % query

        res = self.hq.exe_sql(hql)
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
a = agg_act_user_day(statDate)
a()
