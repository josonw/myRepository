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

class agg_reg_user_ad_dis(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reg_user_ad_dis
            (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fadid bigint,
                fdsucnt bigint
            )
            partitioned by (dt date);
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'convert_groupid_all':sql_const.HQL_CONVERT_GROUPID_ALL,
            'group_by_fuid_no_sub':sql_const.HQL_GROUP_BY_FUID_NO_SUB_GAME % {'bpid_tbl_alias':'bm.','src_tbl_alias':'uam.'},
            'group_by':sql_const.HQL_GROUP_BY_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'},
            'group_by_no_sub_game':sql_const.HQL_GROUP_BY_NO_SUB_GAME % alias_dic,
            'group_by_include_sub_game':sql_const.HQL_GROUP_BY_INCLUDE_SUB_GAME % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT,
            'null_str_report':sql_const.NULL_STR_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reg_user_ad_dis
        partition(dt = '%(statdate)s')

        select
            '%(statdate)s' fdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            un.fad_code fadid,
            count(un.fuid) fdsucnt
        from
            dim.reg_user_array un
        where
            un.dt = '%(statdate)s'
        group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
        un.fversionfsk,un.fchannel_code,un.fad_code
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
a = agg_reg_user_ad_dis(statDate)
a()
