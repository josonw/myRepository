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
资产分布统计
"""
class agg_reg_user_channel_dis(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reg_user_channel_dis
            (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fchannelid bigint,
                fdsucnt bigint,
                fdaucnt bigint
            )
            partitioned by (dt date)
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
        insert overwrite table dcnew.reg_user_channel_dis
        partition(dt = '%(statdate)s')

        select
            '%(statdate)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code fchannelcode,
            fchannelid,
            max(fdsucnt) fdsucnt,
            max(fdaucnt) fdaucnt
        from
            (select
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fgame_id,
                fterminaltypefsk,
                fversionfsk,
                fchannel_code,
                fchannelid,
                count(distinct fuid) fdsucnt,
                0 fdaucnt
            from
                (select
                    un.fgamefsk,
                    un.fplatformfsk,
                    un.fhallfsk,
                    un.fgame_id,
                    un.fterminaltypefsk,
                    un.fversionfsk,
                    un.fchannel_code,
                    un.fchannel_code fchannelid,
                    un.fuid
                from
                    dim.reg_user_array un
                where un.dt = '%(statdate)s' and un.fchannel_code <> %(null_int_group_rule)d

                union all

                select
                    un.fgamefsk,
                    un.fplatformfsk,
                    un.fhallfsk,
                    un.fgame_id,
                    un.fterminaltypefsk,
                    un.fversionfsk,
                    %(null_int_group_rule)d fchannel_code,
                    un.fchannel_code fchannelid,
                    un.fuid
                from
                    dim.reg_user_array un
                where un.dt = '%(statdate)s' and un.fchannel_code <> %(null_int_group_rule)d
                    ) unu
            group by unu.fgamefsk,unu.fplatformfsk,unu.fhallfsk,fgame_id,unu.fterminaltypefsk,
            unu.fversionfsk,unu.fchannel_code,unu.fchannelid

            union all

            select
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fsubgamefsk,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannelcode,%(null_int_group_rule)d) fchannel_code,
                coalesce(fchannelid,%(null_int_report)d) fchannelid,
                0 fdsucnt,
                count(distinct fuid) fdaucnt
            from
                (
                  select fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fsubgamefsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fchannelcode,
                        fchannelcode fchannelid,
                        fuid
                   from dim.user_act_array
                  where dt = '%(statdate)s' and fchannelcode <> %(null_int_group_rule)d
                  union all
                  select fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fsubgamefsk,
                        fterminaltypefsk,
                        fversionfsk,
                        %(null_int_group_rule)d fchannelcode,
                        fchannelcode fchannelid,
                        fuid
                   from dim.user_act_array
                  where dt = '%(statdate)s' and fchannelcode <> %(null_int_group_rule)d
                  ) a
               group by fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,
                     fversionfsk,fchannelcode,fchannelid
            ) a
        group by fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id ,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code,
            fchannelid;
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
a = agg_reg_user_channel_dis(statDate)
a()
