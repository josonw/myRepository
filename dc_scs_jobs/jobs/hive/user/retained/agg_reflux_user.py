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
留存指标
"""
class agg_reflux_user(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reflux_user
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                f2drfxucnt bigint comment '2日回流用户数',
                f5drfxucnt bigint comment '5日回流用户数',
                f7drfxucnt bigint comment '7日回流用户数',
                f14drfxucnt bigint comment '14日回流用户数',
                f30drfxucnt bigint comment '30日回流用户数'
            )
            partitioned by (dt date)
        """
        res = self.hq.exe_sql(hql)


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'group_by_fuid_all':sql_const.HQL_GROUP_BY_FUID_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'},
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)

        hql = """
        insert overwrite table dcnew.reflux_user
        partition(dt ='%(statdate)s')

        select '%(statdate)s' fdate,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fgame_id fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               fchannel_code fchannelcode,
               count(distinct if(ur.freflux = 2, ur.fuid, null)) f2duserback,
               count(distinct if(ur.freflux = 5, ur.fuid, null)) f5duserback,
               count(distinct if(ur.freflux = 7, ur.fuid, null)) f7duserback,
               count(distinct if(ur.freflux = 14, ur.fuid, null)) f14duserback,
               count(distinct if(ur.freflux = 30, ur.fuid, null)) f30duserback
          from dim.user_reflux_array ur
         where ur.dt = '%(statdate)s' and freflux_type = 'cycle'
         group by
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fgame_id,
               fterminaltypefsk,
               fversionfsk,
               fchannel_code
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
a = agg_reflux_user(statDate)
a()
