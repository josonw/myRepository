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
import service.sql_const_bak as sql_const

"""
每日付费各项指标统计
"""
class agg_pay_income_tmp(BaseStat):

    def create_tab(self):
        hql = """create table if not exists dcnew.pay_income_tmp
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fgroupingid int,
            fdpayucnt bigint,
            fdincome decimal(38,2)
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bgm.','src_tbl_alias':'src.'}
        query = {'statdate' : self.stat_date,
            'bpid_tbl_alias' : alias_dic['bpid_tbl_alias'][:-1],
            'src_tbl_alias' : alias_dic['src_tbl_alias'][:-1],
            'convert_groupid' : sql_const.HQL_CONVERT_GROUPID_ALL,
            'group_by' : sql_const.HQL_GROUP_BY_ALL % alias_dic,
            'null_str_group_rule' : sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule' : sql_const.NULL_INT_GROUP_RULE,
            'null_str_report' : sql_const.NULL_STR_REPORT,
            'null_int_report' : sql_const.NULL_INT_REPORT
        }

        hql = """
        insert overwrite table dcnew.pay_income_tmp
        partition(dt = '%(statdate)s')
          select /*+ MAPJOIN(%(bpid_tbl_alias)s) */ '%(statdate)s' fdate,
                %(bpid_tbl_alias)s.fgamefsk,
                %(bpid_tbl_alias)s.fplatformfsk,
                coalesce(%(bpid_tbl_alias)s.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(%(src_tbl_alias)s.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(%(bpid_tbl_alias)s.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(%(bpid_tbl_alias)s.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(%(src_tbl_alias)s.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                %(null_int_report)d fgroupingid,
                count(distinct %(src_tbl_alias)s.fplatform_uid) fdpayucnt,
                round(sum(%(src_tbl_alias)s.fcoins_num * %(src_tbl_alias)s.frate),2) fdincome
            from(
                select /*+ MAPJOIN(mp) */
                    fbpid,
                    coalesce(ugo.fgame_id,%(null_int_report)d) fgame_id,
                    coalesce(ftrader_id,%(null_int_report)d) fchannel_code,
                    us.fplatform_uid,
                    us.fcoins_num,
                    us.frate
                    from(
                        select
                            fbpid,
                            fchannel_id fchannel_code,
                            fplatform_uid,
                            forder_id,
                            fcoins_num,
                            frate
                            from stage.payment_stream_stg
                            where dt='%(statdate)s') us
                    left join
                    analysis.marketing_channel_pkg_info mp
                    on us.fchannel_code = mp.fid
                    left join
                    (select
                        forder_id,
                        max(coalesce(fgame_id,cast (0 as bigint))) fgame_id
                        from stage.user_generate_order_stg
                        where dt='%(statdate)s' group by forder_id) ugo
                    on us.forder_id  = ugo.forder_id
                ) %(src_tbl_alias)s
            join dim.bpid_map %(bpid_tbl_alias)s
            on %(src_tbl_alias)s.fbpid = %(bpid_tbl_alias)s.fbpid

            %(group_by)s
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
a = agg_pay_income_tmp(statDate)
a()
