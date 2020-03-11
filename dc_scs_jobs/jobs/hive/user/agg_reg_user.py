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
每日新增注册用户统计
"""
class agg_reg_user(BaseStat):

    """
    创建每日新增的注册用户统计结果表
    """
    def create_tab(self):
        """
        fgroupingid 废弃字段
        """
        hql = """create table if not exists dcnew.reg_user
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
            fdregucnt bigint,
            fdregpayucnt bigint,
            fdregpaycnt bigint,
            fdregpayamt decimal(20,2),
            fdregptyucnt bigint,
            fdregptycnt bigint,
            fdregbkpucnt bigint,
            fdreglgncnt bigint
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)
        return res


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'','src_tbl_alias':''}
        query = {'statdate':self.stat_date,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reg_user
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
            %(null_int_report)d fgroupingid,
            max(fdregucnt) fdregucnt,
            max(fdregpayucnt) fdregpayucnt,
            max(fdregpaycnt) fdregpaycnt,
            max(fdregpayamt) fdregpayamt,
            max(fdregptyucnt) fdregptyucnt,
            max(fdregptycnt) fdregptycnt,
            max(fdregbkpucnt) fdregbkpucnt,
            max(fdreglgncnt) fdreglgncnt
        from(
            select
                un.fgamefsk,
                un.fplatformfsk,
                un.fhallfsk,
                un.fgame_id,
                un.fterminaltypefsk,
                un.fversionfsk,
                un.fchannel_code,
                count(distinct un.fuid) fdregucnt,
                count(distinct piu.fuid) fdregpayucnt,
                sum(coalesce(piu.fpay_cnt,0)) fdregpaycnt,
                sum(coalesce(piu.ftotal_usd_amt,0)) fdregpayamt,
                0 fdregptyucnt,
                0 fdregptycnt,
                0 fdregbkpucnt,
                sum(coalesce(uau.flogin_cnt,0)) fdreglgncnt
            from dim.reg_user_array un

            left join dim.user_pay_array piu
            on un.fuid = piu.fuid
                and un.fgamefsk = piu.fgamefsk
                and un.fplatformfsk = piu.fplatformfsk
                and un.fhallfsk = piu.fhallfsk
                and un.fgame_id = piu.fsubgamefsk
                and un.fterminaltypefsk = piu.fterminaltypefsk
                and un.fversionfsk = piu.fversionfsk
                and un.fchannel_code = piu.fchannelcode
                and piu.dt = '%(statdate)s'

            left join dim.user_act_array uau
            on un.fuid = uau.fuid
                and un.fgamefsk = uau.fgamefsk
                and un.fplatformfsk = uau.fplatformfsk
                and un.fhallfsk = uau.fhallfsk
                and un.fgame_id = uau.fsubgamefsk
                and un.fterminaltypefsk = uau.fterminaltypefsk
                and un.fversionfsk = uau.fversionfsk
                and un.fchannel_code = uau.fchannelcode
                and uau.dt = '%(statdate)s'
            where un.dt = '%(statdate)s'
            group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,un.fversionfsk,un.fchannel_code

            union all

            select
                un.fgamefsk,
                un.fplatformfsk,
                un.fhallfsk,
                un.fgame_id,
                un.fterminaltypefsk,
                un.fversionfsk,
                un.fchannel_code,
                0 fdregucnt,
                0 fdregpayucnt,
                0 fdregpaycnt,
                0 fdregpayamt,
                count(distinct giu.fuid) fdregptyucnt,
                sum(coalesce(giu.fparty_num,0)) fdregptycnt,
                count(distinct bru.fuid) fdregbkpucnt,
                0 fdreglgncnt
            from dim.reg_user_array un

            left join dim.user_gameparty_array giu
            on un.fuid = giu.fuid
                and un.fgamefsk = giu.fgamefsk
                and un.fplatformfsk = giu.fplatformfsk
                and un.fhallfsk = giu.fhallfsk
                and un.fgame_id = giu.fsubgamefsk
                and un.fterminaltypefsk = giu.fterminaltypefsk
                and un.fversionfsk = giu.fversionfsk
                and un.fchannel_code = giu.fchannelcode
                and giu.dt = '%(statdate)s'

            left join dim.user_bankrupt_array bru
            on un.fuid = bru.fuid
                and un.fgamefsk = bru.fgamefsk
                and un.fplatformfsk = bru.fplatformfsk
                and un.fhallfsk = bru.fhallfsk
                and un.fgame_id = bru.fsubgamefsk
                and un.fterminaltypefsk = bru.fterminaltypefsk
                and un.fversionfsk = bru.fversionfsk
                and un.fchannel_code = bru.fchannelcode
                and bru.dt = '%(statdate)s'
                and frupt_cnt>0
            where un.dt = '%(statdate)s'
            group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,un.fversionfsk,un.fchannel_code

        ) src
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code;
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
a = agg_reg_user(statDate)
a()
