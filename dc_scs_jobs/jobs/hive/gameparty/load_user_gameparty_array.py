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
创建每日活跃用户维度表(去fbpid, 按照7个维度9种组合汇总后的维度表)
"""
class load_user_gameparty_array(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_gameparty_array
        (
            fgamefsk             bigint,            --游戏ID
            fplatformfsk         bigint,            --平台ID
            fhallfsk             bigint,            --大厅ID
            fsubgamefsk          bigint,            --子游戏ID
            fterminaltypefsk     bigint,            --终端ID
            fversionfsk          bigint,            --版本ID
            fchannelcode         bigint,            --渠道ID
            fuid                 bigint,            --用户ID
            fparty_num           decimal(32),       --牌局数
            fcharge              decimal(32,6),       --总台费
            fwin_amt             decimal(32),       --赢得的游戏币
            flose_amt            decimal(32),       --输掉的游戏币
            fwin_party_num       decimal(32),       --赢牌局数
            flose_party_num      decimal(32),       --输牌局数
            fplay_time           decimal(32)        --玩牌时长
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}

        GROUPSET1 = {'alias':['src_tbl_alias'],
                     'field':['fuid'],
                     'comb_value':[ [1] ] }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        query = sql_const.query_list(self.stat_date, alias_dic, GROUPSET1)

        hql_list=[]
        for i in range(2):
            hql = """
                   select
                      %(select_field_str)s,
                      fuid,
                      sum(fparty_num) fparty_num,
                      sum(fcharge) fcharge,
                      sum(fwin_amt) fwin_amt,
                      sum(flose_amt) flose_amt,
                      sum(fwin_party_num) fwin_party_num,
                      sum(flose_party_num) flose_party_num,
                      sum(fplay_time) fplay_time
                   from dim.user_gameparty a
                   join dim.bpid_map b
                     on a.fbpid = b.fbpid
                   where a.dt = '%(ld_daybegin)s' and b.hallmode=%(hallmode)s
                      %(group_by)s
                  """ % query[i]
            hql_list.append(hql)

        hql = """
        insert overwrite table dim.user_gameparty_array
        partition( dt="%s" )
        %s;
        insert into table dim.user_gameparty_array
        partition( dt="%s" )
        %s
              """%(self.stat_date, hql_list[0], self.stat_date, hql_list[1])

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

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
a = load_user_gameparty_array(statDate)
a()
