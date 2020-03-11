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


class agg_reflux_user_gameparty(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reflux_user_gameparty
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                freflux int comment '回流天数，如7表示7日回流',
                feq0 bigint comment '玩牌局数为0的回流用户人数',
                feq1 bigint comment '玩牌局数为1的回流用户人数',
                feq2 bigint comment '玩牌局数为2的回流用户人数',
                feq3 bigint comment '玩牌局数为3的回流用户人数',
                feq4 bigint comment '玩牌局数为4的回流用户人数',
                feq5 bigint comment '玩牌局数为5的回流用户人数',
                feq6 bigint comment '玩牌局数为6的回流用户人数',
                feq7 bigint comment '玩牌局数为7的回流用户人数',
                feq8 bigint comment '玩牌局数为8的回流用户人数',
                feq9 bigint comment '玩牌局数为9的回流用户人数',
                feq10 bigint comment '玩牌局数为10的回流用户人数',
                feq11 bigint comment '玩牌局数为11的回流用户人数',
                feq12 bigint comment '玩牌局数为12的回流用户人数',
                feq13 bigint comment '玩牌局数为13的回流用户人数',
                feq14 bigint comment '玩牌局数为14的回流用户人数',
                feq15 bigint comment '玩牌局数为15的回流用户人数',
                feq16 bigint comment '玩牌局数为16的回流用户人数',
                feq17 bigint comment '玩牌局数为17的回流用户人数',
                feq18 bigint comment '玩牌局数为18的回流用户人数',
                feq19 bigint comment '玩牌局数为19的回流用户人数',
                feq20 bigint comment '玩牌局数为20的回流用户人数',
                fmq21lq30 bigint comment '玩牌局数大于等于21小于等于30的回流用户人数',
                fmq31lq50 bigint comment '玩牌局数大于等于31小于等于50的回流用户人数',
                fmq51lq100 bigint comment '玩牌局数大于等于51小于等于100的回流用户人数',
                fm100 bigint comment '玩牌局数大于100的回流用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        query = { 'statdate':self.stat_date,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reflux_user_gameparty
        partition(dt = '%(statdate)s' )

     select "%(statdate)s" fdate,
            fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
            freflux flossdays,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 0 then ur.fuid else null end),0) feq0,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 1 then ur.fuid else null end),0) feq1,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 2 then ur.fuid else null end),0) feq2,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 3 then ur.fuid else null end),0) feq3,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 4 then ur.fuid else null end),0) feq4,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 5 then ur.fuid else null end),0) feq5,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 6 then ur.fuid else null end),0) feq6,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 7 then ur.fuid else null end),0) feq7,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 8 then ur.fuid else null end),0) feq8,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 9 then ur.fuid else null end),0) feq9,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 10 then ur.fuid else null end),0) feq10,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 11 then ur.fuid else null end),0) feq11,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 12 then ur.fuid else null end),0) feq12,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 13 then ur.fuid else null end),0) feq13,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 14 then ur.fuid else null end),0) feq14,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 15 then ur.fuid else null end),0) feq15,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 16 then ur.fuid else null end),0) feq16,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 17 then ur.fuid else null end),0) feq17,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 18 then ur.fuid else null end),0) feq18,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 19 then ur.fuid else null end),0) feq19,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) = 20 then ur.fuid else null end),0) feq20,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) >= 21 and coalesce(ur.fparty_num,0) <= 30 then ur.fuid else null end),0) fmq21lq30,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) >= 31 and coalesce(ur.fparty_num,0) <= 50 then ur.fuid else null end),0) fmq31lq50,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) >= 51 and coalesce(ur.fparty_num,0) <= 100 then ur.fuid else null end),0) fmq51lq100,
            coalesce(count(distinct case when coalesce(ur.fparty_num,0) > 100 then ur.fuid else null end),0) fm100
       from dim.user_reflux_array ur
      where ur.dt='%(statdate)s'
        and ur.freflux_type='cycle'
      group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               freflux
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
a = agg_reflux_user_gameparty(statDate)
a()
