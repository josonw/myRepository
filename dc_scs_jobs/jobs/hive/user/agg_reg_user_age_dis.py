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

class agg_reg_user_age_dis(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reg_user_age_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fdlq1 bigint comment '注册年龄等于1天的当天活跃用户人数',
                fdmq2lq3 bigint comment '注册年龄大于等于2天且小于等于3天的当天活跃用户人数',
                fdmq4lq7 bigint comment '注册年龄大于等于4天且小于等于7天的当天活跃用户人数',
                fdmq8lq14 bigint comment '注册年龄大于等于8天且小于等于14天的当天活跃用户人数',
                fdmq15lq30 bigint comment '注册年龄大于等于15天且小于等于30天的当天活跃用户人数',
                fdmq31lq90 bigint comment '注册年龄大于等于31天且小于等于90天的当天活跃用户人数',
                fdmq91lq180 bigint comment '注册年龄大于等于91天且小于等于180天的当天活跃用户人数',
                fdmq181lq365 bigint comment '注册年龄大于等于181天且小于等于365天的当天活跃用户人数',
                fdm365 bigint comment '注册年龄大于365天的当天活跃用户人数',
                fdlq0 bigint comment '注册年龄等于0天的当天活跃用户人数'
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
        insert overwrite table dcnew.reg_user_age_dis
        partition(dt = '%(statdate)s' )

        select
            '%(statdate)s' fdate,
            uau.fgamefsk,
            uau.fplatformfsk,
            uau.fhallfsk,
            uau.fsubgamefsk,
            uau.fterminaltypefsk,
            uau.fversionfsk,
            uau.fchannelcode,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday = 1 then un.fuid else null end),0) fdlq1,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 2 and un.retday <= 3 then uau.fuid else null end),0) fdmq2lq3,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 4 and un.retday <= 7 then uau.fuid else null end),0) fdmq4lq7,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 8 and un.retday <= 14 then uau.fuid else null end),0) fdmq8lq14,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 15 and un.retday <= 30 then uau.fuid else null end),0) fdmq15lq30,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 31 and un.retday <= 90 then uau.fuid else null end),0) fdmq31lq90,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 91 and un.retday <= 180 then uau.fuid else null end),0) fdmq91lq180,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday >= 181 and un.retday <= 365 then uau.fuid else null end),0) fdmq181lq365,
            coalesce(count(distinct case when (un.fsignup_at is null) or (un.fsignup_at is not null and un.retday > 365) then uau.fuid else null end),0) fdm365,
            coalesce(count(distinct case when un.fsignup_at is not null and un.retday = 0 then un.fuid else null end),0) fdlq0
         from
            dim.user_act_array uau
        left join
            (select
                dt,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fgame_id,
                fterminaltypefsk,
                fversionfsk,
                fchannel_code,
                fuid,
                fsignup_at,
                datediff('%(statdate)s', dt) retday
            from
                dim.reg_user_array
            where dt >= date_sub('%(statdate)s',365) and dt <= '%(statdate)s') un
        on uau.fuid = un.fuid
            and uau.fgamefsk = un.fgamefsk
            and uau.fplatformfsk = un.fplatformfsk
            and uau.fhallfsk = un.fhallfsk
            and uau.fsubgamefsk = un.fgame_id
            and uau.fterminaltypefsk = un.fterminaltypefsk
            and uau.fversionfsk = un.fversionfsk
            and uau.fchannelcode = un.fchannel_code
        where
             uau.dt = '%(statdate)s' and uau.fsubgamefsk <> %(null_int_report)d
        group by
            uau.fgamefsk,
            uau.fplatformfsk,
            uau.fhallfsk,
            uau.fsubgamefsk,
            uau.fterminaltypefsk,
            uau.fversionfsk,
            uau.fchannelcode;
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
a = agg_reg_user_age_dis(statDate)
a()
