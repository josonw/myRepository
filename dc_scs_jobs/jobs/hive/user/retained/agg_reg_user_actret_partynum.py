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

class agg_reg_user_actret_partynum(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reg_user_actret_partynum
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fptynumrange varchar(32) comment '当天新增用户玩牌局数所属范围',
                frpucnt bigint comment '当天新增用户玩牌局数对应的ID为fptynumfsk的人数',
                fd1ucnt bigint comment '新增用户的1日留存人数',
                fd2ucnt bigint comment '新增用户的2日留存人数',
                fd3ucnt bigint comment '新增用户的3日留存人数',
                fd4ucnt bigint comment '新增用户的4日留存人数',
                fd5ucnt bigint comment '新增用户的5日留存人数',
                fd6ucnt bigint comment '新增用户的6日留存人数',
                fd7ucnt bigint comment '新增用户的7日留存人数',
                fd14ucnt bigint comment '新增用户的14日留存人数',
                fd30ucnt bigint comment '新增用户的30日留存人数'
            )
            partitioned by (dt date);
        """

        res = self.hq.exe_sql(hql)
        return res


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'ld_30dayago': PublicFunc.add_days(statDate, -30),
            'group_by_fuid_no_sub':sql_const.HQL_GROUP_BY_FUID_NO_SUB_GAME % {'bpid_tbl_alias':'bm.','src_tbl_alias':'uam.'},
            'group_by':sql_const.HQL_GROUP_BY_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'},
            'group_by_no_sub_game':sql_const.HQL_GROUP_BY_NO_SUB_GAME % alias_dic,
            'group_by_include_sub_game':sql_const.HQL_GROUP_BY_INCLUDE_SUB_GAME % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        self.hq.exe_sql("set mapreduce.input.fileinputformat.split.maxsize=80000000;")
        self.hq.exe_sql("set hive.exec.reducers.bytes.per.reducer=128000000;")

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reg_user_actret_partynum
        partition(dt)

        select
            gp.fdate,
            gp.fgamefsk,
            gp.fplatformfsk,
            gp.fhallfsk,
            gp.fgame_id fsubgamefsk,
            gp.fterminaltypefsk,
            gp.fversionfsk,
            gp.fchannel_code fchannelcode,
            gp.fptynumrange fptynumfsk,
            count(distinct gp.fuid) frpucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 1) then gp.fuid else null end ) fd1ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 2) then gp.fuid else null end ) fd2ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 3) then gp.fuid else null end ) fd3ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 4) then gp.fuid else null end ) fd4ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 5) then gp.fuid else null end ) fd5ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 6) then gp.fuid else null end ) fd6ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 7) then gp.fuid else null end ) fd7ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 14) then gp.fuid else null end ) fd14ucnt,
            count(distinct case when uau.dt=date_add(gp.fdate, 30) then gp.fuid else null end ) fd30ucnt,
            gp.fdate dt
        from (
            select un.dt fdate,
                un.fgamefsk,
                un.fplatformfsk,
                un.fhallfsk,
                un.fgame_id,
                un.fterminaltypefsk,
                un.fversionfsk,
                un.fchannel_code,
                un.fuid,
                case when sum(nvl(fparty_num,0))=0 then '0局'
                    when sum(nvl(fparty_num,0))=1 then '1局'
                    when 2<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=5 then '2~5局'
                    when 6<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=10 then '6~10局'
                    when 11<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=20 then '11~20局'
                    when 21<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=30 then '21~30局'
                    when 31<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=40 then '31~40局'
                    when 41<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=50 then '41~50局'
                    when 51<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=60 then '51~60局'
                    when 61<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=70 then '61~70局'
                    when 71<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=80 then '71~80局'
                    when 81<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=90 then '81~90局'
                    when 91<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=100 then '91~100局'
                    when 101<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=150 then '101~150局'
                    when 151<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=200 then '151~200局'
                    when 201<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=300 then '201~300局'
                    when 301<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=400 then '301~400局'
                    when 401<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=500 then '401~500局'
                    when 501<=sum(nvl(fparty_num,0)) and sum(nvl(fparty_num,0))<=1000 then '501~1000局'
                    else '1000+局'
                end fptynumrange
            from dim.reg_user_array un

            left join
                (select fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fsubgamefsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fchannelcode,
                        fuid,
                        fparty_num,
                        dt
                   from dim.user_gameparty_array
                   where dt >= "%(ld_30dayago)s" and dt <= '%(statdate)s') giu
            on un.fuid = giu.fuid
                and un.fgamefsk = giu.fgamefsk
                and un.fplatformfsk = giu.fplatformfsk
                and un.fhallfsk = giu.fhallfsk
                and un.fgame_id = giu.fsubgamefsk
                and un.fterminaltypefsk = giu.fterminaltypefsk
                and un.fversionfsk = giu.fversionfsk
                and un.fchannel_code = giu.fchannelcode
                and un.dt = giu.dt
            where
                un.dt >= "%(ld_30dayago)s" and un.dt <= '%(statdate)s'
            group by un.dt,un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,
            un.fversionfsk,un.fchannel_code,un.fuid
        ) gp

        left join
            (select fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fsubgamefsk,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannelcode,
                    fuid,
                    dt
            from
                dim.user_act_array ua
            where dt >= "%(ld_30dayago)s" and dt <= '%(statdate)s'
            ) uau
        on gp.fuid = uau.fuid
            and gp.fgamefsk = uau.fgamefsk
            and gp.fplatformfsk = uau.fplatformfsk
            and gp.fhallfsk = uau.fhallfsk
            and gp.fgame_id = uau.fsubgamefsk
            and gp.fterminaltypefsk = uau.fterminaltypefsk
            and gp.fversionfsk = uau.fversionfsk
            and gp.fchannel_code = uau.fchannelcode

        group by gp.fdate,gp.fgamefsk,gp.fplatformfsk,gp.fhallfsk,gp.fgame_id,gp.fterminaltypefsk,
        gp.fversionfsk,gp.fchannel_code,gp.fptynumrange;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.reg_user_actret_partynum partition(dt='3000-01-01')
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
               fptynumrange,
               frpucnt,
               fd1ucnt,
               fd2ucnt,
               fd3ucnt,
               fd4ucnt,
               fd5ucnt,
               fd6ucnt,
               fd7ucnt,
               fd14ucnt,
               fd30ucnt
        from dcnew.reg_user_actret_partynum
        where dt >= '%(ld_30dayago)s'
        and dt < '%(statdate)s'
        """ % query
        res = self.hq.exe_sql(hql)
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
a = agg_reg_user_actret_partynum(statDate)
a()
