#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_game_activity_rule_data(BaseStat):
    """活动数据
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.game_activity_rule_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fact_id varchar(50),
                frule_id varchar(50),
                fusercnt bigint,
                fusernum bigint,
                fregusercnt bigint,
                fregusernum bigint,
                fpayusercnt bigint,
                fpayusernum bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.game_activity_rule_dim
                (
                fsk bigint,
                fgamefsk bigint,
                fact_id varchar(50),
                frule_id string,
                fdis_name varchar(50)
                )
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update(date)
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        insert overwrite table analysis.game_activity_rule_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fgamefsk, fplatformfsk, fversionfsk,
             fterminalfsk, fact_id, frule_id,
             count(distinct concat(a.fuid, flts_at)) fusercnt,
             count(distinct a.fuid) fusernum,
             count(distinct if(a.fuid is null, null, concat(a.fuid, flts_at))) fregusercnt,
             count(distinct b.fuid) fregusernum,
             count(distinct if(c.fuid is null, null, concat(a.fuid, flts_at))) fpayusercnt,
             count(distinct c.fuid) fpayusernum
        from stage.game_activity_stg a
        left outer join stage.user_dim b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
         and b.dt = '%(ld_daybegin)s'
        left outer join stage.user_pay_info c
          on a.fuid = c.fuid
         and a.fbpid = c.fbpid
         and c.dt = '%(ld_daybegin)s'
        join analysis.bpid_platform_game_ver_map d
          on a.fbpid = d.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fact_id,
                frule_id;



        insert into table analysis.game_activity_rule_dim
        select  0 fsk, a.fgamefsk, a.fact_id, a.frule_id, a.frule_id fdis_name
        from analysis.game_activity_rule_fct a
        left outer join analysis.game_activity_rule_dim b
          on a.fgamefsk = b.fgamefsk
         and a.fact_id = b.fact_id
         and a.frule_id = b.frule_id
         and a.dt = '%(ld_daybegin)s'
        where b.fact_id is null
        group by a.fgamefsk, a.fact_id, a.frule_id


        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_game_activity_rule_data(statDate)
a()
