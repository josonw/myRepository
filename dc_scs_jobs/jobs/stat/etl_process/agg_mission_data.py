#!/user/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_mission_data(BaseStat):
    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.mission_fct
        (
            fdate    date  ,
            fgamefsk    bigint  ,
            fplatformfsk    bigint  ,
            fversionfsk   bigint  ,
            fterminalfsk    bigint  ,
            fmission_tpl_type   string  ,
            fmission_tpl_type_name    string  ,
            fpname    string  ,
            fsubname    string  ,
            fante   bigint ,
            fsk   string  ,
            fname   string  ,
            fspark_cnt    bigint  ,
            fspark_succ_cnt   bigint  ,
            fspark_refuse_cnt   bigint  ,
            fspark_auto_cnt   bigint  ,
            fquit_cnt   bigint  ,
            fquit_succ_cnt    bigint  ,
            fquit_fail_cnt    bigint  ,
            fquit_award_cnt   bigint
                     )
        partitioned by(dt date)
        location '/dw/analysis/mission_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        use analysis;
        create table if not exists analysis.mission_dim
        (
        fgamefsk bigint,
        fsk string,
        fname string
        )
        location '/dw/analysis/mission_dim'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):

        query = { 'statdate':statDate, "num_begin": statDate.replace('-', '')}

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """--任务维度表
        insert into table analysis.mission_dim
        select  m.fgamefsk,m.fmission_tpl_id,m.fmission_tpl_name
           from (
                 select b.fgamefsk, a.fmission_tpl_id, a.fmission_tpl_name
                   from stage.mission_spark_stg a
                   join analysis.bpid_platform_game_ver_map b
                     on a.fbpid = b.fbpid
                  where a.dt = "%(statdate)s"
                  group by b.fgamefsk, a.fmission_tpl_id, a.fmission_tpl_name) m
            left outer join analysis.mission_dim n
                  on n.fgamefsk = m.fgamefsk and n.fsk = m.fmission_tpl_id
            where n.fgamefsk is null
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists stage.agg_mission_data_%(num_begin)s;

        create table stage.agg_mission_data_%(num_begin)s as
          select "%(statdate)s" fdate,
               b.fgamefsk,
               b.fplatformfsk,
               b.fversionfsk,
               b.fterminalfsk,
               fmission_tpl_type,
               fmission_tpl_type_name,
               fpname,
               fsubname,
               fante,
               a.fmission_tpl_id fsk,
               null fname,
               count(*) fspark_cnt,
               sum(case when fis_refuse = 0 then 1 else 0 end) fspark_succ_cnt,
               sum(case when fis_refuse = 1 then 1 else 0 end) fspark_refuse_cnt,
               sum(case when fis_refuse = 2 then 1 else 0 end) fspark_auto_cnt ,
               0  fquit_cnt,
               0  fquit_succ_cnt,
               0  fquit_fail_cnt,
               0  fquit_award_cnt
               from stage.mission_spark_stg a
          join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
         where a.dt = "%(statdate)s"
         group by b.fgamefsk,
                  b.fplatformfsk,
                  b.fversionfsk,
                  b.fterminalfsk,
                  a.fmission_tpl_type,
                  a.fmission_tpl_type_name,
                  a.fpname,
                  a.fsubname,
                  a.fante,
                  a.fmission_tpl_id  """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert into table stage.agg_mission_data_%(num_begin)s

        select "%(statdate)s" fdate,
               b.fgamefsk,
               b.fplatformfsk,
               b.fversionfsk,
               b.fterminalfsk,
               fmission_tpl_type,
               fmission_tpl_type_name,
               fpname,
               fsubname,
               fante,
               a.fmission_tpl_id fsk,
               null fname,
               0 fspark_cnt,
               0 fspark_succ_cnt,
               0 fspark_refuse_cnt,
               0 fspark_auto_cnt ,
               count(*)  fquit_cnt,
               sum(case when fis_failed = 0 then 1 else 0 end)  fquit_succ_cnt,
               sum(case when fis_failed = 1 then 1 else 0 end)  fquit_fail_cnt,
               sum(case when fis_failed = 2 then 1 else 0 end)  fquit_award_cnt
               from stage.mission_quit_stg a
           join analysis.bpid_platform_game_ver_map b
             on a.fbpid = b.fbpid
          where a.dt = "%(statdate)s"
          group by b.fgamefsk,
                   b.fplatformfsk,
                   b.fversionfsk,
                   b.fterminalfsk,
                   a.fmission_tpl_type,
                   a.fmission_tpl_type_name,
                   a.fpname,
                   a.fsubname,
                   a.fante,
                   a.fmission_tpl_id""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.mission_fct
        partition (dt = "%(statdate)s")
        select "%(statdate)s" fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fmission_tpl_type,
               fmission_tpl_type_name,
               fpname,
               fsubname,
               fante,
               fsk,
               fname,
               max(fspark_cnt) fspark_cnt,
               max(fspark_succ_cnt) fspark_succ_cnt,
               max(fspark_refuse_cnt) fspark_refuse_cnt,
               max(fspark_auto_cnt) fspark_auto_cnt ,
               max(fquit_cnt)  fquit_cnt,
               max(fquit_succ_cnt)  fquit_succ_cnt,
               max(fquit_fail_cnt)  fquit_fail_cnt,
               max(fquit_award_cnt)  fquit_award_cnt
               from stage.agg_mission_data_%(num_begin)s a
          group by fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   fterminalfsk,
                   fmission_tpl_type,
                   fmission_tpl_type_name,
                   fpname,
                   fsubname,
                   fante,
                   fsk,
                    fname""" % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_mission_data_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_mission_data(statDate, eid)
a()



