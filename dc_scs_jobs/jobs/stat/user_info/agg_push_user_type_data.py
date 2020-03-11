#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_push_user_type_data(BaseStat):
    """推送类型用户分布。
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_push_rule_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                frule_id varchar(50),
                fpushcnt bigint,
                fpushusernum bigint,
                frececnt bigint,
                freceusernum bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.user_push_tpl_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                frule_id varchar(50),
                ftpl_id varchar(50),
                fpushcnt bigint,
                fpushusernum bigint,
                frececnt bigint,
                freceusernum bigint,
                factivecnt bigint
                )
                partitioned by (dt date)
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.push_rule_dim
                (
                fsk bigint,
                fgamefsk bigint,
                frule_id varchar(50),
                fdis_name varchar(50)
                )
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create external table if not exists analysis.push_tpl_dim
                (
                fsk bigint,
                fgamefsk bigint,
                frule_id varchar(50),
                ftpl_id varchar(50),
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
        insert overwrite table analysis.user_push_rule_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             fterminalfsk,
             frule_id,
             max(fpushcnt) fpushcnt,
             max(fpushusernum) fpushusernum,
             max(frececnt) frececnt,
             max(freceusernum) freceusernum
        from (select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                     frule_id,
                     count(1) fpushcnt,
                     count(distinct a.fuid ) fpushusernum,
                     0 frececnt,
                     0 freceusernum
                from stage.push_send_stg a
                join analysis.bpid_platform_game_ver_map b
                  on a.fbpid = b.fbpid
                where a.dt = '%(ld_daybegin)s'
               group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id
              union all
              select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                     frule_id,
                     0 fpushcnt,
                     0 fpushusernum,
                     count(1) frececnt,
                     count(distinct a.fuid) freceusernum
                from stage.push_rece_stg a
                join analysis.bpid_platform_game_ver_map b
                  on a.fbpid = b.fbpid
                where a.dt = '%(ld_daybegin)s'
               group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id
        ) tmp
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id;



        insert overwrite table analysis.user_push_tpl_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             fterminalfsk,
             regexp_replace(frule_id, "\\u0000", ""),
             regexp_replace(ftpl_id, "\\u0000", ""),
             max(fpushcnt) fpushcnt,
             max(fpushusernum) fpushusernum,
             max(frececnt) frececnt,
             max(freceusernum) freceusernum,
             max(factivecnt) factivecnt
        from (select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                     frule_id,
                     ftpl_id,
                     count(1) fpushcnt,
                     count(distinct a.fuid) fpushusernum,
                     0 frececnt,
                     0 freceusernum,
                     0 factivecnt
                from stage.push_send_stg a
                join analysis.bpid_platform_game_ver_map b
                  on a.fbpid = b.fbpid
                where a.dt = '%(ld_daybegin)s'
               group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id, ftpl_id
              union all
              select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                     frule_id,
                     ftpl_id,
                     0 fpushcnt,
                     0 fpushusernum,
                     count(1) frececnt,
                     count(distinct(a.fuid)) freceusernum,
                     0 factivecnt
                from stage.push_rece_stg a
                join analysis.bpid_platform_game_ver_map b
                  on a.fbpid = b.fbpid
                where a.dt = '%(ld_daybegin)s'
               group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id, ftpl_id
              union all
             select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                    frule_id, ftpl_id,
                    0 fpushcnt,
                    0 fpushusernum,
                    0 frececnt,
                    0 freceusernum,
                    count(distinct a.fuid) factivecnt
              from stage.active_user_mid a
              join stage.push_rece_stg b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
               and b.dt = '%(ld_daybegin)s'
              join analysis.bpid_platform_game_ver_map d
                on a.fbpid = d.fbpid
             where a.dt = '%(ld_daybegin)s'
             group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id, ftpl_id
        ) tmp
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, frule_id, ftpl_id;



           insert overwrite table analysis.push_tpl_dim
         select 0 fsk, a.fgamefsk, a.frule_id, a.ftpl_id,
                a.ftpl_id  fdis_name
        from analysis.user_push_tpl_fct a
        left outer join analysis.push_tpl_dim b
         on a.fgamefsk = b.fgamefsk
        and a.ftpl_id = b.ftpl_id
        and a.frule_id = b.frule_id
        where b.fgamefsk is null
        and a.dt = '%(ld_daybegin)s'
        group by a.fgamefsk, a.frule_id, a.ftpl_id ;


         insert overwrite table analysis.push_rule_dim
         select 0 fsk,
              a.fgamefsk,
              a.frule_id,
              a.frule_id  fdis_name
        from analysis.user_push_tpl_fct a
        left outer join analysis.push_rule_dim b
         on a.fgamefsk = b.fgamefsk
        and a.frule_id = b.frule_id
        where b.fgamefsk is null
        and a.dt = '%(ld_daybegin)s'
        group by a.fgamefsk, a.frule_id;

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
a = agg_push_user_type_data(statDate)
a()
