#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_user_source_data(BaseStat):
    """ 用户来源，广告用户，feed，push
    """
    def create_tab(self):
        hql = """create table if not exists analysis.active_user_feed_ad
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fad_cnt bigint,
                ffeed_cnt bigint,
                fother_cnt bigint,
                push_cnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        hql = """
        insert overwrite table analysis.active_user_feed_ad
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 max(fad_cnt) fad_cnt,
                 max(ffeed_cnt) ffeed_cnt,
                 max(fother_cnt) fother_cnt,
                 max(push_cnt) push_cnt
                from (
                  select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                         0 fad_cnt,
                         0 ffeed_cnt,
                         count(distinct fuid) fother_cnt,
                         0 push_cnt
                    from stage.active_user_mid a
                    join analysis.bpid_platform_game_ver_map b
                      on a.fbpid = b.fbpid
                    where a.dt = '%(ld_daybegin)s'
                   group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk
                  union all
                  select b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                         0 fad_cnt,
                         count(distinct fuid) ffeed_cnt,
                         0 fother_cnt,
                         0 push_cnt
                    from (select b.fbpid, b.fuid
                            from stage.active_user_mid a
                            join stage.feed_clicked_stg b
                              on a.fbpid = b.fbpid
                             and a.fuid = b.fuid
                             and b.dt = '%(ld_daybegin)s'
                             and b.ffeed_as = 1
                            where a.dt = '%(ld_daybegin)s'
                           group by b.fbpid, b.fuid) a
                    join analysis.bpid_platform_game_ver_map b
                      on a.fbpid = b.fbpid
                   group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk
                  union all
                  select d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk,
                         count(distinct b.fuid) fad_cnt,
                         0 ffeed_cnt,
                         0 fother_cnt,
                         0 push_cnt
                    from stage.active_user_mid a
                    join stage.user_dim b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and b.fad_code is not null and b.fad_code != ""
                    join analysis.bpid_platform_game_ver_map d
                      on a.fbpid = d.fbpid
                   where a.dt = '%(ld_daybegin)s'
                   group by d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk
                  union all
                  select fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk,
                        0 fad_cnt,
                        0 ffeed_cnt,
                        0 fother_cnt,
                        count(distinct(a.fuid))  push_cnt
                   from stage.active_user_mid a
                   join stage.push_send_stg b
                     on a.fbpid = b.fbpid
                    and a.fuid = b.fuid
                    and b.dt = '%(ld_daybegin)s'
                   join analysis.bpid_platform_game_ver_map d
                     on a.fbpid = d.fbpid
                  where a.dt = '%(ld_daybegin)s'
                  group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
                   ) tmp
                   group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
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
a = agg_act_user_source_data(statDate)
a()
