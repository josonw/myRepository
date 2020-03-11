#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reflux_user_party_data(BaseStat):
    """回流用户，用户牌局数分布
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_backflow_party_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fpartycnt bigint,
                f2dusernum bigint,
                f5dusernum bigint,
                f7dusernum bigint,
                f14dusernum bigint,
                f30dusernum bigint,
                f2dreflux bigint,
                f5dreflux bigint,
                f7dreflux bigint,
                f14dreflux bigint,
                f30dreflux bigint
                )
                partitioned by ( dt date )
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
        set hive.auto.convert.join=false;
        insert overwrite table analysis.user_backflow_party_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
                 fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpartycnt,
              count(distinct if( a.reflux_type='day' and a.reflux=2, a.fuid, null )) f2dusernum,
              count(distinct if( a.reflux_type='day' and a.reflux=5, a.fuid, null )) f5dusernum,
              count(distinct if( a.reflux_type='day' and a.reflux=7, a.fuid, null )) f7dusernum,
              count(distinct if( a.reflux_type='day' and a.reflux=14, a.fuid, null )) f14dusernum,
              count(distinct if( a.reflux_type='day' and a.reflux=30, a.fuid, null )) f30dusernum,
              count(distinct if( a.reflux_type='cycle' and a.reflux=2, a.fuid, null )) f2dreflux,
              count(distinct if( a.reflux_type='cycle' and a.reflux=5, a.fuid, null )) f5dreflux,
              count(distinct if( a.reflux_type='cycle' and a.reflux=7, a.fuid, null )) f7dreflux,
              count(distinct if( a.reflux_type='cycle' and a.reflux=14, a.fuid, null )) f14dreflux,
              count(distinct if( a.reflux_type='cycle' and a.reflux=30, a.fuid, null )) f30dreflux
          from (select a.fbpid, reflux_type, reflux, a.fuid, sum(fparty_num) fpartycnt
                  from stage.reflux_user_mid a
                  join stage.user_gameparty_info_mid b
                    on a.fbpid = b.fbpid
                   and a.fuid = b.fuid
                   and b.dt = '%(ld_daybegin)s'
                 where a.dt = '%(ld_daybegin)s'
                 group by a.fbpid, reflux_type, reflux, a.fuid
             ) a join analysis.bpid_platform_game_ver_map c
                    on a.fbpid = c.fbpid
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpartycnt;


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
a = agg_reflux_user_party_data(statDate)
a()
