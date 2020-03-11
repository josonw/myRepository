#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reflux_user_grade_data(BaseStat):
    """
    回流用户，用户等级

    f2userback    流失回流
    f2userreflux  回流用户
    """
    def create_tab(self):
        hql = """
        create external table if not exists analysis.user_backflow_grade_fct
        (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fversionfsk   bigint,
            fterminalfsk  bigint,
            fgrade        bigint,
            f2userback    bigint,
            f5userback    bigint,
            f7userback    bigint,
            f14userback   bigint,
            f30userback   bigint,
            f2userreflux  bigint,
            f5userreflux  bigint,
            f7userreflux  bigint,
            f14userreflux bigint,
            f30userreflux bigint
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
        if res != 0:
            return res

        # 创建临时表
        hql = """
        set hive.auto.convert.join=false;

        insert overwrite table analysis.user_backflow_grade_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk,
            c.fsk fgrade,
            count( distinct if( a.reflux_type='day' and a.reflux='2', a.fuid, null) ) f2userback,
            count( distinct if( a.reflux_type='day' and a.reflux='5', a.fuid, null) ) f5userback,
            count( distinct if( a.reflux_type='day' and a.reflux='7', a.fuid, null) ) f7userback,
            count( distinct if( a.reflux_type='day' and a.reflux='14', a.fuid, null) ) f14userback,
            count( distinct if( a.reflux_type='day' and a.reflux='30', a.fuid, null) ) f30userback,
            count( distinct if( a.reflux_type='cycle' and a.reflux='2', a.fuid, null) ) f2userreflux,
            count( distinct if( a.reflux_type='cycle' and a.reflux='5', a.fuid, null) ) f5userreflux,
            count( distinct if( a.reflux_type='cycle' and a.reflux='7', a.fuid, null) ) f7userreflux,
            count( distinct if( a.reflux_type='cycle' and a.reflux='14', a.fuid, null) ) f14userreflux,
            count( distinct if( a.reflux_type='cycle' and a.reflux='30', a.fuid, null) ) f30userreflux
        from stage.reflux_user_mid a
        left outer join stage.user_attribute_dim b
          on a.fbpid = b.fbpid and a.fuid = b.fuid
        join analysis.grade_dim c
          on b.fgrade = c.flevel
        join analysis.bpid_platform_game_ver_map d
          on a.fbpid = d.fbpid
        where a.dt='%(ld_daybegin)s'
        group by d.fgamefsk, d.fplatformfsk, d.fversionfsk, d.fterminalfsk, c.fsk;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

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
a = agg_reflux_user_grade_data(statDate)
a()
