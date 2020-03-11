#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reflux_user_property_data(BaseStat):
    """回流用户，用户资产分布
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_backflow_property_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fnum bigint,
                fname varchar(50),
                f2userback bigint,
                f5userback bigint,
                f7userback bigint,
                f14userback bigint,
                f30userback bigint,
                f2userreflux bigint,
                f5userreflux bigint,
                f7userreflux bigint,
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
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict;""" % query)
        if res != 0: return res
        # 创建临时表
        hql = """
        set hive.auto.convert.join=false;
        insert overwrite table analysis.user_backflow_property_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fnum, fname ,
            count(distinct if(reflux_type='day' and reflux='2', fuid, null ) ) f2userback,
            count(distinct if(reflux_type='day' and reflux='5', fuid, null ) ) f5userback,
            count(distinct if(reflux_type='day' and reflux='7', fuid, null ) ) f7userback,
            count(distinct if(reflux_type='day' and reflux='14', fuid, null ) ) f14userback,
            count(distinct if(reflux_type='day' and reflux='30', fuid, null ) ) f30userback,
            count(distinct if(reflux_type='cycle' and reflux='2', fuid, null ) ) f2userreflux,
            count(distinct if(reflux_type='cycle' and reflux='5', fuid, null ) ) f5userreflux,
            count(distinct if(reflux_type='cycle' and reflux='7', fuid, null ) ) f7userreflux,
            count(distinct if(reflux_type='cycle' and reflux='14', fuid, null ) ) f14userreflux,
            count(distinct if(reflux_type='cycle' and reflux='30', fuid, null ) ) f30userreflux
        from (
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
               a.fuid, a.reflux_type, a.reflux,
               case when b.user_gamecoins_num <=0 then 0
                    when b.user_gamecoins_num >=1 and b.user_gamecoins_num <1000 then 1
                    when b.user_gamecoins_num >=1000 and b.user_gamecoins_num <5000 then 1000
                    when b.user_gamecoins_num >=5000 and b.user_gamecoins_num <10000 then 5000
                    when b.user_gamecoins_num >=10000 and b.user_gamecoins_num <50000 then 10000
                    when b.user_gamecoins_num >=50000 and b.user_gamecoins_num <100000 then 50000
                    when b.user_gamecoins_num >=100000 and b.user_gamecoins_num <500000 then 100000
                    when b.user_gamecoins_num >=500000 and b.user_gamecoins_num <1000000 then 500000
                    when b.user_gamecoins_num >=1000000 and b.user_gamecoins_num <5000000 then 1000000
                    when b.user_gamecoins_num >=5000000 and b.user_gamecoins_num <10000000 then 5000000
                    when b.user_gamecoins_num >=10000000 and b.user_gamecoins_num<50000000 then 10000000
                    when b.user_gamecoins_num >=50000000 and b.user_gamecoins_num<100000000 then 50000000
                    when b.user_gamecoins_num >=100000000 and b.user_gamecoins_num<1000000000 then 100000000
                    else 1000000000 end Fnum,
               case when b.user_gamecoins_num <=0 then '0' --string
                    when b.user_gamecoins_num >=1 and b.user_gamecoins_num <1000 then '1-1000'
                    when b.user_gamecoins_num >=1000 and b.user_gamecoins_num <5000 then '1000-5000'
                    when b.user_gamecoins_num >=5000 and b.user_gamecoins_num <10000 then '5000-1万'
                    when b.user_gamecoins_num >=10000 and b.user_gamecoins_num <50000 then '1万-5万'
                    when b.user_gamecoins_num >=50000 and b.user_gamecoins_num <100000 then '5万-10万'
                    when b.user_gamecoins_num >=100000 and b.user_gamecoins_num <500000 then '10万-50万'
                    when b.user_gamecoins_num >=500000 and b.user_gamecoins_num <1000000 then '50万-100万'
                    when b.user_gamecoins_num >=1000000 and b.user_gamecoins_num<5000000 then '100万-500万'
                    when b.user_gamecoins_num >=5000000 and b.user_gamecoins_num <10000000 then '500万-1000万'
                    when b.user_gamecoins_num >=10000000 and b.user_gamecoins_num<50000000 then '1000万-5000万'
                    when b.user_gamecoins_num >=50000000 and b.user_gamecoins_num<100000000 then '5000万-1亿'
                    when b.user_gamecoins_num >=100000000 and b.user_gamecoins_num<1000000000 then '1亿-10亿'
                    else '10亿+' end fname
          from stage.reflux_user_mid a
          join stage.pb_gamecoins_stream_mid b
            on a.fbpid = b.fbpid
           and a.fuid = b.fuid
           and b.dt = '%(ld_daybegin)s'
          join analysis.bpid_platform_game_ver_map c
            on a.fbpid = c.fbpid
         where a.dt = '%(ld_daybegin)s'
         ) tmp
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum, fname;

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
a = agg_reflux_user_property_data(statDate)
a()
