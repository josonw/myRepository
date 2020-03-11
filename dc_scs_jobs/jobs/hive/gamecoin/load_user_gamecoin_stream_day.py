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


class load_user_gamecoin_stream_day(BaseStat):
    """
    游戏币流水，发放、消耗中间表
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_gamecoin_stream_day
        (
          fdate                string,              --时间
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户游戏ID
          fact_type            bigint,              --操作类型
          fact_id              string,              --操作编号
          fnum                 bigint,              --操作数值
          fcnt                 bigint,              --操作次数
          flast_time           string               --最后操作时间
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = {'statdate':self.stat_date,
                'null_int_report':sql_const.NULL_INT_REPORT}

        self.hql_dict.update(query)

        hql = """
        drop table if exists work.user_gamecoin_stream_day_%(num_begin)s;
        create table work.user_gamecoin_stream_day_%(num_begin)s
        as
        select
            fbpid,
            coalesce(fgame_id,cast (0 as bigint)) fgame_id,
            fchannel_code,
            fuid,
            act_type,
            act_id,
            sum(abs(coalesce(act_num,0))) fnum,
            count(1) fcnt,
            max(lts_at) flast_time
        from stage.pb_gamecoins_stream_stg
        where dt = "%(statdate)s"
        group by fbpid,
            coalesce(fgame_id,cast (0 as bigint)),
            fchannel_code,
            fuid,
            act_type,
            act_id;
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table dim.user_gamecoin_stream_day
        partition( dt="%(statdate)s" )
        select /*+ mapjoin(ci) */
            '%(statdate)s' fdate,
                fbpid,
                coalesce(gs.fgame_id, %(null_int_report)d) fgame_id,
                coalesce(ci.ftrader_id, %(null_int_report)d) fchannel_code,
                fuid,
                act_type fact_type,
                act_id fact_id,
                sum(fnum) fnum,
                sum(fcnt) fcnt,
                max(flast_time) flast_time
         from work.user_gamecoin_stream_day_%(num_begin)s gs
         left join analysis.marketing_channel_pkg_info ci
              on gs.fchannel_code = ci.fid
            group by fbpid,
                coalesce(gs.fgame_id, %(null_int_report)d),
                coalesce(ci.ftrader_id, %(null_int_report)d),
                fuid,
                act_type ,
                act_id
        ;
        drop table if exists work.user_gamecoin_stream_day_%(num_begin)s;
        """ % self.hql_dict

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
a = load_user_gamecoin_stream_day(statDate, eid)
a()
