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


class load_entrance_dim(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create external table if not exists analysis.entrance_game_dim
        (
          fsk             bigint,
          fentrance_id    bigint,
          fplatformfsk    bigint,
          fgamefsk        bigint,
          fversionfsk     bigint,
          fentrance_name  varchar(100),
          fmemo           varchar(200)
        )
        location '/dw/analysis/entrance_game_dim'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
        add file hdfs://192.168.0.92:8020/dw/streaming/pg_exchange.py;

        insert into table analysis.entrance_game_dim
        select transform('entrance_game_dim', 'fentrance_id, fplatformfsk, fgamefsk, fversionfsk, fentrance_name, fmemo',
         c.fentrance_id, c.fplatformfsk, c.fgamefsk, c.fversionfsk, c.fentrance_name, c.fmemo)
        using 'pg_exchange.py'
        as (fsk bigint, fentrance_id bigint, fplatformfsk bigint, fgamefsk bigint, fversionfsk bigint, fentrance_name string, fmemo string)
        from (
                  select a.fentrance_id fentrance_id, a.fplatformfsk fplatformfsk, a.fgamefsk fgamefsk, a.fversionfsk fversionfsk,
                   a.fentrance_name fentrance_name, null fmemo
                from (
                        select distinct gs.fentrance_id fentrance_id, 0 fplatformfsk, bpm.fgamefsk fgamefsk, 0 fversionfsk, '??????' fentrance_name
                        from   ( select distinct coalesce(rs.fentrance_id, 0) fentrance_id, fbpid
                                from stage.user_dim rs
                               where rs.dt = "%(statdate)s"
                              union all
                              select distinct coalesce(ls.fentrance_id, 0) fentrance_id, fbpid
                                from stage.user_login_stg ls
                               where ls.dt = "%(statdate)s"
                                 ) gs
                        join analysis.bpid_platform_game_ver_map bpm
                            on gs.fbpid=bpm.fbpid
                    ) a
                left outer join analysis.entrance_game_dim ed
                    on ed.fentrance_id = a.fentrance_id and ed.fgamefsk=a.fgamefsk
                where ed.fentrance_id is null
            ) as c
        """    % query
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
a = load_entrance_dim(statDate)
a()
