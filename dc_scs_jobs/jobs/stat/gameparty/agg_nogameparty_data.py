#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_nogameparty_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_nogameparty_fct
        (
          fdate             date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fnoplayusernum    bigint,
          fregnoplayusernum bigint,
          factnoplayusernum bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_nogameparty_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate, "num_begin": statDate.replace('-', '') }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    -- 活跃没玩牌
        drop table if exists stage.agg_nogameparty_data_%(num_begin)s;

        create table stage.agg_nogameparty_data_%(num_begin)s as
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                count(distinct ud.fuid) fnoplayusernum,
                0 fregnoplayusernum,
                0 factnoplayusernum
            from (
                   select distinct c.fbpid fbpid, c.fuid fuid
                   from (
                        select a.fbpid fbpid, a.fuid fuid, b.fuid flag
                        from stage.active_user_mid a
                        left outer join stage.gameparty_uid_playcnt_mid b
                            on a.fbpid = b.fbpid and a.fuid = b.fuid and b.dt="%(statdate)s"
                        where a.dt = "%(statdate)s"
                   ) c
                    where c.flag is null
            ) ud
            join analysis.bpid_platform_game_ver_map bpm
              on ud.fbpid = bpm.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 注册没玩牌
        insert into table stage.agg_nogameparty_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                0 fnoplayusernum,
                count(distinct ud.fuid) fregnoplayusernum,
                0 factnoplayusernum
            from (
                select distinct c.fbpid fbpid, c.fuid fuid
                from (
                    select a.fbpid fbpid, a.fuid fuid, b.fuid flag
                    from stage.user_dim a
                    left outer join stage.gameparty_uid_playcnt_mid b
                        on a.fbpid = b.fbpid and a.fuid = b.fuid and b.dt="%(statdate)s"
                    where a.dt = "%(statdate)s"
                ) c
                where c.flag is null
            ) ud
            join analysis.bpid_platform_game_ver_map bpm
                on ud.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_nogameparty_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                max(fnoplayusernum) fnoplayusernum,
                max(fregnoplayusernum) fregnoplayusernum,
                max(factnoplayusernum) factnoplayusernum
            from stage.agg_nogameparty_data_%(num_begin)s
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_nogameparty_data_%(num_begin)s;
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
a = agg_nogameparty_data(statDate, eid)
a()
