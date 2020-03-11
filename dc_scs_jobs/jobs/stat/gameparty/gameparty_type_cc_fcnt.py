#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class gameparty_type_cc_fcnt(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.game_cardtype_cc_fct
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fname        varchar(50),   --底注分布
          ftype        varchar(50),   --牌型分布
          fcnt         bigint,
          fusercnt     bigint,
          fpname       varchar(100),  --场次一级分类
          fsubname     varchar(100)   --场次二级分类
        )
        partitioned by(dt date)
        location '/dw/analysis/game_cardtype_cc_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        use analysis;
        create table if not exists analysis.game_cardtype_new
        (
          fdate        date,
          fgamefsk        bigint,
          fplatformfsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          ftype        varchar(50),
          fcnt         bigint,
          fusercnt     bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/game_cardtype_new'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        use analysis;
        create external table if not exists analysis.game_card_type_dim
        (
          fsk        bigint,
          fgamefsk        bigint,
          ftype    varchar(50),
          fgamename    varchar(200)
        )
        location '/dw/analysis/game_card_type_dim'
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

        hql = """ -- 牌型场次底注分布图
        insert overwrite table analysis.game_cardtype_cc_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
                '' fname, a.fcard_type ftype,
                count(1) fcnt, count(distinct(a.fuid)) fusercnt,
                a.fpname fpname, a.fsubname fsubname
            from analysis.bpid_platform_game_ver_map b
            join stage.user_gameparty_stg a
                on a.fbpid = b.fbpid and a.dt="%(statdate)s"
                and a.fcard_type is not null
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk,
                 a.fcard_type, a.fpname, a.fsubname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """ -- 只计算牌型的人数人次
        insert overwrite table analysis.game_cardtype_new
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk,
                b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk, a.fcard_type ftype,
                count(1) fcnt, count(distinct(a.fuid)) fusercnt
            from analysis.bpid_platform_game_ver_map b
            join stage.user_gameparty_stg a
                on a.fbpid = b.fbpid and a.dt="%(statdate)s"
                and a.fcard_type is not null
            group by b.fgamefsk, b.fplatformfsk, b.fversionfsk, b.fterminalfsk, a.fcard_type
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        ## -- 牌型类型添加
        hql = """
        add file hdfs://192.168.0.92:8020/dw/streaming/pg_exchange.py;

        insert into table analysis.game_card_type_dim
        select transform('analysis.game_card_type_dim', 'fgamefsk,ftype,fgamename', c.fgamefsk, c.ftype, c.fgamename)
        using 'pg_exchange.py'
        as (fsk bigint, fgamefsk bigint, ftype string, fgamename string)
          from (
              select a.fgamefsk fgamefsk, a.ftype ftype, a.ftype fgamename
            from (
                select distinct fgamefsk, ftype from analysis.game_cardtype_cc_fct
                where dt = "%(statdate)s" and ftype is not null
                and ftype != '' and ftype != ' '
            ) as a
            left outer join analysis.game_card_type_dim b
                on a.fgamefsk = b.fgamefsk and a.ftype = b.ftype
            where b.ftype is null
          ) as c
        """    % query
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
a = gameparty_type_cc_fcnt(statDate, eid)
a()
