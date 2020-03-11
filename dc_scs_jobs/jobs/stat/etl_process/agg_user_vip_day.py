#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_vip_day(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_vip_fct
        (
          fdate            date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fagefsk        bigint,
          foccupationalfsk        bigint,
          fsexfsk        bigint,
          fcityfsk        bigint,
          fgradefsk        bigint,
          fviptypefsk        bigint,
          fusercnt         bigint,
          faddusernum      bigint,
          fdueusernum      bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_vip_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        query = { 'statdate':statDate, "num_begin": statDate.replace('-', ''),
            'ld_1daylater':PublicFunc.add_days(statDate, 1) }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    --
        drop table if exists stage.agg_user_vip_day_%(num_begin)s;

        create table stage.agg_user_vip_day_%(num_begin)s as
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                1 fagefsk, 1 foccupationalfsk, 1 fsexfsk, 1 fcityfsk, 1 fgradefsk,
                nvl(vtd.fsk, 1) fviptypefsk,
                count(distinct vs.fuid) fusercnt,
                0 faddusernum, 0 fdueusernum
            from (
                select fbpid, fuid, fvip_type
                from stage.user_vip_stg
                where fdue_at > "%(statdate)s" and fvip_at < "%(ld_1daylater)s"
            ) vs
            join analysis.bpid_platform_game_ver_map bpm
                on vs.fbpid = bpm.fbpid
            left outer join analysis.vip_type_dim vtd
                on vs.fvip_type = vtd.ftype
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(vtd.fsk, 1)
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        insert into table stage.agg_user_vip_day_%(num_begin)s
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                1 fagefsk, 1 foccupationalfsk, 1 fsexfsk, 1 fcityfsk, 1 fgradefsk,
                nvl(vtd.fsk, 1) fviptypefsk, 0 fusercnt,
                count( distinct vs.fuid ) faddusernum, 0 fdueusernum
            from (
                select fbpid, fuid, fvip_type
                from stage.user_vip_stg
                where fvip_at >= "%(statdate)s" and fvip_at < "%(ld_1daylater)s"
            ) vs
            join analysis.bpid_platform_game_ver_map bpm
            on vs.fbpid = bpm.fbpid
            left outer join analysis.vip_type_dim vtd
                on vs.fvip_type = vtd.ftype
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(vtd.fsk, 1)
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    --
        insert into table stage.agg_user_vip_day_%(num_begin)s
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                1 fagefsk, 1 foccupationalfsk, 1 fsexfsk, 1 fcityfsk, 1 fgradefsk,
                nvl(vtd.fsk, 1) fviptypefsk, 0 fusercnt, 0 faddusernum,
                count(distinct vs.fuid) fdueusernum
            from (
                select fbpid, fuid, fvip_type
                from stage.user_vip_stg
                where fdue_at >= "%(statdate)s" and fdue_at < "%(ld_1daylater)s"
            ) vs
            join analysis.bpid_platform_game_ver_map bpm
            on vs.fbpid = bpm.fbpid
            left outer join analysis.vip_type_dim vtd
                on vs.fvip_type = vtd.ftype
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, nvl(vtd.fsk, 1)
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_vip_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                fagefsk, foccupationalfsk, fsexfsk, fcityfsk, fgradefsk, fviptypefsk,
                max(fusercnt) fusercnt,
                max(faddusernum) faddusernum,
                max(fdueusernum) fdueusernum
            from stage.agg_user_vip_day_%(num_begin)s
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                fagefsk, foccupationalfsk, fsexfsk, fcityfsk, fgradefsk, fviptypefsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

         # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_user_vip_day_%(num_begin)s;
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
a = agg_user_vip_day(statDate, eid)
a()
