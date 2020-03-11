#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_num_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_num_fct
        (
          fdate            date,
          fplatformfsk        bigint,
          fgamefsk        bigint,
          fversionfsk        bigint,
          fterminalfsk        bigint,
          fpartynumfsk        bigint,
          fpartynum        bigint,
          f1daypartynum    bigint,
          f3daypartynum    bigint,
          f7daypartynum    bigint,
          fregpartynum     bigint,
          factpartynum     bigint,
          f7dayactpartynum bigint,
          fdbuydpartynum   bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_gameparty_num_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        fpartynumfsk = """
        case
            when gp.fgameparty_num=0 then 1777140815
            when gp.fgameparty_num=1 then 1782301487
            when 2<=gp.fgameparty_num and gp.fgameparty_num<=5 then 1782301488
            when 6<=gp.fgameparty_num and gp.fgameparty_num<=10 then 1782301489
            when 11<=gp.fgameparty_num and gp.fgameparty_num<=20 then 1777140817
            when 21<=gp.fgameparty_num and gp.fgameparty_num<=30 then 1777140818
            when 31<=gp.fgameparty_num and gp.fgameparty_num<=40 then 1777140819
            when 41<=gp.fgameparty_num and gp.fgameparty_num<=50 then 1777140820
            when 51<=gp.fgameparty_num and gp.fgameparty_num<=60 then 1777140821
            when 61<=gp.fgameparty_num and gp.fgameparty_num<=70 then 1777140822
            when 71<=gp.fgameparty_num and gp.fgameparty_num<=80 then 1777140823
            when 81<=gp.fgameparty_num and gp.fgameparty_num<=90 then 1777140824
            when 91<=gp.fgameparty_num and gp.fgameparty_num<=100 then 1777140825
            when 101<=gp.fgameparty_num and gp.fgameparty_num<=150 then 1777140826
            when 151<=gp.fgameparty_num and gp.fgameparty_num<=200 then 1777140827
            when 201<=gp.fgameparty_num and gp.fgameparty_num<=300 then 1777140828
            when 301<=gp.fgameparty_num and gp.fgameparty_num<=400 then 1777140829
            when 401<=gp.fgameparty_num and gp.fgameparty_num<=500 then 1777140830
            when 501<=gp.fgameparty_num and gp.fgameparty_num<=1000 then 1777140831
            else 1777140832
        end"""

        query = { 'statdate':statDate, "num_begin": statDate.replace('-', ''), 'fpartynumfsk':fpartynumfsk,
            'ld_1dayago': PublicFunc.add_days(statDate, -1), 'ld_3dayago': PublicFunc.add_days(statDate, -3),
            'ld_7dayago': PublicFunc.add_days(statDate, -7)
        }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    -- 活跃玩牌
        drop table if exists stage.agg_gameparty_num_data_%(num_begin)s;

        create table stage.agg_gameparty_num_data_%(num_begin)s as
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                %(fpartynumfsk)s fpartynumfsk,
                count(distinct gp.fuid) fpartynum,
                0 f1daypartynum,
                0 f3daypartynum,
                0 f7daypartynum,
                0 fregpartynum,
                count(distinct gp.fuid) factpartynum,
                0 f7dayactpartynum,
                0 fdbuydpartynum
            from (
                select c.fbpid fbpid, c.fuid fuid, nvl( sum(c.fplaycnt), 0 ) fgameparty_num
                  from (
                    select a.fbpid, a.fuid, b.fplaycnt
                    from stage.active_user_mid a
                    left outer join stage.gameparty_uid_playcnt_mid b
                          on a.fbpid = b.fbpid and a.fuid=b.fuid and b.dt="%(statdate)s"
                    where a.dt="%(statdate)s"
                  ) c
                  group by c.fbpid, c.fuid
            ) gp
            join analysis.bpid_platform_game_ver_map bpm
              on gp.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                %(fpartynumfsk)s
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 昨注玩牌 1,3,7
        insert into table stage.agg_gameparty_num_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                %(fpartynumfsk)s fpartynumfsk,
                0 fpartynum,
                count(case when reg_day="%(ld_1dayago)s" then gp.fuid else null end) f1daypartynum,
                count(case when reg_day="%(ld_3dayago)s" then gp.fuid else null end) f3daypartynum,
                count(case when reg_day="%(ld_7dayago)s" then gp.fuid else null end) f7daypartynum,
                0 fregpartynum,
                0 factpartynum,
                0 f7dayactpartynum,
                0 fdbuydpartynum
            from (
                select c.fbpid fbpid, c.fuid fuid, c.reg_day reg_day, nvl( sum(c.fplaycnt), 0 ) fgameparty_num
                  from (
                      select a.fbpid fbpid, a.fuid fuid, b.fplaycnt fplaycnt, a.reg_day reg_day
                      from (
                        select c.fbpid fbpid, c.fuid fuid, d.dt reg_day
                        from stage.user_dim d
                        join stage.active_user_mid c
                               on c.fbpid = d.fbpid and c.fuid = d.fuid and c.dt="%(statdate)s"
                        where cast(d.dt as string) in ("%(ld_1dayago)s", "%(ld_3dayago)s", "%(ld_7dayago)s")
                      ) a
                      left outer join stage.gameparty_uid_playcnt_mid b
                      on a.fbpid = b.fbpid and a.fuid=b.fuid and b.dt="%(statdate)s"
                  ) c
                  group by c.fbpid, c.fuid, c.reg_day
            ) gp
            join analysis.bpid_platform_game_ver_map bpm
              on gp.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                %(fpartynumfsk)s
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 注册用户玩牌
        insert into table stage.agg_gameparty_num_data_%(num_begin)s
        select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
            bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
            %(fpartynumfsk)s fpartynumfsk,
            0 fpartynum,
            0 f1daypartynum,
            0 f3daypartynum,
            0 f7daypartynum,
            count(distinct gp.fuid) fregpartynum,
            0 factpartynum,
            0 f7dayactpartynum,
            0 fdbuydpartynum
        from (
            select c.fbpid fbpid, c.fuid fuid, nvl( sum(c.fplaycnt), 0 ) fgameparty_num
              from (
                select a.fbpid fbpid, a.fuid fuid, b.fplaycnt fplaycnt
                from stage.user_dim a
                left outer join stage.gameparty_uid_playcnt_mid b
                      on a.fbpid = b.fbpid and a.fuid=b.fuid and b.dt="%(statdate)s"
                where a.dt="%(statdate)s"
              ) c
              group by c.fbpid, c.fuid
        ) gp
        join analysis.bpid_platform_game_ver_map bpm
          on gp.fbpid = bpm.fbpid
        group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
            %(fpartynumfsk)s
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 昨注回头用户，昨日玩牌
        insert into table stage.agg_gameparty_num_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                %(fpartynumfsk)s fpartynumfsk,
                0 fpartynum,
                0 f1daypartynum,
                0 f3daypartynum,
                0 f7daypartynum,
                0 fregpartynum,
                0 factpartynum,
                0 f7dayactpartynum,
                count(distinct gp.fuid) fdbuydpartynum
            from (
                select a.fbpid fbpid, a.fuid fuid, nvl( sum(b.fparty_num),0 ) fgameparty_num
                from (
                    select c.fbpid fbpid, c.fuid fuid
                    from stage.active_user_mid c
                    join stage.user_dim d
                           on c.fbpid = d.fbpid and c.fuid = d.fuid and d.dt = "%(ld_1dayago)s"
                    where c.dt="%(statdate)s"
                ) a
                left outer join stage.user_gameparty_info_mid b
                    on a.fbpid = b.fbpid and a.fuid=b.fuid and b.dt = "%(ld_1dayago)s"
                group by a.fbpid, a.fuid
            ) gp
            join analysis.bpid_platform_game_ver_map bpm
              on gp.fbpid = bpm.fbpid
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                %(fpartynumfsk)s
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_gameparty_num_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpartynumfsk,
                sum(fpartynum) fpartynum,
                sum(f1daypartynum) f1daypartynum,
                sum(f3daypartynum) f3daypartynum,
                sum(f7daypartynum) f7daypartynum,
                sum(fregpartynum) fregpartynum,
                sum(factpartynum) factpartynum,
                sum(f7dayactpartynum) f7dayactpartynum,
                sum(fdbuydpartynum) fdbuydpartynum
            from stage.agg_gameparty_num_data_%(num_begin)s
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpartynumfsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_num_data_%(num_begin)s;
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
a = agg_gameparty_num_data(statDate, eid)
a()
