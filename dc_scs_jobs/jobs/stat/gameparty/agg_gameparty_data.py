# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_data(BaseStat):
    def create_tab(self):

        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_game_fct
        (
          fdate           date,
          fplatformfsk    bigint, --平台编号
          fgamefsk        bigint, --游戏编号
          fversionfsk     bigint, --版本编号
          fterminalfsk    bigint, --终端编号
          fusernum        bigint, --玩牌人次
          fpartynum       bigint, --牌局数
          fcharge         decimal(20,4), --每局费用
          fplayusernum    bigint, --玩牌人数
          fregplayusernum bigint, --当天注册用户玩牌人次
          factplayusernum bigint, --当天活跃用户玩牌人次
          fpaypartynum    bigint, --付费用户且玩牌场次
          fpayusernum     bigint  --付费用户且玩牌用户数
        )
        partitioned by(dt date)
        location '/dw/analysis/user_gameparty_game_fct'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.hq.debug = 0
        query = {
            'statdate': statDate,
            "num_begin": statDate.replace('-', '')
        }

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """    -- 老牌局
        drop table if exists stage.agg_gameparty_data_1_%(num_begin)s;
        create table stage.agg_gameparty_data_1_%(num_begin)s as
          select fplatformfsk,
                 fgamefsk,
                 fversion_old fversionfsk,
                 fterminalfsk,
                 count(*) fpartynum,
                 round(sum(a.fcharge), 4) fcharge,
                 1 flag --新老牌局标志
            from stage.finished_gameparty_stg a
            join dim.bpid_map bpm
                on a.fbpid = bpm.fbpid
            where a.dt = "%(statdate)s"
           group by fplatformfsk, fgamefsk, fversion_old, fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 新牌局
        insert into table stage.agg_gameparty_data_1_%(num_begin)s
          select fplatformfsk,
                 fgamefsk,
                 fversion_old fversionfsk,
                 fterminalfsk,
                 count(distinct concat_ws('0', finning_id, ftbl_id) ) fpartynum, --牌局数
                 round(sum(a.fcharge), 4) fcharge,
                 2 flag --新老牌局标志
                from stage.user_gameparty_stg a
                join dim.bpid_map bpm
                on a.fbpid = bpm.fbpid
                where a.dt = "%(statdate)s"
               group by fplatformfsk, fgamefsk, fversion_old, fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 新老牌局整合 PS:直接去掉了老牌局数据
        drop table if exists stage.agg_gameparty_data_2_%(num_begin)s;

        create table stage.agg_gameparty_data_2_%(num_begin)s as
          select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                   0 fusernum,
                 gs.fpartynum fpartynum,
                 gs.fcharge fcharge,
                 0 fplayusernum,
                 0 fregplayusernum,
                 0 factplayusernum,
                 0 fpaypartynum,
                 0 fpayusernum
                from (
                    select ss.fplatformfsk fplatformfsk, ss.fgamefsk fgamefsk,
                    ss.fversionfsk fversionfsk, ss.fterminalfsk fterminalfsk,
                        ss.fpartynum fpartynum, ss.fcharge fcharge
                    from (
                        select fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpartynum, fcharge,
                            row_number() over(partition by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk order by flag desc) rown
                        from stage.agg_gameparty_data_1_%(num_begin)s
                    ) ss
                    where ss.rown = 1
                ) gs
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 玩牌用户
        insert into table stage.agg_gameparty_data_2_%(num_begin)s
          select fplatformfsk,
                 fgamefsk,
                 fversion_old fversionfsk,
                 fterminalfsk,
                 sum(ut.fplaycnt) fusernum,
                 0 fpartynum,
                 0 fcharge,
                 count(distinct ut.fuid) fplayusernum,
                 count(distinct ud.fuid) fregplayusernum,
                 0 factplayusernum,
                 0 fpaypartynum,
                 0 fpayusernum
             from stage.gameparty_uid_playcnt_mid ut
             left join stage.user_dim ud
               on ud.fbpid = ut.fbpid
               and ud.fuid = ut.fuid
               and ud.dt = "%(statdate)s"
             join dim.bpid_map bpm
               on ut.fbpid = bpm.fbpid
             where ut.dt = "%(statdate)s"
            group by fplatformfsk, fgamefsk, fversion_old, fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 付费用户牌局数，付费玩牌用户数
        insert into table stage.agg_gameparty_data_2_%(num_begin)s
          select fplatformfsk, fgamefsk, fversion_old fversionfsk, fterminalfsk,
                 0 fusernum,
                 0 fpartynum,
                 0 fcharge,
                 0 fplayusernum,
                 0 fregplayusernum,
                 0 factplayusernum,
                 sum(fparty_num) fpaypartynum,
                 count(distinct b.fplatform_uid) fpayusernum
            from stage.user_gameparty_info_mid a
            join stage.user_pay_info b
              on a.fuid = b.fuid
              and a.fbpid = b.fbpid
              and b.dt = "%(statdate)s"
            join dim.bpid_map c
              on a.fbpid = c.fbpid
            where a.dt = "%(statdate)s"
            group by fplatformfsk, fgamefsk, fversion_old, fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_gameparty_game_fct
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk,
                sum(fusernum) fusernum,
                sum(fpartynum) fpartynum,
                sum(fcharge) fcharge,
                sum(fplayusernum) fplayusernum,
                sum(fregplayusernum) fregplayusernum,
                sum(factplayusernum) factplayusernum,
                sum(fpaypartynum) fpaypartynum,
                sum(fpayusernum) fpayusernum
            from stage.agg_gameparty_data_2_%(num_begin)s
            group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

            # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_data_1_%(num_begin)s;

        drop table if exists stage.agg_gameparty_data_2_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


# 愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else:
    args = sys.argv[1].split(',')
    statDate = args[0]


# 生成统计实例
a = agg_gameparty_data(statDate, eid)
a()
