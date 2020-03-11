#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameparty_room_data(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.user_gameparty_fct
        (
            fdate           date,
            fplatformfsk        bigint,
            fgamefsk        bigint,
            fversionfsk        bigint,
            fterminalfsk        bigint,
            fpartyfsk        bigint,
            fusernum        bigint,
            fpartynum       bigint,
            fcharge         decimal(20,4),
            fplayusernum    bigint,
            fregplayusernum bigint,
            factplayusernum bigint,
            fpartytypefsk        bigint,
            fregplaynum     bigint
        )
        partitioned by(dt date)
        location '/dw/analysis/user_gameparty_fct'
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

        hql = """
        drop table if exists stage.agg_gameparty_room_data_%(num_begin)s;

        create table stage.agg_gameparty_room_data_%(num_begin)s as
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                nvl(nd.fsk,1) fpartyfsk,
                0 fusernum,
                sum(gs.fpartynum) fpartynum,
                sum(gs.fcharge) fcharge,
                0 fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                case when gs.ftype=0 then 12781097
                    when gs.ftype=1 then 32174101
                    else 1
                end fpartytypefsk,
                0 fregplaynum
                from (
                    select fbpid, ftype, fname, round(sum(fcharge), 4) fcharge, count(1) fpartynum
                    from stage.finished_gameparty_stg
                    where dt = "%(statdate)s"
                    group by fbpid, ftype, fname
                        union all
                    select fbpid,
                        case when fpname='比赛场' then 1 else 0 end ftype,
                        fsubname fname, round(sum(fcharge), 4) fcharge,
                        count(distinct concat_ws('0', finning_id, ftbl_id ) ) fpartynum
                    from stage.user_gameparty_stg
                    where dt = "%(statdate)s"
                    group by fbpid, case when fpname='比赛场' then 1 else 0 end, fsubname
                ) gs
                join analysis.bpid_platform_game_ver_map bpm
                    on gs.fbpid = bpm.fbpid
                left join dcnew.gameparty_name_dim nd
                    on nd.fgameparty_name = gs.fname
                        and nd.fgamefsk = bpm.fgamefsk
                group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                    nvl(nd.fsk,1),
                    case when gs.ftype=0 then 12781097
                        when gs.ftype=1 then 32174101
                    else 1 end
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert into table stage.agg_gameparty_room_data_%(num_begin)s
            select bpm.fplatformfsk fplatformfsk, bpm.fgamefsk fgamefsk,
                bpm.fversionfsk fversionfsk, bpm.fterminalfsk fterminalfsk,
                nvl(nd.fsk, 1) fpartyfsk,
                sum(ut.fplaycnt) fusernum,
                0 fpartynum,
                0 fcharge,
                count(distinct ut.fuid) fplayusernum,
                count(distinct ud.fuid) fregplayusernum,
                0 factplayusernum,
                case when ut.ftype=0 then 12781097
                    when ut.ftype=1 then 32174101
                    else 1
                end fpartytypefsk,
                0 fregplaynum
            from stage.gameparty_uid_playcnt_mid ut
            join analysis.bpid_platform_game_ver_map bpm
                on ut.fbpid = bpm.fbpid
            left join stage.user_dim ud
                on ud.fbpid = ut.fbpid and ud.fuid = ut.fuid and ud.dt = "%(statdate)s"
            left join dcnew.gameparty_name_dim nd
                on nd.fgameparty_name = ut.fname and nd.fgamefsk = bpm.fgamefsk
            where ut.dt = "%(statdate)s"
            group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk,
                nvl(nd.fsk, 1),
                case when ut.ftype=0 then 12781097
                    when ut.ftype=1 then 32174101
                    else 1
                end
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_gameparty_fct
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate, fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpartyfsk,
            sum(fusernum) fusernum,
            sum(fpartynum) fpartynum,
            sum(fcharge) fcharge,
            sum(fplayusernum) fplayusernum,
            sum(fregplayusernum) fregplayusernum,
            sum(factplayusernum) factplayusernum,
            fpartytypefsk,
            sum(fregplaynum) fregplaynum
        from stage.agg_gameparty_room_data_%(num_begin)s
        group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpartyfsk, fpartytypefsk
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gameparty_room_data_%(num_begin)s;
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
a = agg_gameparty_room_data(statDate, eid)
a()
