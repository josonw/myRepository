#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_user_property_data(BaseStat):
    """流失用户，资产分布
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_loss_gamecoin_property
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fnum bigint,
                fname varchar(50),
                f1dlossusernum bigint,
                f3dlossusernum bigint,
                f7dlossusernum bigint,
                f14dlossusernum bigint,
                f30dlossusernum bigint,
                f2eduserloss bigint,
                f5eduserloss bigint,
                f7eduserloss bigint,
                f14eduserloss bigint,
                f30eduserloss bigint
                )
                partitioned by ( dt date )
            """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        use stage;
        drop table if exists stage.user_loss_gamecoin_property_merge_tmp;
        create table stage.user_loss_gamecoin_property_merge_tmp
        (
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fnum bigint,
            f2eduserloss bigint,
            f5eduserloss bigint,
            f7eduserloss bigint,
            f14eduserloss bigint,
            f30eduserloss bigint
        )
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        # self.hq.debug = 0

        fnum = """
            case when a.user_gamecoins_num <=0 then 0
                when a.user_gamecoins_num >=1 and a.user_gamecoins_num <1000 then 1
                when a.user_gamecoins_num >=1000 and a.user_gamecoins_num <5000 then 1000
                when a.user_gamecoins_num >=5000 and a.user_gamecoins_num <10000 then 5000
                when a.user_gamecoins_num >=10000 and a.user_gamecoins_num <50000 then 10000
                when a.user_gamecoins_num >=50000 and a.user_gamecoins_num <100000 then 50000
                when a.user_gamecoins_num >=100000 and a.user_gamecoins_num <500000 then 100000
                when a.user_gamecoins_num >=500000 and a.user_gamecoins_num <1000000 then 500000
                when a.user_gamecoins_num >=1000000 and a.user_gamecoins_num <5000000 then 1000000
                when a.user_gamecoins_num >=5000000 and a.user_gamecoins_num <10000000 then 5000000
                when a.user_gamecoins_num >=10000000 and a.user_gamecoins_num<50000000 then 10000000
                when a.user_gamecoins_num >=50000000 and a.user_gamecoins_num<100000000 then 50000000
                when a.user_gamecoins_num >=100000000 and a.user_gamecoins_num<1000000000 then 100000000
            else 1000000000
            end fnum
        """

        query = {'fnum':fnum}
        date = PublicFunc.date_define( self.stat_date )
        query.update(date)
        res = self.hq.exe_sql("""use stage; set io.sort.mb=256; """ % query)
        if res != 0: return res

        hql = """  -- 每日(2)流失 用户资产
        insert overwrite table stage.user_loss_gamecoin_property_merge_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum,
            count(fuid) f2eduserloss, 0 f5eduserloss, 0 f7eduserloss, 0 f14eduserloss, 0 f30eduserloss
        from (
            select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, %(fnum)s, a.fuid fuid
            from (
                select b.fbpid, b.fuid, nvl(p.user_gamecoins_num, 0) user_gamecoins_num
                from stage.churn_user_mid b
                left join stage.pb_gamecoins_stream_mid p
                    on b.fbpid = p.fbpid and b.fuid = p.fuid and p.dt = '%(ld_2dayago)s'
                where b.churn_type = 'day' and b.days = 2
            ) a
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        ) d
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """  -- 每日(5)流失 用户资产
        insert into table stage.user_loss_gamecoin_property_merge_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum,
            0 f2eduserloss, count(fuid) f5eduserloss, 0 f7eduserloss, 0 f14eduserloss, 0 f30eduserloss
        from (
            select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, %(fnum)s, a.fuid fuid
            from (
                select b.fbpid, b.fuid, nvl(p.user_gamecoins_num, 0) user_gamecoins_num
                from stage.churn_user_mid b
                left join stage.pb_gamecoins_stream_mid p
                    on b.fbpid = p.fbpid and b.fuid = p.fuid and p.dt = '%(ld_5dayago)s'
                where b.churn_type = 'day' and b.days = 5
            ) a
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        ) d
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """  -- 每日(7)流失 用户资产
        insert into table stage.user_loss_gamecoin_property_merge_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum,
            0 f2eduserloss, 0 f5eduserloss, count(fuid) f7eduserloss, 0 f14eduserloss, 0 f30eduserloss
        from (
            select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, %(fnum)s, a.fuid fuid
            from (
                select b.fbpid, b.fuid, nvl(p.user_gamecoins_num, 0) user_gamecoins_num
                from stage.churn_user_mid b
                left join stage.pb_gamecoins_stream_mid p
                    on b.fbpid = p.fbpid and b.fuid = p.fuid and p.dt = '%(ld_7dayago)s'
                where b.churn_type = 'day' and b.days = 7
            ) a
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        ) d
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """  -- 每日(14)流失 用户资产
        insert into table stage.user_loss_gamecoin_property_merge_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum,
            0 f2eduserloss, 0 f5eduserloss, 0 f7eduserloss, count(fuid) f14eduserloss, 0 f30eduserloss
        from (
            select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, %(fnum)s, a.fuid fuid
            from (
                select b.fbpid, b.fuid, nvl(p.user_gamecoins_num, 0) user_gamecoins_num
                from stage.churn_user_mid b
                left join stage.pb_gamecoins_stream_mid p
                    on b.fbpid = p.fbpid and b.fuid = p.fuid and p.dt = '%(ld_14dayago)s'
                where b.churn_type = 'day' and b.days = 14
            ) a
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        ) d
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """  -- 每日(30)流失 用户资产
        insert into table stage.user_loss_gamecoin_property_merge_tmp
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum,
            0 f2eduserloss, 0 f5eduserloss, 0 f7eduserloss, 0 f14eduserloss, count(fuid) f30eduserloss
        from (
            select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, %(fnum)s, a.fuid fuid
            from (
                select b.fbpid, b.fuid, nvl(p.user_gamecoins_num, 0) user_gamecoins_num
                from stage.churn_user_mid b
                left join stage.pb_gamecoins_stream_mid p
                    on b.fbpid = p.fbpid and b.fuid = p.fuid and p.dt = '%(ld_30dayago)s'
                where b.churn_type = 'day' and b.days = 30
            ) a
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        ) d
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """   -- 把临时表数据处理后弄到正式表上
        insert overwrite table analysis.user_loss_gamecoin_property
        partition( dt="%(ld_daybegin)s" )
            select "%(ld_daybegin)s" fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                fnum,
                null fname,
                null f1dlossusernum,
                null f3dlossusernum,
                null f7dlossusernum,
                null f14dlossusernum,
                null f30dlossusernum,
                sum(f2eduserloss) f2eduserloss,
                sum(f5eduserloss) f5eduserloss,
                sum(f7eduserloss) f7eduserloss,
                sum(f14eduserloss) f14eduserloss,
                sum(f30eduserloss) f30eduserloss
            from stage.user_loss_gamecoin_property_merge_tmp
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fnum
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
a = agg_churn_user_property_data(statDate)
a()
