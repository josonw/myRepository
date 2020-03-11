#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gamecoin_property_data(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.user_gamecoin_balance
        (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fnum bigint,
            fusernum int,
            fname varchar(50)
        )
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date,
            "ld_end": PublicFunc.add_days(self.stat_date, 1),
            "ld_90dayago": PublicFunc.add_days(self.stat_date, -90),
            "num_date": self.stat_date.replace("-", "")
        }

        hql = """ set hive.auto.convert.join=false; """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        #
        hql = """
        use stage;

        drop table if exists stage.active_gamecoins_tmp_%(num_date)s;

        -- 当日活跃，且有游戏币变化
        create table stage.active_gamecoins_tmp_%(num_date)s
        as
        select p.fbpid, p.fuid, p.user_gamecoins_num
        from stage.pb_gamecoins_stream_mid p
        join stage.active_user_mid a
            on p.fbpid = a.fbpid and p.fuid = a.fuid
            and a.dt = "%(ld_begin)s"
        where p.dt = "%(ld_begin)s";

        -- 当日活跃，没有游戏币变化的，取近90天里最后一次有游戏币变化的记录
        insert into table stage.active_gamecoins_tmp_%(num_date)s
        select t.fbpid, t.fuid, t.user_gamecoins_num
        from
        (
            select a.fbpid, a.fuid, p.user_gamecoins_num,
                row_number() over(partition by a.fbpid, a.fuid order by p.fdate desc, p.user_gamecoins_num desc) rown
            from
            (
                -- 当日活跃，但是没有游戏币变化记录
                select a.fbpid, a.fuid
                from stage.active_user_mid a
                left join stage.pb_gamecoins_stream_mid p
                    on a.fbpid = p.fbpid and a.fuid = p.fuid
                    and p.dt = "%(ld_begin)s"
                where a.dt = "%(ld_begin)s" and p.fbpid is null
            ) a
            join stage.pb_gamecoins_stream_mid p
                on a.fbpid = p.fbpid and a.fuid = p.fuid
            where p.dt >= '%(ld_90dayago)s' and p.dt < '%(ld_begin)s'
        ) t
        where t.rown = 1;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.user_gamecoin_balance partition(dt = "%(ld_begin)s")
        select "%(ld_begin)s" fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fnum,
            count(distinct fuid) fusernum, -- 多个bpid对应一个gpv是要去重
            fname
        from
        (
            select fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                a.fuid,
                case
                    when a.user_gamecoins_num <= 0 then 0
                    when a.user_gamecoins_num >= 1 and a.user_gamecoins_num < 5000 then 1
                    when a.user_gamecoins_num >= 5000 and a.user_gamecoins_num < 10000 then 5000
                    when a.user_gamecoins_num >= 10000 and a.user_gamecoins_num < 50000 then 10000
                    when a.user_gamecoins_num >= 50000 and a.user_gamecoins_num < 100000 then 50000
                    when a.user_gamecoins_num >= 100000 and a.user_gamecoins_num < 500000 then 100000
                    when a.user_gamecoins_num >= 500000 and a.user_gamecoins_num < 1000000 then 500000
                    when a.user_gamecoins_num >= 1000000 and a.user_gamecoins_num < 5000000 then 1000000
                    when a.user_gamecoins_num >= 5000000 and a.user_gamecoins_num < 10000000 then 5000000
                    when a.user_gamecoins_num >= 10000000 and a.user_gamecoins_num < 50000000 then 10000000
                    when a.user_gamecoins_num >= 50000000 and a.user_gamecoins_num < 100000000 then 50000000
                    when a.user_gamecoins_num >= 100000000 and a.user_gamecoins_num < 1000000000 then 100000000
                else 1000000000 end fnum,
                case
                    when a.user_gamecoins_num <= 0 then '0'
                    when a.user_gamecoins_num >= 1 and a.user_gamecoins_num < 5000 then '1-5000'
                    when a.user_gamecoins_num >= 5000 and a.user_gamecoins_num < 10000 then '5000-1万'
                    when a.user_gamecoins_num >= 10000 and a.user_gamecoins_num < 50000 then '1万-5万'
                    when a.user_gamecoins_num >= 50000 and a.user_gamecoins_num < 100000 then '5万-10万'
                    when a.user_gamecoins_num >= 100000 and a.user_gamecoins_num < 500000 then '10万-50万'
                    when a.user_gamecoins_num >= 500000 and a.user_gamecoins_num < 1000000 then '50万-100万'
                    when a.user_gamecoins_num >= 1000000 and a.user_gamecoins_num < 5000000 then '100万-500万'
                    when a.user_gamecoins_num >= 5000000 and a.user_gamecoins_num < 10000000 then '500万-1000万'
                    when a.user_gamecoins_num >= 10000000 and a.user_gamecoins_num < 50000000 then '1000万-5000万'
                    when a.user_gamecoins_num >= 50000000 and a.user_gamecoins_num < 100000000 then '5000万-1亿'
                    when a.user_gamecoins_num >= 100000000 and a.user_gamecoins_num < 1000000000 then '1亿-10亿'
                else '10亿+' end fname
            from stage.active_gamecoins_tmp_%(num_date)s a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
        ) t
        group by fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fnum,
            fname;
        """ % args_dic

        hql += """
        drop table if exists stage.active_gamecoins_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = agg_gamecoin_property_data()
    a()
