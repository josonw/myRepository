#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_reflux_user_mid(BaseStat):
    """
    建立（周期，每日流失）回流用户中间表。

    回流：N天流失用户，近N天没有活跃，当天活跃了
    流失回流、N日回流用户，N天前那天活跃，近N天没有活跃，当天活跃了
    """
    def create_tab(self):
        hql = """
        create external table if not exists stage.reflux_user_mid
        (
            fdate        date,
            fbpid        string,
            fuid         bigint,
            reflux       bigint,
            reflux_type  string
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/reflux_user_mid'
        """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {
            "num_begin": statDate.replace('-', '')
        }
        query.update(date)

        hql = """
        use stage;
        set hive.exec.dynamic.partition.mode=nonstrict;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        # 创建临时表
        hql ="""
        drop table if exists stage.load_reflux_user_mid_1_%(num_begin)s;

        create table stage.load_reflux_user_mid_1_%(num_begin)s
        as
        select fdate, fbpid, fuid, flast_date
        from
        (
            -- 当天的活跃用户，如果最近30天活跃，取最近活跃时间，否则取31天之前那天
            -- 为用户的最后活跃时间
            select '%(ld_daybegin)s' fdate, fbpid, fuid,
                max(flast_date) flast_date, max(is_ok) is_ok
            from
            (
                select fbpid, fuid, '%(ld_31dayago)s' flast_date, 1 is_ok
                from stage.active_user_mid
                where dt =  '%(ld_daybegin)s'

                union all

                select fbpid, fuid, dt flast_date, 0 is_ok
                from stage.active_user_mid
                where dt >= '%(ld_30dayago)s' and dt < '%(ld_daybegin)s'

                union all

                select fbpid, fuid, dt flast_date, 0 is_ok
                from stage.user_dim
                where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
            ) t
            group by fbpid, fuid
        ) tt
        where is_ok = 1;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table stage.reflux_user_mid
        partition (dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fbpid, fuid, reflux, 'cycle' reflux_type
        from
        (
            select fbpid, fuid, 2 reflux
            from stage.load_reflux_user_mid_1_%(num_begin)s
            where flast_date < '%(ld_2dayago)s'

            union all

            select fbpid, fuid, 5 reflux
            from stage.load_reflux_user_mid_1_%(num_begin)s
            where flast_date < '%(ld_5dayago)s'

            union all

            select fbpid, fuid, 7 reflux
            from stage.load_reflux_user_mid_1_%(num_begin)s
            where flast_date < '%(ld_7dayago)s'

            union all

            select fbpid, fuid, 14 reflux
            from stage.load_reflux_user_mid_1_%(num_begin)s
            where flast_date < '%(ld_14dayago)s'

            union all

            select fbpid, fuid, 30 reflux
            from stage.load_reflux_user_mid_1_%(num_begin)s
            where flast_date < '%(ld_30dayago)s'
        ) tmp ;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists stage.load_reflux_user_mid_2_%(num_begin)s;

        create table if not exists stage.load_reflux_user_mid_2_%(num_begin)s
        as
        select fbpid, fuid,
            max(begin_day) begin_day,
            max(dayago2_h) dayago2_h,
            max(dayago5_h) dayago5_h,
            max(dayago7_h) dayago7_h,
            max(dayago14_h) dayago14_h,
            max(dayago30_h) dayago30_h,
            max(dayago2) dayago2,
            max(dayago5) dayago5,
            max(dayago7) dayago7,
            max(dayago14) dayago14,
            max(dayago30) dayago30
        from
        (
            select fbpid, fuid,
                if(a.dt = '%(ld_daybegin)s' , 1, 0 ) begin_day,
                if(a.dt >= '%(ld_30dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 ) dayago30_h,
                if(a.dt >= '%(ld_14dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 ) dayago14_h,
                if(a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 ) dayago7_h,
                if(a.dt >= '%(ld_5dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 ) dayago5_h,
                if(a.dt >= '%(ld_2dayago)s' and a.dt < '%(ld_daybegin)s' , 1, 0 ) dayago2_h,
                if(a.dt >= '%(ld_3dayago)s' and a.dt < '%(ld_2dayago)s' , 1, 0 ) dayago2,
                if(a.dt >= '%(ld_6dayago)s' and a.dt < '%(ld_5dayago)s' , 1, 0 ) dayago5,
                if(a.dt >= '%(ld_8dayago)s' and a.dt < '%(ld_7dayago)s' , 1, 0 ) dayago7,
                if(a.dt >= '%(ld_15dayago)s' and a.dt < '%(ld_14dayago)s' , 1, 0 ) dayago14,
                if(a.dt >= '%(ld_31dayago)s' and a.dt < '%(ld_30dayago)s' , 1, 0 ) dayago30
           from stage.active_user_mid a
          where dt >= '%(ld_31dayago)s' and dt < '%(ld_dayend)s'
        ) tmp
        group by fbpid, fuid;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        -- 现在进行流失回流用户插入。
        insert into table stage.reflux_user_mid
        partition(dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate, fbpid, fuid, reflux, 'day' reflux_type
        from
        (
            -- 当天活跃，30天之前活跃，两者之间不活跃
            select fbpid, fuid, 30 reflux
            from load_reflux_user_mid_2_%(num_begin)s
            where begin_day=1 and dayago30_h=0 and dayago30=1

            union all

            select fbpid, fuid, 14 reflux
            from load_reflux_user_mid_2_%(num_begin)s
            where begin_day=1 and dayago14_h=0 and dayago14=1

            union all

            select fbpid, fuid, 7 reflux
            from load_reflux_user_mid_2_%(num_begin)s
            where begin_day=1 and dayago7_h=0 and dayago7=1

            union all

            select fbpid, fuid, 5 reflux
            from load_reflux_user_mid_2_%(num_begin)s
            where begin_day=1 and dayago5_h=0 and dayago5=1

            union all

            select fbpid, fuid, 2 reflux
            from load_reflux_user_mid_2_%(num_begin)s
            where begin_day=1 and dayago2_h=0 and dayago2=1
        ) tmp;
         """ % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 用完将临时表清理掉
        hql ="""
        drop table if exists stage.load_reflux_user_mid_1_%(num_begin)s;

        drop table if exists stage.load_reflux_user_mid_2_%(num_begin)s;
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
a = load_reflux_user_mid(statDate)
a()
