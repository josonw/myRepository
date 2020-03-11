#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_mau_mpu_retained(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 昨天分区update pg,唯一值:fdate,fgamefsk,fterminalsysfsk,fdistrictfsk,fterminalfsk
        create table if not exists analysis.finance_mau_mpu_retained
        (
            fdate                       date,
            fgamefsk                    bigint,
            fgamename                   varchar(20),
            fterminalsysfsk             bigint,
            fterminalsysname            varchar(50),
            fdistrictfsk                bigint,
            fdistrictname               varchar(50),
            fterminalfsk                bigint,
            fterminalname               varchar(50),
            mau_retainuser              bigint,
            mau_lastmonth               bigint,
            mpu_retainuser              bigint,
            mpu_lastmonth               bigint
        )
        partitioned by (dt date);        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        # 月度-付费留存，活跃留存
        hql = """
        use analysis;
        alter table finance_mau_mpu_retained drop partition(dt="%(ld_month_begin)s");
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
          -- 活跃留存
          insert into table analysis.finance_mau_mpu_retained partition
            (dt = "%(ld_month_begin)s")
          select '%(ld_month_begin)s' fdate ,
                b.fgamefsk ,
                b.fgamename,
                b.fterminalsysfsk ,
                b.fterminalsysname ,
                b.fdistrictfsk,
                b.fdistrictname,
                b.fterminalfsk,
                b.fterminalname,
                count(fuid) mau_retainuser,
                null mau_lastmonth,
                null mpu_retainuser,
                null mpu_lastmonth
          from (
                select fbpid, fuid, max(flag1), max(flag2)
                from(
                   select fbpid, fuid, 1 flag1, 0 flag2
                     from stage.active_user_mid
                    where dt >= '%(ld_month_begin)s'
                      and dt < '%(ld_1month_after_begin)s'
                   union all
                   select fbpid, fuid, 0 flag1, 1 flag2
                     from stage.active_user_mid
                    where dt >= '%(ld_1month_ago_begin)s'
                      and dt < '%(ld_month_begin)s'
                ) a
                group by fbpid, fuid
                having max(flag1) = 1 and max(flag2) = 1
             ) a
             join analysis.bpid_platform_game_ver_map  b
             on a.fbpid = b.fbpid
          group by b.fgamefsk ,b.fgamename, b.fterminalsysfsk ,b.fterminalsysname,b.fdistrictfsk, b.fdistrictname,b.fterminalfsk, b.fterminalname
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 上个月活跃
          insert into table analysis.finance_mau_mpu_retained partition
            (dt = "%(ld_month_begin)s")
          select '%(ld_month_begin)s' fdate ,
                b.fgamefsk ,
                b.fgamename,
                b.fterminalsysfsk ,
                b.fterminalsysname ,
                b.fdistrictfsk,
                b.fdistrictname,
                b.fterminalfsk,
                b.fterminalname ,
                null mau_retainuser,
                count(fuid) mau_lastmonth,
                null mpu_retainuser,
                null mpu_lastmonth
          from (
               select distinct fbpid, fuid
                 from stage.active_user_mid
                where dt >= '%(ld_1month_ago_begin)s'
                  and dt < '%(ld_month_begin)s'
             ) a
             join analysis.bpid_platform_game_ver_map  b
             on a.fbpid = b.fbpid
            group by b.fgamefsk ,b.fgamename, b.fterminalsysfsk ,b.fterminalsysname,b.fdistrictfsk, b.fdistrictname ,b.fterminalfsk, b.fterminalname
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 付费留存
          insert into table analysis.finance_mau_mpu_retained partition
            (dt = "%(ld_month_begin)s")
          select '%(ld_month_begin)s' fdate ,
                b.fgamefsk ,
                b.fgamename,
                b.fterminalsysfsk ,
                b.fterminalsysname ,
                b.fdistrictfsk,
                b.fdistrictname,
                b.fterminalfsk,
                b.fterminalname ,
                null mau_retainuser,
                null mau_lastmonth,
                count(fplatform_uid) mpu_retainuser,
                null mpu_lastmonth
          from (
            select fbpid, fplatform_uid, max(flag1), max(flag2)
                from(
                   select fbpid, fplatform_uid, 1 flag1, 0 flag2
                   from stage.payment_stream_stg
                  where dt >= '%(ld_month_begin)s'
                    and dt < '%(ld_1month_after_begin)s'
                 union all
                 select fbpid, fplatform_uid, 0 flag1, 1 flag2
                   from stage.payment_stream_stg
                  where dt >= '%(ld_1month_ago_begin)s'
                    and dt < '%(ld_month_begin)s'
                ) a
                group by fbpid, fplatform_uid
                having max(flag1) = 1 and max(flag2) = 1
             ) a
             join analysis.bpid_platform_game_ver_map  b
             on a.fbpid = b.fbpid
            group by b.fgamefsk ,b.fgamename, b.fterminalsysfsk ,b.fterminalsysname,b.fdistrictfsk, b.fdistrictname ,b.fterminalfsk, b.fterminalname
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 上个月付费用户
          insert into table analysis.finance_mau_mpu_retained partition
            (dt = "%(ld_month_begin)s")
          select '%(ld_month_begin)s' fdate ,
                b.fgamefsk,
                b.fgamename,
                b.fterminalsysfsk ,
                b.fterminalsysname ,
                b.fdistrictfsk,
                b.fdistrictname,
                b.fterminalfsk,
                b.fterminalname ,
                null mau_retainuser,
                null mau_lastmonth,
                null mpu_retainuser,
                count(fplatform_uid) mpu_lastmonth
          from (
             select distinct fbpid, fplatform_uid
               from stage.payment_stream_stg
              where dt >= '%(ld_1month_ago_begin)s'
                and dt < '%(ld_month_begin)s'
             ) a
             join analysis.bpid_platform_game_ver_map  b
             on a.fbpid = b.fbpid
          group by b.fgamefsk ,b.fgamename, b.fterminalsysfsk ,b.fterminalsysname,b.fdistrictfsk, b.fdistrictname ,b.fterminalfsk, b.fterminalname
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.finance_mau_mpu_retained partition
            (dt = "%(ld_month_begin)s")
        select fdate,
                fgamefsk,
                fgamename,
                fterminalsysfsk,
                fterminalsysname,
                fdistrictfsk,
                fdistrictname,
                fterminalfsk,
                fterminalname,
                max(mau_retainuser)     mau_retainuser,
                max(mau_lastmonth)      mau_lastmonth,
                max(mpu_retainuser)     mpu_retainuser,
                max(mpu_lastmonth)      mpu_lastmonth
        from analysis.finance_mau_mpu_retained
        where dt = "%(ld_month_begin)s"
        group by fdate,
                fgamefsk,
                fgamename,
                fterminalsysfsk,
                fterminalsysname,
                fdistrictfsk,
                fdistrictname,
                fterminalfsk,
                fterminalname
        """ % self.hql_dict
        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_mau_mpu_retained(stat_date)
    a()
