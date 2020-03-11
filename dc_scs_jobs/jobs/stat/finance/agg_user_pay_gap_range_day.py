#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_pay_gap_range_day(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_pay_gap_range_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fversionfsk             bigint,
            fterminalfsk            bigint,
            fdaygap                 bigint,
            fregusercnt             bigint,                 --当天首付费用户距离注册日期的间隔天数分布
            fgapusercnt             bigint,
            f2gapusercnt            bigint,
            f7dregusercnt           bigint,                  --新增用户的 最近7天的首付费人数
            f7dgapusercnt           bigint,
            f7d2gapusercnt          bigint,
            f30dregusercnt          bigint,                 --新增用户的 最近30天的首付费人数
            f30dgapusercnt          bigint,
            f30d2gapusercnt         bigint
        )
        partitioned by (dt date);

        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        # self.hq.debug = 1

        hql_list = []

        hql = """
        use stage;

        drop table if exists stage.user_pay_gap_range_fct_reg_%(num_begin)s;
        create table if not exists stage.user_pay_gap_range_fct_reg_%(num_begin)s as
        select "%(ld_begin)s" fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fdaygap,
               count(distinct case when d1 = 1 then fplatform_uid end) fregusercnt,
               0 fgapusercnt,
               0 f2gapusercnt,
               count(distinct case when d7 = 1 then fplatform_uid end) f7dregusercnt,
               0 f7dgapusercnt,
               0 f7d2gapusercnt,
               count(distinct case when d30 = 1 then fplatform_uid end) f30dregusercnt,
               0 f30dgapusercnt,
               0 f30d2gapusercnt
          from (select ps.fbpid,
                       ps.fplatform_uid,
                       case when ps.fisrtdate >= "%(ld_begin)s" then 1 else 0 end d1,
                       case when ps.fisrtdate >= date_sub("%(ld_begin)s", 6) then 1 else 0 end d7,
                       case when ps.fisrtdate >= date_sub("%(ld_begin)s", 29) then 1 else 0 end d30,
                       min(datediff(ps.fisrtdate, coalesce(ud.dt, cast('2001-01-01' as date)))) fdaygap
                  from (select fbpid, fplatform_uid, min(dt) fisrtdate
                          from stage.payment_stream_stg
                         where dt < "%(ld_end)s"
                           and fplatform_uid is not null
                           and fplatform_uid != ''
                         group by fbpid, fplatform_uid) ps
                  left outer join stage.user_dim ud
                    on ps.fbpid = ud.fbpid
                   and ps.fplatform_uid = ud.fplatform_uid
                   and ud.dt < "%(ld_end)s"
                 where ps.fisrtdate >= date_sub("%(ld_begin)s", 29)
                 group by ps.fbpid,
                          ps.fplatform_uid,
                          case when ps.fisrtdate >= "%(ld_begin)s" then 1 else 0 end,
                          case when ps.fisrtdate >= date_sub("%(ld_begin)s", 6) then 1 else 0 end,
                          case when ps.fisrtdate >= date_sub("%(ld_begin)s", 29) then 1 else 0 end
               ) t
          join analysis.bpid_platform_game_ver_map b
            on t.fbpid = b.fbpid
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap;

        """ % self.hql_dict
        hql_list.append( hql )

        #gap 1day
        hql = """
        drop table if exists stage.user_pay_gap_tmp_%(num_begin)s;
        create table if not exists stage.user_pay_gap_tmp_%(num_begin)s as
        select fbpid, fplatform_uid, f1, f7, f30, fdate, frank
          from (select ps.fbpid,
                       ps.fplatform_uid,
                       f1,
                       f7,
                       f30,
                       ps.dt fdate,
                       row_number() over(partition by ps.fbpid, ps.fplatform_uid order by ps.dt desc) frank
                  from (select dt,fbpid,fplatform_uid
                          from stage.payment_stream_stg
                         where dt < "%(ld_end)s"
                         group by dt,fbpid,fplatform_uid) ps
                  join (
                        select fbpid,
                               fplatform_uid,
                               case when max(dt) >= "%(ld_begin)s"
                                     and min(dt) < "%(ld_begin)s"
                                    then 1 else 0 end f1,
                               case when max(dt) >= date_sub("%(ld_begin)s", 6)
                                     and min(dt) < date_sub("%(ld_begin)s", 6)
                                    then 1 else 0 end f7,
                               case when max(dt) >= date_sub("%(ld_begin)s", 29)
                                     and min(dt) < date_sub("%(ld_begin)s", 29)
                                    then 1 else 0 end f30
                          from stage.payment_stream_stg
                         where dt < "%(ld_end)s"
                      group by fbpid, fplatform_uid
                       ) p
                    on ps.fbpid = p.fbpid
                   and ps.fplatform_uid = p.fplatform_uid
               ) t
         where frank <= 2;

        drop table if exists stage.user_pay_gap_range_fct_gap_%(num_begin)s;
        create table if not exists stage.user_pay_gap_range_fct_gap_%(num_begin)s as
        select fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fdaygap,
               0 fregusercnt,
               count(distinct case when f1=1 then fplatform_uid end) fgapusercnt,
               0 f2gapusercnt,
               0 f7dregusercnt,
               count(distinct case when f7=1 then fplatform_uid end) f7dgapusercnt,
               0 f7d2gapusercnt,
               0 f30dregusercnt,
               count(distinct case when f30=1 then fplatform_uid end) f30dgapusercnt,
               0 f30d2gapusercnt
          from (select  fbpid,
                        fplatform_uid,
                        f1,
                        f7,
                        f30,
                        datediff(max(fdate), min(fdate)) fdaygap
                   from user_pay_gap_tmp_%(num_begin)s
                  group by fbpid, fplatform_uid, f1, f7, f30
                ) t
          join analysis.bpid_platform_game_ver_map b
            on t.fbpid = b.fbpid
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap
        """% self.hql_dict
        hql_list.append( hql )

        # 2gap 1day
        hql = """
        drop table if exists stage.user_pay_gap_range_fct_2gap_1_%(num_begin)s;
        create table if not exists stage.user_pay_gap_range_fct_2gap_1_%(num_begin)s as
          select fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 datediff(payday, firstpayday) fdaygap,
                 0 fregusercnt,
                 0 fgapusercnt,
                 count(a.fplatform_uid) f2gapusercnt,
                 0 f7dregusercnt,
                 0 f7dgapusercnt,
                 0 f7d2gapusercnt,
                 0 f30dregusercnt,
                 0 f30dgapusercnt,
                 0 f30d2gapusercnt
            from (select fbpid, fplatform_uid, min(dt) payday
                    from stage.payment_stream_stg
                   where dt = '%(ld_begin)s'
                   group by fbpid, fplatform_uid ) a
            join (select fbpid, fplatform_uid, max(dt) firstpayday
                    from stage.payment_stream_stg
                   where dt < '%(ld_begin)s'
                   group by fbpid, fplatform_uid
                  having count(1) = 1) b
              on a.fbpid = b.fbpid
             and a.fplatform_uid = b.fplatform_uid
            join analysis.bpid_platform_game_ver_map bpm
              on a.fbpid = bpm.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, datediff(payday, firstpayday)
        """ % self.hql_dict
        hql_list.append( hql )

        # 2gap 7day
        hql = """
        drop table if exists stage.user_pay_gap_range_fct_2gap_7_%(num_begin)s;
        create table if not exists stage.user_pay_gap_range_fct_2gap_7_%(num_begin)s as
          select fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 datediff(payday, firstpayday) fdaygap,
                 0 fregusercnt,
                 0 fgapusercnt,
                 0 f2gapusercnt,
                 0 f7dregusercnt,
                 0 f7dgapusercnt,
                 count(a.fplatform_uid) f7d2gapusercnt,
                 0 f30dregusercnt,
                 0 f30dgapusercnt,
                 0 f30d2gapusercnt
            from (select fbpid, fplatform_uid, min(dt) payday
                    from stage.payment_stream_stg
                   where dt >= date_sub('%(ld_begin)s', 6)
                     and dt < "%(ld_end)s"
                   group by fbpid, fplatform_uid ) a
            join (select fbpid, fplatform_uid, max(dt) firstpayday
                    from stage.payment_stream_stg
                   where dt < date_sub('%(ld_begin)s', 6)
                   group by fbpid, fplatform_uid
                  having count(1) = 1) b
              on a.fbpid = b.fbpid
             and a.fplatform_uid = b.fplatform_uid
            join analysis.bpid_platform_game_ver_map bpm
              on a.fbpid = bpm.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, datediff(payday, firstpayday)
        """ % self.hql_dict
        hql_list.append( hql )

        # 2gap 30day
        hql = """
        drop table if exists stage.user_pay_gap_range_fct_2gap_30_%(num_begin)s;
        create table if not exists stage.user_pay_gap_range_fct_2gap_30_%(num_begin)s as
          select fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 datediff(payday, firstpayday) fdaygap,
                 0 fregusercnt,
                 0 fgapusercnt,
                 0 f2gapusercnt,
                 0 f7dregusercnt,
                 0 f7dgapusercnt,
                 0 f7d2gapusercnt,
                 0 f30dregusercnt,
                 0 f30dgapusercnt,
                 count(a.fplatform_uid) f30d2gapusercnt
            from (select fbpid, fplatform_uid, min(dt) payday
                    from stage.payment_stream_stg
                   where dt >= date_sub('%(ld_begin)s', 29)
                     and dt < "%(ld_end)s"
                   group by fbpid, fplatform_uid ) a
            join (select fbpid, fplatform_uid, max(dt) firstpayday
                    from stage.payment_stream_stg
                   where dt < date_sub('%(ld_begin)s', 29)
                   group by fbpid, fplatform_uid
                  having count(1) = 1) b
              on a.fbpid = b.fbpid
             and a.fplatform_uid = b.fplatform_uid
            join analysis.bpid_platform_game_ver_map bpm
              on a.fbpid = bpm.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, datediff(payday, firstpayday)
        """ % self.hql_dict
        hql_list.append( hql )

        # 合并结果
        hql = """
        insert overwrite table analysis.user_pay_gap_range_fct partition(dt="%(ld_begin)s")
        select "%(ld_begin)s" fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fdaygap,
               max(fregusercnt) fregusercnt,
               max(fgapusercnt) fgapusercnt,
               max(f2gapusercnt) f2gapusercnt,
               max(f7dregusercnt) f7dregusercnt,
               max(f7dgapusercnt) f7dgapusercnt,
               max(f7d2gapusercnt) f7d2gapusercnt,
               max(f30dregusercnt) f30dregusercnt,
               max(f30dgapusercnt) f30dgapusercnt,
               max(f30d2gapusercnt) f30d2gapusercnt
          from (select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap, fregusercnt, fgapusercnt, f2gapusercnt, f7dregusercnt, f7dgapusercnt, f7d2gapusercnt, f30dregusercnt, f30dgapusercnt, f30d2gapusercnt
                  from stage.user_pay_gap_range_fct_reg_%(num_begin)s
                 where fregusercnt + f7dregusercnt + f30dregusercnt > 0
                union all
                select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap, fregusercnt, fgapusercnt, f2gapusercnt, f7dregusercnt, f7dgapusercnt, f7d2gapusercnt, f30dregusercnt, f30dgapusercnt, f30d2gapusercnt
                  from stage.user_pay_gap_range_fct_gap_%(num_begin)s
                 where fgapusercnt + f7dgapusercnt + f30dgapusercnt > 0
                union all
                select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap, fregusercnt, fgapusercnt, f2gapusercnt, f7dregusercnt, f7dgapusercnt, f7d2gapusercnt, f30dregusercnt, f30dgapusercnt, f30d2gapusercnt
                  from stage.user_pay_gap_range_fct_2gap_1_%(num_begin)s
                 where f2gapusercnt > 0
                union all
                select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap, fregusercnt, fgapusercnt, f2gapusercnt, f7dregusercnt, f7dgapusercnt, f7d2gapusercnt, f30dregusercnt, f30dgapusercnt, f30d2gapusercnt
                  from stage.user_pay_gap_range_fct_2gap_7_%(num_begin)s
                 where f7d2gapusercnt > 0
                union all
                select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fdaygap, fregusercnt, fgapusercnt, f2gapusercnt, f7dregusercnt, f7dgapusercnt, f7d2gapusercnt, f30dregusercnt, f30dgapusercnt, f30d2gapusercnt
                  from stage.user_pay_gap_range_fct_2gap_30_%(num_begin)s
                 where f30d2gapusercnt > 0
               ) t
      group by fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fdaygap
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists stage.user_pay_gap_tmp_%(num_begin)s;
        drop table if exists stage.user_pay_gap_range_fct_reg_%(num_begin)s;
        drop table if exists stage.user_pay_gap_range_fct_gap_%(num_begin)s;
        drop table if exists stage.user_pay_gap_range_fct_2gap_1_%(num_begin)s;
        drop table if exists stage.user_pay_gap_range_fct_2gap_7_%(num_begin)s;
        drop table if exists stage.user_pay_gap_range_fct_2gap_30_%(num_begin)s;

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
    a = agg_user_pay_gap_range_day(stat_date)
    a()
