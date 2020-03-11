#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_pay_retained_data(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求,30天
        create table if not exists analysis.user_pay_num_retained_fct
        (
            fdate                   date,
            fgamefsk                bigint,
            fplatformfsk            bigint,
            fversionfsk             bigint,
            fterminalfsk            bigint,
            f1daycnt                bigint,
            f2daycnt                bigint,
            f3daycnt                bigint,
            f4daycnt                bigint,
            f5daycnt                bigint,
            f6daycnt                bigint,
            f7daycnt                bigint,
            f14daycnt               bigint,
            f30daycnt               bigint
        )
        partitioned by (dt date);        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

        # 付费用户，N日付费留存
    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        hql = """
        use analysis;
        alter table user_pay_num_retained_fct add if not exists partition (dt='%(ld_begin)s')
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -1) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        count(distinct p.fplatform_uid) f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -1) dt
      from analysis.bpid_platform_game_ver_map bpm
      join(
        select fbpid, fplatform_uid, max(flag1), max(flag2) from (
            select fbpid, fplatform_uid, 1 flag1, 0 flag2
                from stage.payment_stream_stg
              where fdate >= date_add('%(stat_date)s', -1) and fdate < '%(stat_date)s'
              union all
              select fbpid, fplatform_uid, 0 flag1, 1 flag2
              from stage.payment_stream_stg
              where dt = '%(stat_date)s'
        ) a
        group by fbpid, fplatform_uid
        having max(flag1) = 1 and max(flag2) = 1
       ) p
       on bpm.fbpid = p.fbpid
       group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -2) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        count(distinct p.fplatform_uid) f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -2) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -2) and fdate < date_add('%(stat_date)s', -1)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -3) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        count(distinct p.fplatform_uid) f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -3) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -3) and fdate < date_add('%(stat_date)s', -2)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -4) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        count(distinct p.fplatform_uid) f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -4) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -4) and fdate < date_add('%(stat_date)s', -3)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
                """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -5) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        count(distinct p.fplatform_uid) f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -5) dt
        from analysis.bpid_platform_game_ver_map bpm
        join (
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                  from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -5) and fdate < date_add('%(stat_date)s', -4)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            )a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
        ) p
        on bpm.fbpid = p.fbpid
        group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -6) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        count(distinct p.fplatform_uid) f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -6) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -6) and fdate < date_add('%(stat_date)s', -5)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -7) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        count(distinct p.fplatform_uid) f7daycnt,
        0 f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -7) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -7) and fdate < date_add('%(stat_date)s', -6)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -14) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        count(distinct p.fplatform_uid) f14daycnt,
        0 f30daycnt,
        date_add('%(stat_date)s', -14) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -14) and fdate < date_add('%(stat_date)s', -13)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert into table analysis.user_pay_num_retained_fct partition (dt)
        select date_add('%(stat_date)s', -30) fdate, bpm.fgamefsk, bpm.fplatformfsk , bpm.fversionfsk, bpm.fterminalfsk,
        0 f1daycnt,
        0 f2daycnt,
        0 f3daycnt,
        0 f4daycnt,
        0 f5daycnt,
        0 f6daycnt,
        0 f7daycnt,
        0 f14daycnt,
        count(distinct p.fplatform_uid) f30daycnt,
        date_add('%(stat_date)s', -30) dt
          from analysis.bpid_platform_game_ver_map bpm
          join(
            select fbpid, fplatform_uid, max(flag1), max(flag2) from (
                select fbpid, fplatform_uid, 1 flag1, 0 flag2
                    from stage.payment_stream_stg
                  where fdate >= date_add('%(stat_date)s', -30) and fdate < date_add('%(stat_date)s', -29)
                  union all
                  select fbpid, fplatform_uid, 0 flag1, 1 flag2
                  from stage.payment_stream_stg
                  where dt = '%(stat_date)s'
            ) a
            group by fbpid, fplatform_uid
            having max(flag1) = 1 and max(flag2) = 1
           ) p
           on bpm.fbpid = p.fbpid
           group by bpm.fplatformfsk, bpm.fgamefsk, bpm.fversionfsk, bpm.fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_pay_num_retained_fct partition (dt)
        select
        a.fdate,
        a.fgamefsk,
        a.fplatformfsk,
        a.fversionfsk,
        a.fterminalfsk,
        max(a.f1daycnt)       f1daycnt,
        max(a.f2daycnt)       f2daycnt,
        max(a.f3daycnt)       f3daycnt,
        max(a.f4daycnt)       f4daycnt,
        max(a.f5daycnt)       f5daycnt,
        max(a.f6daycnt)       f6daycnt,
        max(a.f7daycnt)       f7daycnt,
        max(a.f14daycnt)      f14daycnt,
        max(a.f30daycnt)      f30daycnt,
        a.dt dt
        from analysis.user_pay_num_retained_fct a
        where a.dt >= date_add('%(stat_date)s', -30)
          and a.dt < '%(ld_end)s'
        group by
            a.fdate,
            a.fgamefsk,
            a.fplatformfsk,
            a.fversionfsk,
            a.fterminalfsk,
            a.dt
        """ % self.hql_dict
        hql_list.append( hql )

        #加上昨天的分区,留存不会当天的数据
        hql = """
        use analysis;
        alter table user_pay_num_retained_fct add  if not exists partition (dt='%(stat_date)s')
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_pay_num_retained_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        f1daycnt,
        f2daycnt,
        f3daycnt,
        f4daycnt,
        f5daycnt,
        f6daycnt,
        f7daycnt,
        f14daycnt,
        f30daycnt
        from analysis.user_pay_num_retained_fct
        where dt >= date_add('%(stat_date)s', -30)
          and dt < '%(ld_end)s'

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
    a = agg_payment_pay_retained_data(stat_date)
    a()
