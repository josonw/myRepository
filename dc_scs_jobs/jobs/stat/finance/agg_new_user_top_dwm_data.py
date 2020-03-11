#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经被注释取消
class agg_new_user_top_dwm_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_new_top_pay_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fuid                        bigint,
            fplatform_uid               string,
            ftoppay_cnt                 decimal(20,2),
            frank                       bigint,
            fdaypay                     decimal(20,2),
            factdays                    bigint,
            fgamecoins                  bigint,
            ftype                       bigint,
            fis_gameparty               bigint,
            fwin_gamecoins              bigint,
            flost_gamecoins             bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.user_new_top_pay_fct_tmp
        (
            fdate                       date,
            fbpid                       string,
            fuid                        bigint,
            fplatform_uid               string,
            ftoppay_cnt                 decimal(20,2),
            frank                       bigint,
            fdaypay                     decimal(20,2),
            factdays                    bigint,
            fgamecoins                  bigint,
            ftype                       bigint,
            fis_gameparty               bigint,
            fwin_gamecoins              bigint,
            flost_gamecoins             bigint
        );

        create table if not exists analysis.game_coin_type_dim
        (
            fsk                 bigint,
            fgamefsk            bigint,
            fcointype           varchar(50),
            fdirection          varchar(50),
            ftype               varchar(50),
            fname               varchar(200),
            fmemo               varchar(500),
            fis_pay             bigint,
            fis_gambling        bigint,
            fis_prop            bigint,
            fis_box             int,
            fis_circulate       int,
            fis_taifei          bigint
        );

        create table if not exists stage.user_gamecoins_stream_mid
        (
            fbpid   varchar(50),
            fuid    bigint,
            ftype   varchar(50),
            fact_id varchar(32),
            fnum    bigint,
            fcnt    bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        truncate table user_new_top_pay_fct_tmp
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_new_top_pay_fct_tmp
        select '%(stat_date)s' fdate,
            a.fbpid,
            a.fuid,
            a.fplatform_uid,
            a.aip ftoppay_cnt,
            a.rown frank,
            a.dip fdaypay,
            datediff('%(stat_date)s', max(b.dt)) factdays,
            sum(c.user_gamecoins) fgamecoins,
            1 ftype,
            null fis_gameparty,
            null fwin_gamecoins,
            null flost_gamecoins
        from
        (
            select a.fbpid, a.fplatform_uid, a.fuid, a.aip, a.rown, b.dip
            from
            (
                select a.fbpid, a.fplatform_uid, b.fuid, a.aip, a.rown
                from
                (
                    select *
                    from
                    (
                        select a.fbpid, a.fplatform_uid,
                            round(sum(a.fcoins_num * a.frate), 2) aip,
                            row_number() over(partition by a.fbpid order by round(sum(a.fcoins_num * a.frate), 2) desc) rown
                        from stage.payment_stream_stg a
                        where a.dt = '%(stat_date)s'
                        -- group by a.fbpid, a.fplatform_uid
                    ) a
                    where a.rown <= 100
                ) a
                left join
                (
                    select a.fbpid, a.fuid, a.fplatform_uid
                    from stage.user_pay_info a
                    where a.dt = '%(stat_date)s'
                    group by a.fbpid, a.fuid, a.fplatform_uid
                ) b
                on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
            ) a
            left join
            (
                select a.fbpid, a.fplatform_uid, round(sum(a.fcoins_num * a.frate), 2) dip
                from stage.payment_stream_stg a
                where a.dt = '%(stat_date)s'
                group by a.fbpid, a.fplatform_uid
            ) b
            on a.fbpid = b.fbpid and a.fplatform_uid = b.fplatform_uid
        ) a
        left join stage.active_user_mid b
        on a.fbpid = b.fbpid and a.fuid = b.fuid and b.dt = '%(stat_date)s'
        left join
        (
            select *
            from
            (
                select fbpid,
                fplatform_uid,
                user_gamecoins,
                row_number() over(partition by fbpid, fplatform_uid order by flogin_at) rown
                from stage.user_login_stg
                where dt = '%(stat_date)s'
            ) a
            where rown <= 1
        ) c
        on a.fbpid = c.fbpid and a.fplatform_uid = c.fplatform_uid
        group by a.fbpid, a.fplatform_uid, a.fuid, a.aip, a.rown, a.dip
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        --日top用户输赢钱
        insert into table analysis.user_new_top_pay_fct_tmp
        select '%(stat_date)s' fdate,
                a.fbpid,
                a.fuid,
                a.fplatform_uid,
                null ftoppay_cnt,
                null frank,
                null fdaypay,
                null factdays,
                null fgamecoins,
                1 ftype,
                null fis_gameparty,
                sum(case when d.fdirection = 'IN' then  c.fnum end) fwin_gamecoins,
                sum(case when d.fdirection = 'OUT' then c.fnum end) flost_gamecoins
          from  analysis.user_new_top_pay_fct_tmp a
          join stage.user_gamecoins_stream_mid c
            on a.fbpid = c.fbpid
           and a.fuid = c.fuid
           and a.ftype = 1
          join analysis.game_coin_type_dim d
            on c.fact_id = d.ftype
         where d.fis_gambling = 1
         group by a.fdate, a.fbpid, a.fuid, a.fplatform_uid
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        --周top100
        insert into table analysis.user_new_top_pay_fct_tmp
          select '%(stat_date)s' fdate,
                a.fbpid,
                a.fuid,
                a.fplatform_uid,
                a.aip ftoppay_cnt,
                a.rown frank,
                a.dip fdaypay,
                datediff('%(stat_date)s', max(b.dt)) factdays,
                sum(c.user_gamecoins) fgamecoins,
                2 ftype,
                null fis_gameparty,
                null fwin_gamecoins,
                null flost_gamecoins
            from (select a.fbpid, a.fplatform_uid, a.fuid, a.aip, a.rown, b.dip
                    from (select a.fbpid, a.fplatform_uid, b.fuid, a.aip, a.rown
                            from (select *
                                    from (select a.fbpid,
                                                 a.fplatform_uid,
                                                 round(sum(a.fcoins_num * a.frate), 2) aip,
                                                 row_number() over(partition by a.fbpid order by round(sum(a.fcoins_num * a.frate), 2) desc) rown
                                            from stage.payment_stream_stg a
                                           where a.dt >= date_add('%(stat_date)s', -6)
                                           and a.dt < date_add('%(stat_date)s', 1)
                                           group by a.fbpid, a.fplatform_uid) a
                                   where a.rown <= 100) a
                            left join (select a.fbpid, a.fuid, a.fplatform_uid
                                        from stage.user_pay_info a
                                       where a.dt >= date_add('%(stat_date)s', -6)
                                        and a.dt < date_add('%(stat_date)s', 1)
                                       group by a.fbpid, a.fuid, a.fplatform_uid) b
                              on a.fbpid = b.fbpid
                             and a.fplatform_uid = b.fplatform_uid) a
                    left join (select a.fbpid,
                                     a.fplatform_uid,
                                     round(sum(a.fcoins_num * a.frate), 2) dip
                                from stage.payment_stream_stg a
                               where a.dt = '%(stat_date)s'
                               group by a.fbpid, a.fplatform_uid) b
                      on a.fbpid = b.fbpid
                     and a.fplatform_uid = b.fplatform_uid) a
          -- 活跃天数
            left join stage.active_user_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt >= date_add('%(stat_date)s', -6)
             and b.dt < date_add('%(stat_date)s', 1)
          -- 首次登录游戏币
            left join (select *
                         from (select fbpid,
                                      fplatform_uid,
                                      user_gamecoins,
                                      row_number() over(partition by fplatform_uid order by flogin_at) rown
                                 from stage.user_login_stg
                                where dt = '%(stat_date)s') a
                        where rown <= 1) c
              on a.fbpid = c.fbpid
             and a.fplatform_uid = c.fplatform_uid
           group by a.fbpid, a.fplatform_uid, a.fuid, a.aip, a.rown, a.dip
        """% self.hql_dict
        hql_list.append( hql )


        hql = """
        insert into table analysis.user_new_top_pay_fct_tmp
        select '%(stat_date)s' fdate,
                a.fbpid,
                a.fuid,
                a.fplatform_uid,
                null ftoppay_cnt,
                null frank,
                null fdaypay,
                null factdays,
                null fgamecoins,
                2 ftype,
                null fis_gameparty,
                sum(case when d.fdirection = 'IN' then  c.fnum end) fwin_gamecoins,
                sum(case when d.fdirection = 'OUT' then c.fnum end) flost_gamecoins
          from  analysis.user_new_top_pay_fct_tmp a
          join stage.user_gamecoins_stream_mid c
            on a.fbpid = c.fbpid
           and a.fuid = c.fuid
           and a.ftype = 2
          join analysis.game_coin_type_dim d
            on c.fact_id = d.ftype
         where d.fis_gambling = 1
         group by a.fdate, a.fbpid, a.fuid, a.fplatform_uid
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        --月top100
        insert into table analysis.user_new_top_pay_fct_tmp
          select '%(stat_date)s' fdate,
                a.fbpid,
                a.fuid,
                a.fplatform_uid,
                a.aip ftoppay_cnt,
                a.rown frank,
                a.dip fdaypay,
                datediff('%(stat_date)s', max(b.dt)) factdays,
                sum(c.user_gamecoins) fgamecoins,
                3 ftype,
                null fis_gameparty,
                null fwin_gamecoins,
                null flost_gamecoins
            from (select a.fbpid, a.fplatform_uid, a.fuid, a.aip, a.rown, b.dip
                    from (select a.fbpid, a.fplatform_uid, b.fuid, a.aip, a.rown
                            from (select *
                                    from (select a.fbpid,
                                                 a.fplatform_uid,
                                                 round(sum(a.fcoins_num * a.frate), 2) aip,
                                                 row_number() over(partition by a.fbpid order by round(sum(a.fcoins_num * a.frate), 2) desc) rown
                                            from stage.payment_stream_stg a
                                           where a.dt >= date_add('%(stat_date)s', -29)
                                           and a.dt < date_add('%(stat_date)s', 1)
                                           group by a.fbpid, a.fplatform_uid) a
                                   where a.rown <= 100) a
                            left join (select a.fbpid, a.fuid, a.fplatform_uid
                                        from stage.user_pay_info a
                                       where a.dt >= date_add('%(stat_date)s', -29)
                                       and a.dt < date_add('%(stat_date)s', 1)
                                       group by a.fbpid, a.fuid, a.fplatform_uid) b
                              on a.fbpid = b.fbpid
                             and a.fplatform_uid = b.fplatform_uid) a
                    left join (select a.fbpid,
                                     a.fplatform_uid,
                                     round(sum(a.fcoins_num * a.frate), 2) dip
                                from stage.payment_stream_stg a
                               where a.dt = '%(stat_date)s'
                               group by a.fbpid, a.fplatform_uid) b
                      on a.fbpid = b.fbpid
                     and a.fplatform_uid = b.fplatform_uid) a
          -- 活跃天数
            left join stage.active_user_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt >= date_add('%(stat_date)s', -29)
             and b.dt < date_add('%(stat_date)s', 1)
          -- 首次登录游戏币
            left join (select *
                         from (select fbpid,
                                      fplatform_uid,
                                      user_gamecoins,
                                      row_number() over(partition by fplatform_uid order by flogin_at) rown
                                 from stage.user_login_stg
                                where dt = '%(stat_date)s') a
                        where rown <= 1) c
              on a.fbpid = c.fbpid
             and a.fplatform_uid = c.fplatform_uid
           group by a.fbpid, a.fplatform_uid, a.fuid, a.aip, a.rown, a.dip;
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        --月top用户输赢钱
        insert into table analysis.user_new_top_pay_fct_tmp
          select '%(stat_date)s' fdate,
                a.fbpid,
                a.fuid,
                a.fplatform_uid,
                null ftoppay_cnt,
                null frank,
                null fdaypay,
                null factdays,
                null fgamecoins,
                3 ftype,
                null fis_gameparty,
                sum(case when d.fdirection = 'IN' then c.fnum end) fwin_gamecoins,
                sum(case when d.fdirection = 'OUT' then c.fnum end) flost_gamecoins
          from analysis.user_new_top_pay_fct_tmp a
          join stage.user_gamecoins_stream_mid c
            on a.fbpid = c.fbpid
           and a.fuid = c.fuid
           and a.ftype = 3
          join analysis.game_coin_type_dim d
            on c.fact_id = d.ftype
         where d.fis_gambling = 1
         group by a.fdate, a.fbpid, a.fuid, a.fplatform_uid
        """% self.hql_dict
        hql_list.append( hql )



        hql = """
        --top用户是否玩牌
        insert overwrite table analysis.user_new_top_pay_fct_tmp
        select '%(stat_date)s' fdate,
                a.fbpid,
                a.fuid,
                a.fplatform_uid,
                max(ftoppay_cnt)    ftoppay_cnt,
                max(frank)          frank,
                max(fdaypay)        fdaypay,
                max(factdays)       factdays,
                max(fgamecoins)     fgamecoins,
                a.ftype,
                1 fis_gameparty,
                max(fwin_gamecoins)     fwin_gamecoins,
                max(flost_gamecoins)    flost_gamecoins
        from analysis.user_new_top_pay_fct_tmp a
        join  stage.user_gameparty_info_mid c
          on a.fbpid = c.fbpid
         and a.fuid = c.fuid
         and c.dt = '%(stat_date)s'
        group by a.fbpid, a.fuid, a.fplatform_uid, a.ftype
        """% self.hql_dict
        hql_list.append( hql )


        # fuck fsk
        # 目前还不是怎么处理
        # 加入全局唯一fsk
        hql = """

        """ % self.hql_dict
        # hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_new_top_pay_fct partition
        (dt = "%(stat_date)s")
         select
            fdate,
            b.fgamefsk,
            b.fplatformfsk,
            b.fversionfsk,
            b.fterminalfsk,
            fuid,
            fplatform_uid,
            max(ftoppay_cnt)       ftoppay_cnt,
            max(frank)             frank,
            max(fdaypay)           fdaypay,
            max(factdays)          factdays,
            max(fgamecoins)        fgamecoins,
            ftype,
            max(fis_gameparty)     fis_gameparty,
            max(fwin_gamecoins)    fwin_gamecoins,
            max(flost_gamecoins)   flost_gamecoins
        from analysis.user_new_top_pay_fct_tmp a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        group by
            fdate,
            b.fgamefsk,
            b.fplatformfsk,
            b.fversionfsk,
            b.fterminalfsk,
            fuid,
            fplatform_uid,
            ftype
        """ % self.hql_dict
        hql_list.append( hql )

        # result = self.exe_hql_list(hql_list)
        result = 0
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
    a = agg_new_user_top_dwm_data(stat_date)
    a()
