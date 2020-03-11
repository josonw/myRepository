#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_retained_reg_user_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_retained_rupt_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fruptcnt bigint,
                f1dusernum bigint,
                f7dusernum bigint,
                f30dusernum bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create table if not exists analysis.user_retained_login_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                flogincnt bigint,
                f1dusernum bigint,
                f7dusernum bigint,
                f30dusernum bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create table if not exists analysis.user_retained_grade_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fgrade bigint,
                fregcnt bigint,
                f1dusernum bigint,
                f7dusernum bigint,
                f30dusernum bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res



        hql = """create table if not exists analysis.user_retained_pay_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fispay bigint,
                fname varchar(20),
                f1dusernum bigint,
                f7dusernum bigint,
                f30dusernum bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        # 注意开启动态分区
        res = self.hq.exe_sql("""use stage;
            set hive.exec.dynamic.partition=true;
            """)
        if res != 0: return res

        hql = """
        drop table if exists stage.user_reg_retained_uid_tmp;
        create table stage.user_reg_retained_uid_tmp
        as
        select distinct b.dt dt, b.fbpid, b.fuid, datediff(a.dt, b.dt) retday
        from stage.active_user_mid a
        join stage.user_dim b
        on a.fbpid=b.fbpid
        and a.fuid=b.fuid
        and b.dt >= '%(ld_30dayago)s'
        and b.dt < '%(ld_dayend)s'
        where a.dt = '%(ld_daybegin)s';

        insert overwrite table analysis.user_retained_rupt_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 a.ruptcnt,
                 count(if(a.dt = '%(ld_1dayago)s', a.fuid, null)) f1dusernum,
                 count(if(a.dt = '%(ld_7dayago)s', a.fuid, null)) f7dusernum,
                 count(if(a.dt = '%(ld_30dayago)s', a.fuid, null)) f30dusernum
            from (select a.dt, a.fbpid, a.fuid, nvl(count(b.fuid), 0) ruptcnt
                    from stage.user_reg_retained_uid_tmp a
                    left outer join stage.user_bankrupt_stg b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and a.dt = b.dt
                   where a.dt='%(ld_1dayago)s'
                      or a.dt='%(ld_7dayago)s'
                      or a.dt='%(ld_30dayago)s'
                   group by a.dt, a.fbpid, a.fuid ) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by b.fplatformfsk,
                    b.fgamefsk,
                    b.fversionfsk,
                    b.fterminalfsk,
                    a.ruptcnt;

        insert overwrite table analysis.user_retained_login_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 a.logincnt,
                 count(if(a.dt = '%(ld_1dayago)s', a.fuid, null)) f1dusernum,
                 count(if(a.dt = '%(ld_7dayago)s', a.fuid, null)) f7dusernum,
                 count(if(a.dt = '%(ld_30dayago)s', a.fuid, null)) f30dusernum
            from (select a.dt, a.fbpid, a.fuid, nvl(count(b.fuid), 0) logincnt
                    from stage.user_reg_retained_uid_tmp a
                    left outer join stage.user_login_stg b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and a.dt = b.dt
                    where a.dt='%(ld_1dayago)s'
                       or a.dt='%(ld_7dayago)s'
                       or a.dt='%(ld_30dayago)s'
                   group by a.dt, a.fbpid, a.fuid ) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by b.fplatformfsk,
                    b.fgamefsk,
                    b.fversionfsk,
                    b.fterminalfsk,
                    a.logincnt;

        insert overwrite table analysis.user_retained_grade_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fgrade,
                 max(fregcnt) fregcnt,
                 max(f1dusernum) f1dusernum,
                 max(f7dusernum) f7dusernum,
                 max(f30dusernum) f30dusernum
            from (select b.fgamefsk,
                         b.fplatformfsk,
                         b.fversionfsk,
                         b.fterminalfsk,
                         a.fgrade,
                         0 fregcnt,
                         count(if(a.dt = '%(ld_1dayago)s', a.fuid, null)) f1dusernum,
                         count(if(a.dt = '%(ld_7dayago)s', a.fuid, null)) f7dusernum,
                         count(if(a.dt = '%(ld_30dayago)s', a.fuid, null)) f30dusernum
                    from (select a.dt, a.fbpid, a.fuid, max(nvl(flevel, 0)) fgrade
                            from stage.user_reg_retained_uid_tmp a
                            left outer join stage.user_grade_stg b
                              on a.fbpid = b.fbpid
                             and a.fuid = b.fuid
                             and a.dt = b.dt
                           where a.dt = '%(ld_1dayago)s'
                                 or a.dt ='%(ld_7dayago)s'
                              or a.dt ='%(ld_30dayago)s'
                           group by a.dt, a.fbpid, a.fuid) a
                    join analysis.bpid_platform_game_ver_map b
                      on a.fbpid = b.fbpid
                   group by b.fplatformfsk,
                            b.fgamefsk,
                            b.fversionfsk,
                            b.fterminalfsk,
                            a.fgrade
                  union all
                  select d.fgamefsk,
                         d.fplatformfsk,
                         d.fversionfsk,
                         d.fterminalfsk,
                         fgrade,
                         count(distinct t.fuid) fregcnt,
                         0 f1dusernum,
                         0 f7dusernum,
                         0 f30dusernum
                    from (select a.fbpid, a.fuid, max(nvl(flevel, 1)) fgrade
                            from stage.user_dim a
                            left outer join stage.user_grade_stg b
                              on a.fbpid = b.fbpid
                             and a.fuid = b.fuid
                             and b.dt = '%(ld_daybegin)s'
                           where a.dt = '%(ld_daybegin)s'
                           group by a.fbpid, a.fuid) t
                    join analysis.bpid_platform_game_ver_map d
                      on t.fbpid = d.fbpid
                   group by d.fplatformfsk,
                            d.fgamefsk,
                            d.fversionfsk,
                            d.fterminalfsk,
                            fgrade) tmp
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fgrade;

        insert overwrite table analysis.user_retained_pay_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 a.fispay,
                 if(a.fispay=1, '付费', '非付费') fname,
                 count(if(a.dt = '%(ld_1dayago)s', a.fuid, null)) f1dusernum,
                 count(if(a.dt = '%(ld_7dayago)s', a.fuid, null)) f7dusernum,
                 count(if(a.dt = '%(ld_30dayago)s', a.fuid, null)) f30dusernum
            from (select distinct a.dt, a.fbpid, a.fuid, if(b.fpay_at is null, 0, 1) fispay
                    from stage.user_reg_retained_uid_tmp a
                    left outer join stage.user_pay_info b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and a.dt = b.dt
                    where a.dt='%(ld_1dayago)s'
                       or a.dt='%(ld_7dayago)s'
                       or a.dt='%(ld_30dayago)s'
                ) a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by b.fplatformfsk,
                    b.fgamefsk,
                    b.fversionfsk,
                    b.fterminalfsk,
                    a.fispay,
                     if(a.fispay=1, '付费', '非付费');
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


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
a = agg_act_retained_reg_user_data(statDate)
a()
