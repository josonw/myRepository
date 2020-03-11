#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class ddzjinbiqujian(BaseStat):
    """该表中携带博雅币的用户数区间粒度太大，如需要细粒度可参考ddzjinbiqujianthin.py脚本"""
    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
              create table if not exists analysis.ddz_jinbi_fct(
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fcnt bigint,        --携带游戏币登陆的用户数
                flft bigint,
                frgt bigint,
                fregcnt bigint,
                fbrcnt bigint,
                fpaycnt bigint,
                fbankcnt bigint,     --保险箱  金币数在该区间内的用户数
                ftotalcnt bigint,      --保险箱+携带  金币数在该区间内的用户数
                fbycnt bigint)       --携带博雅币的用户数
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
            "num_date": self.stat_date.replace("-", "")
        }


        hql = """
              use stage;
              drop table if exists stage.jw_ddz_user_tmp_%(num_date)s;
              create table stage.jw_ddz_user_tmp_%(num_date)s as
              select b.fgamefsk fgamefsk,
                     b.fplatformfsk fplatformfsk,
                     b.fversionfsk fversionfsk,
                     b.fterminalfsk fterminalfsk,
                     t.fuid fuid,
                     t.user_gamecoins user_gamecoins,
                     t.bank_gamecoins bank_gamecoins,
                     t.user_gamecoins + t.bank_gamecoins total_gamecoins,
                     t.user_bycoins
                from analysis.bpid_platform_game_ver_map b
                join(
                  select fbpid, fuid, user_gamecoins,bank_gamecoins,user_bycoins
                  from (
                      select fbpid, fuid, user_gamecoins,bank_gamecoins,user_bycoins,
                          row_number() over(partition by fbpid, fuid order by flogin_at, user_gamecoins) rown
                      from stage.user_login_stg
                      where dt = "%(ld_begin)s"
                  ) ss
                  where ss.rown = 1
                ) t
                  on t.fbpid = b.fbpid
               ;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              use stage;

              drop table if exists stage.ddz_jinbi_cnt_tmp_%(num_date)s;

              create table stage.ddz_jinbi_cnt_tmp_%(num_date)s as
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     count(a.fuid) fcnt,
                     0 fbankcnt,
                     0 ftotalcnt,
                     0 fbycnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.jw_ddz_user_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.user_gamecoins >= b.flft
                 and a.user_gamecoins < b.frgt
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        b.flft,
                        b.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
              use stage;
              insert into table stage.ddz_jinbi_cnt_tmp_%(num_date)s
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     0 fcnt,
                     count(a.fuid) fbankcnt,
                     0 ftotalcnt,
                     0 fbycnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.jw_ddz_user_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.bank_gamecoins >= b.flft
                 and a.bank_gamecoins < b.frgt
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        b.flft,
                        b.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
              use stage;
              insert into table stage.ddz_jinbi_cnt_tmp_%(num_date)s
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     0 fcnt,
                     0 fbankcnt,
                     count(a.fuid) ftotalcnt,
                     0 fbycnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.jw_ddz_user_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.total_gamecoins >= b.flft
                 and a.total_gamecoins < b.frgt
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        b.flft,
                        b.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
              use stage;
              insert into table stage.ddz_jinbi_cnt_tmp_%(num_date)s
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     0 fcnt,
                     0 fbankcnt,
                     0 ftotalcnt,
                     count(a.fuid) fbycnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.jw_ddz_user_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.user_bycoins >= b.flft
                 and a.user_bycoins < b.frgt
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        b.flft,
                        b.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
              use stage;
              insert overwrite table stage.ddz_jinbi_cnt_tmp_%(num_date)s
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     sum(fcnt) fcnt,
                     sum(fbankcnt) fbankcnt,
                     sum(ftotalcnt) ftotalcnt,
                     sum(fbycnt) fbycnt,
                     a.flft flft,
                     a.frgt frgt
                from stage.ddz_jinbi_cnt_tmp_%(num_date)s a
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        a.flft,
                        a.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

############################################################################################
#

        hql = """
              use stage;

              drop table if exists stage.jw_ddz_bankrupt_user_tmp_%(num_date)s;

              create table stage.jw_ddz_bankrupt_user_tmp_%(num_date)s as
              select t.fdate fdate,
                     t.fgamefsk fgamefsk,
                     t.fplatformfsk fplatformfsk,
                     t.fversionfsk fversionfsk,
                     t.fterminalfsk fterminalfsk,
                     t.fuid fuid,
                     t.frupt_num frupt_num,
                     j.user_gamecoins user_gamecoins
                from (
                      select u.dt fdate,
                             b.fgamefsk fgamefsk,
                             b.fplatformfsk fplatformfsk,
                             b.fversionfsk fversionfsk,
                             b.fterminalfsk fterminalfsk,
                             u.fuid fuid,
                             count(1) frupt_num
                        from stage.user_bankrupt_stg u
                        join analysis.bpid_platform_game_ver_map b
                          on u.fbpid = b.fbpid
                       where u.dt = "%(ld_begin)s"
                    group by u.dt,
                             b.fgamefsk,
                             b.fplatformfsk,
                             b.fversionfsk,
                             b.fterminalfsk,
                             u.fuid
                     ) t
           left join stage.jw_ddz_user_tmp_%(num_date)s j
                  on t.fgamefsk = j.fgamefsk
                 and t.fplatformfsk = j.fplatformfsk
                 and t.fversionfsk = j.fversionfsk
                 and t.fuid = j.fuid
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              use stage;

              drop table if exists stage.ddz_jinbi_brcnt_tmp_%(num_date)s;

              create table stage.ddz_jinbi_brcnt_tmp_%(num_date)s as
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     count(a.fuid) fbrcnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.jw_ddz_bankrupt_user_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.user_gamecoins >= b.flft
                 and a.user_gamecoins < b.frgt
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        b.flft,
                        b.frgt;
              """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              use stage;

              drop table if exists stage.jw_ddz_pay_user_tmp_%(num_date)s;

              create table stage.jw_ddz_pay_user_tmp_%(num_date)s as
              select "%(ld_begin)s" fdate,
                     b.fgamefsk fgamefsk,
                     b.fplatformfsk fplatformfsk,
                     b.fversionfsk fversionfsk,
                     b.fterminalfsk fterminalfsk,
                     pu.fuid fuid,
                     p.user_gamecoins_num user_gamecoins_num
                from stage.pb_gamecoins_stream_mid p
                join stage.pay_user_mid pu
                  on p.fbpid = pu.fbpid
                 and p.fuid = pu.fuid
                join analysis.bpid_platform_game_ver_map b
                  on p.fbpid = b.fbpid
               where p.dt = "%(ld_begin)s";
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              use stage;

              drop table if exists stage.ddz_jinbi_paycnt_tmp_%(num_date)s;

              create table stage.ddz_jinbi_paycnt_tmp_%(num_date)s as
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     count(a.fuid) fpaycnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.jw_ddz_pay_user_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.user_gamecoins_num >= b.flft
                 and a.user_gamecoins_num < b.frgt
            group by a.fgamefsk,
                     a.fplatformfsk,
                     a.fversionfsk,
                     a.fterminalfsk,
                     b.flft,
                     b.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
                insert overwrite table analysis.ddz_jinbi_fct
                  partition(dt="%(ld_begin)s")
                select coalesce(a.fdate, b.fdate, c.fdate) fdate,
                       coalesce(a.fgamefsk, b.fgamefsk, c.fgamefsk) fgamefsk,
                       coalesce(a.fplatformfsk, b.fplatformfsk, c.fplatformfsk) fplatformfsk,
                       coalesce(a.fversionfsk, b.fversionfsk, c.fversionfsk) fversionfsk,
                       coalesce(a.fterminalfsk, b.fterminalfsk, c.fterminalfsk) fterminalfsk,
                       nvl(a.fcnt, 0) fcnt,
                       coalesce(a.flft, b.flft, c.flft) flft,
                       coalesce(a.frgt, b.frgt, c.frgt) frgt,
                       case when a.fdate is null and b.fdate is null
                            then c.fpaycnt else 0 end fregcnt,
                       nvl(b.fbrcnt, 0) fbrcnt,
                       nvl(c.fpaycnt, 0) fpaycnt,
                       nvl(a.fbankcnt, 0) fbankcnt,
                       nvl(a.ftotalcnt, 0) ftotalcnt,
                       nvl(a.fbycnt, 0) fbycnt
                  from stage.ddz_jinbi_cnt_tmp_%(num_date)s a
       full outer join stage.ddz_jinbi_brcnt_tmp_%(num_date)s b
                    on a.fgamefsk = b.fgamefsk
                   and a.fplatformfsk = b.fplatformfsk
                   and a.fversionfsk = b.fversionfsk
                   and a.fdate = b.fdate
                   and a.flft = b.flft
                   and a.frgt = b.frgt
       full outer join stage.ddz_jinbi_paycnt_tmp_%(num_date)s c
                    on a.fgamefsk = c.fgamefsk
                   and a.fplatformfsk = c.fplatformfsk
                   and a.fversionfsk = c.fversionfsk
                   and a.fdate = c.fdate
                   and a.flft = c.flft
                   and a.frgt = c.frgt;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:return res


        # hql = """
        #       insert overwrite table analysis.ddz_jinbi_fct
        #       partition(dt="%(ld_begin)s")
        #           select fdate,
        #                  fgamefsk,
        #                  fplatformfsk,
        #                  fversionfsk,
        #                  fterminalfsk,
        #                  max(fcnt) fcnt,
        #                  flft,
        #                  frgt,
        #                  max(fregcnt) fregcnt,
        #                  max(fbrcnt) fbrcnt,
        #                  max(fpaycnt) fpaycnt
        #             from (
        #                   select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fcnt, flft, frgt, 0 fregcnt, 0 fbrcnt, 0 fpaycnt
        #                     from stage.ddz_jinbi_cnt_tmp_%(num_date)s
        #                   union all
        #                   select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, 0 fcnt, flft, frgt, 0 fregcnt, fbrcnt, 0 fpaycnt
        #                     from stage.ddz_jinbi_brcnt_tmp_%(num_date)s
        #                   union all
        #                   select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, 0 fcnt, flft, frgt, 0 fregcnt, 0 fbrcnt, fpaycnt
        #                     from stage.ddz_jinbi_paycnt_tmp_%(num_date)s
        #                  ) t
        #         group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, flft, frgt;
        #       """ % args_dic

        hql = """
        drop table if exists stage.jw_ddz_user_tmp_%(num_date)s;
        drop table if exists stage.ddz_jinbi_cnt_tmp_%(num_date)s;
        drop table if exists stage.jw_ddz_bankrupt_user_tmp_%(num_date)s;
        drop table if exists stage.ddz_jinbi_brcnt_tmp_%(num_date)s;
        drop table if exists stage.jw_ddz_pay_user_tmp_%(num_date)s;
        drop table if exists stage.ddz_jinbi_paycnt_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:return res

        return res


if __name__ == '__main__':
    a = ddzjinbiqujian()
    a()
