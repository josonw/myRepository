#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class ddzjinbiqujianthin(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
              create table if not exists analysis.ddz_jinbi_thin(
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                flft bigint,
                frgt bigint,
                fbycnt bigint)       --携带博雅币的用户数
              partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:return res

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
              drop table if exists stage.jw_ddz_user_thin_tmp_%(num_date)s;
              create table stage.jw_ddz_user_thin_tmp_%(num_date)s as
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
                          row_number() over(partition by fbpid, fuid order by flogin_at) rown
                      from stage.user_login_stg
                      where dt = "%(ld_begin)s"
                  ) ss
                  where ss.rown = 1
                ) t
                  on t.fbpid = b.fbpid
               ;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:return res


        hql = """
              insert overwrite table analysis.ddz_jinbi_thin
              partition( dt="%(ld_begin)s" )
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     b.flft flft,
                     b.frgt frgt,
                     count(a.fuid) fbycnt
                from stage.jw_ddz_user_thin_tmp_%(num_date)s a
                join stage.jw_qujian_grain b
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
        if res != 0:return res

        hql = """
        drop table if exists stage.jw_ddz_user_thin_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:return res


        return res


if __name__ == '__main__':
    a = ddzjinbiqujianthin()
    a()
