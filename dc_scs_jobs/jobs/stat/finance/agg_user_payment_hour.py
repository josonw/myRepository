#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_payment_hour(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_payment_hour_fct
        (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fversionfsk         bigint,
            fterminalfsk        bigint,
            fhourfsk            bigint,
            fpayusercnt         bigint,
            fpaycnt             bigint,
            fincome             decimal(20,2),
            fcum_pun            bigint
        )
        partitioned by (dt date)
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table user_payment_hour_fct drop partition(dt="%(stat_date)s");
        """ % self.hql_dict
        hql_list.append(hql)


        hql = """
          insert into table analysis.user_payment_hour_fct partition(dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
             bpm.fgamefsk,
             bpm.fplatformfsk,
             bpm.fversionfsk,
             bpm.fterminalfsk,
             hd.fsk fhourfsk,
             count(distinct ps.fplatform_uid) fpayusercnt,
             count(1) fpaycnt,
             round(sum(ps.fcoins_num * ps.frate), 2) fincome,
             null fcum_pun
        from stage.payment_stream_stg ps
        join analysis.bpid_platform_game_ver_map bpm
          on ps.fbpid = bpm.fbpid
        join analysis.hour_dim hd
          on hour(ps.fdate) = hd.fhourid
        where ps.dt = '%(stat_date)s'
        group by bpm.fgamefsk,
                bpm.fplatformfsk,
                bpm.fversionfsk,
                bpm.fterminalfsk,
                hd.fsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
          insert into table analysis.user_payment_hour_fct partition(dt = "%(stat_date)s")
          select '%(stat_date)s' fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fhourfsk,
               null fpayusercnt,
               null fpaycnt,
               null fincome,
               sum(fcum) over(partition by fgamefsk, fplatformfsk, fversionfsk order by fhourfsk rows between unbounded preceding and current row) fcum_pun
          from (select hd.fhour+1 fhourfsk,
                       bpm.fplatformfsk,
                       bpm.fgamefsk,
                       bpm.fversionfsk,
                       bpm.fterminalfsk,
                       count(distinct case when rown=1 then fplatform_uid else null end) fcum
                  from (select fdate,
                               fbpid,
                               fplatform_uid,
                               row_number() over(partition by fbpid, fplatform_uid order by fdate) rown
                          from stage.payment_stream_stg a
                         where dt = '%(stat_date)s') ps
                  join analysis.bpid_platform_game_ver_map bpm
                    on ps.fbpid = bpm.fbpid
                  join analysis.hour_dim hd
                    on hour(ps.fdate) = hd.fhourid
                 group by bpm.fgamefsk,
                          bpm.fplatformfsk,
                          bpm.fversionfsk,
                          bpm.fterminalfsk,
                          hd.fhour+1
            ) a
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_payment_hour_fct partition
        (dt = "%(stat_date)s")
         select
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fhourfsk,
            max(fpayusercnt)   fpayusercnt,
            max(fpaycnt)   fpaycnt,
            max(fincome)   fincome,
            max(fcum_pun)   fcum_pun
        from analysis.user_payment_hour_fct
        where dt = "%(stat_date)s"
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fhourfsk

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
    a = agg_user_payment_hour(stat_date)
    a()
