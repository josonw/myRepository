#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_pay_cnt_range_day(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_pay_cnt_range_fct
        (
        fdate                   date,
        fgamefsk                bigint,
        fplatformfsk            bigint,
        fversionfsk             bigint,
        fterminalfsk            bigint,
        fpaycnt                 bigint,
        f1dpayusercnt           bigint,
        f7dpayusercnt           bigint,
        f30dpayusercnt          bigint
        )
        partitioned by (dt date)   """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table user_pay_cnt_range_fct drop partition(dt = "%(stat_date)s")
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
          insert into table analysis.user_pay_cnt_range_fct partition(dt = "%(stat_date)s")
          select '%(stat_date)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fpaycnt,
                 count(distinct fplatform_uid) f1dpayusercnt,
                 null f7dpayusercnt,
                 null f30dpayusercnt
            from (select bpm.fplatformfsk,
                         bpm.fgamefsk,
                         bpm.fversionfsk,
                         bpm.fterminalfsk,
                         fplatform_uid,
                         count(*) fpaycnt
                    from stage.payment_stream_stg ps
                    join analysis.bpid_platform_game_ver_map bpm
                      on ps.fbpid = bpm.fbpid
                   where ps.fdate >= '%(stat_date)s'
                     and ps.fdate < date_add('%(stat_date)s', 1)
                   group by bpm.fplatformfsk,
                            bpm.fgamefsk,
                            bpm.fversionfsk,
                            bpm.fterminalfsk,
                            fplatform_uid) a
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpaycnt
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
          insert into table analysis.user_pay_cnt_range_fct partition(dt = "%(stat_date)s")
          select '%(stat_date)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fpaycnt,
                 null f1dpayusercnt,
                 count(distinct fplatform_uid) f7dpayusercnt,
                 null f30dpayusercnt
            from (select bpm.fplatformfsk,
                         bpm.fgamefsk,
                         bpm.fversionfsk,
                         bpm.fterminalfsk,
                         fplatform_uid,
                         count(*) fpaycnt
                    from stage.payment_stream_stg ps
                    join analysis.bpid_platform_game_ver_map bpm
                      on ps.fbpid = bpm.fbpid
                   where ps.fdate >= date_add('%(stat_date)s', -6)
                     and ps.fdate < date_add('%(stat_date)s', 1)
                   group by bpm.fplatformfsk,
                            bpm.fgamefsk,
                            bpm.fversionfsk,
                            bpm.fterminalfsk,
                            fplatform_uid) a
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fpaycnt
        """% self.hql_dict
        hql_list.append(hql)

        hql = """
          insert into table analysis.user_pay_cnt_range_fct partition(dt = "%(stat_date)s")
          select '%(stat_date)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fpaycnt,
                 null f1dpayusercnt,
                 null f7dpayusercnt,
                 count(distinct fplatform_uid) f30dpayusercnt
            from (select bpm.fplatformfsk,
                         bpm.fgamefsk,
                         bpm.fversionfsk,
                         bpm.fterminalfsk,
                         fplatform_uid,
                         count(*) fpaycnt
                    from stage.payment_stream_stg ps
                    join analysis.bpid_platform_game_ver_map bpm
                      on ps.fbpid = bpm.fbpid
                   where ps.fdate >= date_add('%(stat_date)s', -29)
                     and ps.fdate < date_add('%(stat_date)s', 1)
                   group by bpm.fplatformfsk,
                            bpm.fgamefsk,
                            bpm.fversionfsk,
                            bpm.fterminalfsk,
                            fplatform_uid) a
           group by fplatformfsk, fgamefsk, fversionfsk, fterminalfsk, fpaycnt

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_pay_cnt_range_fct partition(dt = "%(stat_date)s")
        select
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fpaycnt,
            max(f1dpayusercnt) f1dpayusercnt,
            max(f7dpayusercnt) f7dpayusercnt,
            max(f30dpayusercnt) f30dpayusercnt
        from analysis.user_pay_cnt_range_fct
        where dt = "%(stat_date)s"
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fpaycnt
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
    a = agg_user_pay_cnt_range_day(stat_date)
    a()
