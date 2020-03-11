#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

"""
payment_stream_stg 全部流水
"""

class agg_user_payment_continued_day(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        # 连续付费天
        hql = """
        drop table if exists stage.payment_date_tmp_%(num_begin)s;
        create table if not exists stage.payment_date_tmp_%(num_begin)s as

        select fbpid, fplatform_uid, pss.dt fpaydate, count(*) fpaycnt
        from stage.payment_stream_stg pss
        where pss.dt < '%(ld_end)s'
        group by fbpid, fplatform_uid, pss.dt;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """

        drop table if exists stage.payment_date_result_tmp_%(num_begin)s;
        create table if not exists stage.payment_date_result_tmp_%(num_begin)s as

        select fbpid,
            fplatform_uid,
            fpaydate,
            count(1) over(partition by fbpid, fplatform_uid, fflag) fconpayday,
            fpaycnt
        from
        (
            select fbpid,
                fplatform_uid,
                fpaydate,
                date_add(fpaydate, -dense_rank() over(partition by fbpid, fplatform_uid order by fpaydate) ) fflag,
                fpaycnt
            from stage.payment_date_tmp_%(num_begin)s
        ) a
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_payment_fct_continue_part_%(num_begin)s;

        create table if not exists analysis.user_payment_fct_continue_part_%(num_begin)s
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            favgconpayday               bigint,
            fmaxconpayday               bigint,
            f1daymaxpaycnt              bigint,
            f1dayminpaycnt              bigint,
            f3daymaxpaycnt              bigint,
            f3dayminpaycnt              bigint,
            f7daymaxpaycnt              bigint,
            f7dayminpaycnt              bigint,
            f30daymaxpaycnt             bigint,
            f30dayminpaycnt             bigint
        )
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_payment_fct_continue_part_%(num_begin)s
            select '%(stat_date)s' fdate,
                    fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                    floor(avg(drt.fconpayday)) favgconpayday,
                    max(drt.fconpayday) fmaxconpayday,
                    0 f1daymaxpaycnt,
                    0 f1dayminpaycnt,
                    0 f3daymaxpaycnt,
                    0 f3dayminpaycnt,
                    0 f7daymaxpaycnt,
                    0 f7dayminpaycnt,
                    0 f30daymaxpaycnt,
                    0 f30dayminpaycnt
                 from stage.payment_date_result_tmp_%(num_begin)s drt
                 join analysis.bpid_platform_game_ver_map b
                   on drt.fbpid = b.fbpid
                 where drt.fpaydate >= '%(stat_date)s'
                   and drt.fpaydate < '%(ld_end)s'
                group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.user_payment_fct_continue_part_%(num_begin)s
            select '%(stat_date)s' fdate,
                  fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                  0 favgconpayday,
                  0 fmaxconpayday,
                  nvl(max(ucpo.f1daymaxpaycnt), 0) f1daymaxpaycnt,
                  nvl(min(case ucpo.f1dayminpaycnt when 0 then null else ucpo.f1dayminpaycnt end), 0) f1dayminpaycnt,
                  nvl(max(ucpo.f3daymaxpaycnt), 0) f3daymaxpaycnt,
                  nvl(min(case ucpo.f3dayminpaycnt when 0 then null else ucpo.f3dayminpaycnt end), 0) f3dayminpaycnt,
                  nvl(max(ucpo.f7daymaxpaycnt), 0) f7daymaxpaycnt,
                  nvl(min(case ucpo.f7dayminpaycnt when 0 then null else ucpo.f7dayminpaycnt end), 0) f7dayminpaycnt,
                  nvl(max(ucpo.f30daymaxpaycnt), 0) f30daymaxpaycnt,
                  nvl(min(case ucpo.f30dayminpaycnt when 0 then null else ucpo.f30dayminpaycnt end), 0) f30dayminpaycnt
             from (select fbpid,
                          fplatform_uid,
                          max(case when ucp.fpaydate >= '%(stat_date)s' and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f1daymaxpaycnt,
                          min(case
                                when ucp.fpaydate >= '%(stat_date)s' and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f1dayminpaycnt,
                          max(case
                                when ucp.fpaydate >= date_add('%(ld_end)s',-3) and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f3daymaxpaycnt,
                          min(case
                                when ucp.fpaydate >= date_add('%(ld_end)s',-3) and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f3dayminpaycnt,
                          max(case
                                when ucp.fpaydate >= date_add('%(ld_end)s',-7) and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f7daymaxpaycnt,
                           min(case
                                when ucp.fpaydate >= date_add('%(ld_end)s',-7) and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f7dayminpaycnt,
                          max(case
                                when ucp.fpaydate >= date_add('%(ld_end)s',-30) and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f30daymaxpaycnt,
                          min(case
                                when ucp.fpaydate >= date_add('%(ld_end)s',-30) and
                                     ucp.fpaydate < '%(ld_end)s' then
                                 ucp.fpaycnt
                                else
                                 null
                              end) f30dayminpaycnt
                     from stage.payment_date_result_tmp_%(num_begin)s ucp
                    where ucp.fpaydate >= date_add('%(ld_end)s',-30)
                      and ucp.fpaydate < '%(ld_end)s'
                    group by fbpid, fplatform_uid
                    ) ucpo
            join analysis.bpid_platform_game_ver_map b
              on ucpo.fbpid = b.fbpid
            group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_payment_fct_continue_part_%(num_begin)s
        select
            fdate,
            fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(favgconpayday)          favgconpayday,
            max(fmaxconpayday)          fmaxconpayday,
            max(f1daymaxpaycnt)         f1daymaxpaycnt,
            max(f1dayminpaycnt)         f1dayminpaycnt,
            max(f3daymaxpaycnt)         f3daymaxpaycnt,
            max(f3dayminpaycnt)         f3dayminpaycnt,
            max(f7daymaxpaycnt)         f7daymaxpaycnt,
            max(f7dayminpaycnt)         f7dayminpaycnt,
            max(f30daymaxpaycnt)        f30daymaxpaycnt,
            max(f30dayminpaycnt)        f30dayminpaycnt
        from analysis.user_payment_fct_continue_part_%(num_begin)s
        group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append( hql )

        # 统计完清理掉临时表
        hql = """
          drop table if exists stage.payment_date_tmp_%(num_begin)s;
          drop table if exists stage.payment_date_result_tmp_%(num_begin)s;
        """ % self.hql_dict
        hql_list.append(hql)

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
    a = agg_user_payment_continued_day(stat_date)
    a()
