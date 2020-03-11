#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_payment_qy_data(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        self.hql_dict['ld_season_begin'] = PublicFunc.trunc(self.stat_date, "q")
        self.hql_dict['ld_year_begin'] = PublicFunc.trunc(self.stat_date, "yyyy")

        hql = """
        drop table if exists analysis.user_payment_fct_year_season_part_%(num_begin)s;
        create table if not exists analysis.user_payment_fct_year_season_part_%(num_begin)s
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fquarterpu                  bigint,
            fhyearpu                    bigint
        );
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_payment_fct_year_season_part_%(num_begin)s
        select '%(stat_date)s' fdate,
             fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
             count(distinct case
                     when dt >= '%(ld_season_begin)s' and
                          dt < date_add('%(stat_date)s', 1) then
                          fplatform_uid
                   end) fquarterpu,
             count(distinct fplatform_uid) fhyearpu
            from stage.payment_stream_stg a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
            where (dt >= '%(ld_year_begin)s' and dt < date_add('%(stat_date)s', 1) )
            group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
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
    a = agg_user_payment_qy_data(stat_date)
    a()
