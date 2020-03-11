#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_user_online_data(BasePGCluster):

    """用户在线在玩数据
    """

    def stat(self):

        stimestamp = time.mktime(
            datetime.datetime.strptime(self.stat_date, '%Y-%m-%d').timetuple())
        etimestamp = stimestamp + 86400

        self.sql_dict['stimestamp'] = int(stimestamp)
        self.sql_dict['etimestamp'] = int(etimestamp)

        sql = """
              delete from analysis.user_online_byday_agg where fdate= date'%(ld_begin)s'
        """ %  self.sql_dict
        self.append(sql)

        sql = """SET statement_mem='256MB';
              insert into analysis.user_online_byday_agg
              (fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fmaxonline,
               favgonline,
               fminonline,
               fmaxplay,
               favgplay,
               fminplay,
               fmaxnoplay,
               favgnoplay,
               fminnoplay,
               fmaxhall,
               favghall,
               fminhall,
               fmaxroom,
               favgroom,
               fminroom)
              select date'%(ld_begin)s',
                     bpm.fgamefsk,
                     bpm.fplatformfsk,
                     bpm.fversionfsk,
                     bpm.fterminalfsk,
                     sum(fmaxonline) fmaxonline,
                     sum(favgonline) favgonline,
                     sum(fminonline) fminonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgplay) favgplay,
                     sum(fminplay) fminplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(favgnoplay) favgnoplay,
                     sum(fminnoplay) fminnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(favghall) favghall,
                     sum(fminhall) fminhall,
                     sum(fmaxroom) fmaxroom,
                     sum(favgroom) favgroom,
                     sum(fminroom) fminroom
                from (select fbpid,
                             max(fonline_num) fmaxonline,
                             floor(avg(fonline_num)) favgonline,
                             min(fonline_num) fminonline,
                             max(fplaying_num) fmaxplay,
                             floor(avg(fplaying_num)) favgplay,
                             min(fplaying_num) fminplay,
                             max(uof.fonline_num - uof.fplaying_num) fmaxnoplay,
                             floor(avg(uof.fonline_num - uof.fplaying_num)) favgnoplay,
                             min(uof.fonline_num - uof.fplaying_num) fminnoplay,
                             max(uof.fonline_num - uof.fplaying_num - uof.fspectator_num) fmaxhall,
                             floor(avg(uof.fonline_num - uof.fplaying_num -
                                       uof.fspectator_num)) favghall,
                             min(uof.fonline_num - uof.fplaying_num - uof.fspectator_num) fminhall,
                             max(uof.fplaying_num + uof.fspectator_num) fmaxroom,
                             floor(avg(uof.fplaying_num + uof.fspectator_num)) favgroom,
                             min(uof.fplaying_num + uof.fspectator_num) fminroom
                        from (select fbpid,
                                     floor(flogged_at / 300) flogged_at,
                                     max(fonline_num) fonline_num,
                                     max(fplaying_num) fplaying_num,
                                     max(fspectator_num) fspectator_num
                                from
                                (
                                    select fbpid, flogged_at, fonline_num, fplaying_num, fspectator_num from analysis.user_online_fct
                                     where flogged_at >= %(stimestamp)s
                                     and flogged_at < %(etimestamp)s
                                     union all
                                     select fbpid, flogged_at, fonline_num, fplaying_num, fspectator_num from analysis.user_online_cn_fct
                                     where flogged_at >= %(stimestamp)s
                                     and flogged_at < %(etimestamp)s
                                 ) as uof
                               group by fbpid, floor(flogged_at / 300)
                               ) uof
                       group by fbpid) uof
                join analysis.bpid_platform_game_ver_map bpm
                  on uof.fbpid = bpm.fbpid
               group by bpm.fplatformfsk,
                        bpm.fgamefsk,
                        bpm.fversionfsk,
                        bpm.fterminalfsk """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_user_online_data(stat_date)
    a()
