#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_user_online_data_new(BasePGStat):

    """用户在线在玩数据
    """

    def stat(self):

        stimestamp = time.mktime(
            datetime.datetime.strptime(self.stat_date, '%Y-%m-%d').timetuple())
        etimestamp = stimestamp + 86400

        self.sql_dict['stimestamp'] = stimestamp
        self.sql_dict['etimestamp'] = etimestamp

        sql = """
              delete from dcnew.user_online_byday_new where fdate= date'%(ld_begin)s';

                 COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
              with tmp as (select fbpid,
                             -13658 as fgame_id,
                             -13658 as fchannel_code,
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
                             floor(avg(uof.fonline_num - uof.fplaying_num - uof.fspectator_num)) favghall,
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
                       group by fbpid)

              insert into dcnew.user_online_byday_new
              (fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminaltypefsk,
               fhallfsk,
               fsubgamefsk,
               fchannelcode,
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
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=1
               group by fplatformfsk,
                        fgamefsk,
                        fversionfsk,
                        fterminaltypefsk,
                        fhallfsk
               union all
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     -21379 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=1
               group by fplatformfsk,
                        fgamefsk,
                        fhallfsk
               union all
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminaltypefsk,
                     -21379 fhallfsk,
                     -21379 fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=0
               group by fplatformfsk,
                        fgamefsk,
                        fversionfsk,
                        fterminaltypefsk
               union all
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     -21379 fversionfsk,
                     -21379 fterminaltypefsk,
                     -21379 fhallfsk,
                     -21379 fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=0
               group by fplatformfsk,
                        fgamefsk;
                 COMMIT;""" % self.sql_dict
        self.exe_hql(sql)

        sql = """
              with tmp as (select fbpid,
                             fgame_id,
                             -13658 as fchannel_code,
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
                             floor(avg(uof.fonline_num - uof.fplaying_num - uof.fspectator_num)) favghall,
                             min(uof.fonline_num - uof.fplaying_num - uof.fspectator_num) fminhall,
                             max(uof.fplaying_num + uof.fspectator_num) fmaxroom,
                             floor(avg(uof.fplaying_num + uof.fspectator_num)) favgroom,
                             min(uof.fplaying_num + uof.fspectator_num) fminroom
                        from (select fbpid,fsubgame_id fgame_id,
                                     floor(extract(epoch from fcreate_at) / 300) flogged_at,
                                     max(fonline_num) fonline_num,
                                     max(fplay_num) fplaying_num,
                                     0 fspectator_num
                                from (select fbpid, fsubgame_id, fcreate_at, fonline_num, fplay_num from analysis.user_online_subgme_fct
                                       where fdate >= to_timestamp(%(stimestamp)s)
                                         and fdate < to_timestamp(%(etimestamp)s)
                                       union all
                                      select fbpid, fsubgame_id, fcreate_at, fonline_num, fplay_num from analysis.user_online_subgme_cn_fct
                                       where fdate >= to_timestamp(%(stimestamp)s)
                                         and fdate < to_timestamp(%(etimestamp)s)
                                     ) as uof
                               group by fbpid, fsubgame_id, floor(extract(epoch from fcreate_at) / 300)
                             ) uof
                       group by fbpid,fgame_id)

         insert into dcnew.user_online_byday_new
              (fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminaltypefsk,
               fhallfsk,
               fsubgamefsk,
               fchannelcode,
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
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=1
               group by fplatformfsk,
                        fgamefsk,
                        fversionfsk,
                        fterminaltypefsk,
                        fhallfsk,
                        fgame_id
               union all
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     -21379 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=1
               group by fplatformfsk,
                        fgamefsk,
                        fhallfsk,
                        fgame_id
               union all
              select date'%(stat_date)s',
                     fgamefsk,
                     -21379 fplatformfsk,
                     -21379 fversionfsk,
                     -21379 fterminaltypefsk,
                     -21379 fhallfsk,
                     fgame_id,
                     -21379 fchannel_code,
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
                from tmp uof
                join dcbase.bpid_map bpm
                  on uof.fbpid = bpm.fbpid
               where bpm.hallmode=1
               group by fgamefsk,
                        fgame_id;

                 COMMIT; """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_user_online_data_new(stat_date)
    a()
