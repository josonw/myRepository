#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_bud_user_online_data(BasePGCluster):

    """地方棋牌在线在玩数据
    """

    def stat(self):

        stimestamp = time.mktime(
            datetime.datetime.strptime(self.stat_date, '%Y-%m-%d').timetuple())
        etimestamp = stimestamp + 86400

        self.sql_dict['stimestamp'] = stimestamp
        self.sql_dict['etimestamp'] = etimestamp

        sql = """
              delete from bud_dm.bud_user_online_data where fdate= date'%(ld_begin)s';

                 COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
             SET statement_mem='256MB';
              drop table  IF EXISTS tmp;
              create table  tmp as (select fbpid,
                             -13658 as fgame_id,
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
                       group by fbpid);

              insert into bud_dm.bud_user_online_data
              (fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminaltypefsk,
               fhallfsk,
               fsubgamefsk,
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
                     -21379 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
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
                join bud_dm.bpid_map_bud bpm
                  on uof.fbpid = bpm.fbpid
               group by fplatformfsk,
                        fgamefsk,
                        fhallfsk
               union all
              select date'%(stat_date)s',
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
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
                join bud_dm.bpid_map_bud bpm
                  on uof.fbpid = bpm.fbpid
               group by fplatformfsk,
                        fgamefsk,
                        fversionfsk,
                        fterminaltypefsk,
                        fhallfsk;
                 COMMIT;""" % self.sql_dict
        self.exe_hql(sql)

        sql = """
              SET statement_mem='256MB';
              drop table  IF EXISTS tmp;
              create table  tmp  as (select fbpid,
                             fgame_id,
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
                       group by fbpid,fgame_id);

         insert into bud_dm.bud_user_online_data
              (fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminaltypefsk,
               fhallfsk,
               fsubgamefsk,
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
                join bud_dm.bpid_map_bud bpm
                  on uof.fbpid = bpm.fbpid
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
                join bud_dm.bpid_map_bud bpm
                  on uof.fbpid = bpm.fbpid
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
                join bud_dm.bpid_map_bud bpm
                  on uof.fbpid = bpm.fbpid
               group by fgamefsk,
                        fgame_id;

                 COMMIT; """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_bud_user_online_data(stat_date)
    a()
