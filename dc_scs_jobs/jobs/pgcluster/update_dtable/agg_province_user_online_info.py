#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_user_province_online_user_info(BasePGCluster):

    """地方棋牌分省数据增加在线在玩
    """

    def stat(self):

        stimestamp = time.mktime(
            datetime.datetime.strptime(self.stat_date, '%Y-%m-%d').timetuple())
        etimestamp = stimestamp + 86400

        self.sql_dict['stimestamp'] = int(stimestamp)
        self.sql_dict['etimestamp'] = int(etimestamp)

        sql = """--结果表临时表
              delete from dcnew.province_user_info_tmp;
                 COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)


#        sql = """ --在线在玩临时表
#         delete from dcnew.province_user_online_tmp;
#         SET statement_mem='256MB';
#         insert into dcnew.province_user_online_tmp
#         select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince,
#                             max(fonline_num) fmaxonline,
#                             max(fplaying_num) fmaxplay,
#                             floor(avg(fonline_num)) favgonline,
#                             floor(avg(fplaying_num)) favgplay,
#                             max(uof.fonline_num - uof.fplaying_num) fmaxnoplay,
#                             max(uof.fonline_num - uof.fplaying_num - uof.fspectator_num) fmaxhall,
#                             max(uof.fplaying_num + uof.fspectator_num) fmaxroom
#                        from (select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince,
#                                     floor(flogged_at / 300) flogged_at,
#                                     max(fonline_num) fonline_num,
#                                     max(fplaying_num) fplaying_num,
#                                     max(fspectator_num) fspectator_num
#                                from (select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince, flogged_at, fonline_num, fplaying_num, fspectator_num
#                                        from analysis.user_online_fct uof
#                                        join dcbase.bpid_map bpm
#                                          on uof.fbpid = bpm.fbpid
#                                         and bpm.fgamefsk = 4132314431  --地方棋牌
#                                       where flogged_at >= %(stimestamp)s
#                                         and flogged_at < %(etimestamp)s
#                                       union all
#                                      select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince, flogged_at, fonline_num, fplaying_num, fspectator_num
#                                        from analysis.user_online_cn_fct uof
#                                        join dcbase.bpid_map bpm
#                                          on uof.fbpid = bpm.fbpid
#                                         and bpm.fgamefsk = 4132314431  --地方棋牌
#                                       where flogged_at >= %(stimestamp)s
#                                         and flogged_at < %(etimestamp)s
#                                     ) as uof
#                               group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince, floor(flogged_at / 300)
#                             ) uof
#                       group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince;
#                 COMMIT;
#        """ % self.sql_dict
#        self.exe_hql(sql)


        sql = """--结果临时表中插入在线在玩数据
      insert into dcnew.province_user_info_tmp
      select date'%(stat_date)s' fdate,
             fgamefsk,
             fplatformfsk,
             fhallfsk,
             fgame_id fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannel_code fchannelcode,
             fprovince,
             0 fact_unum,
             0 freg_unum,
             fmaxonline fmax_online,
             fmaxplay fmax_play,
             0 fldr_back_unum,
             0 fgame_back_unum,
             0 fhall_back_unum,
             0 fpart_reg_unum,
             0 fpart_back_unum,
             0 fpart_ind_reg_unum,
             0 fpart_ind_back_unum,
             0 fshare_unum,
             0 fshare_reg_unum,
             0 fgame_act_unum,
             0 f30act_unum,
             favgonline favg_online,
             favgplay favg_play
        from (select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fterminaltypefsk,
                        fhallfsk,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fhallfsk,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     fterminaltypefsk,
                     -21379 fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fterminaltypefsk,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     -21379 fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fterminaltypefsk,
                        fhallfsk
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fhallfsk
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     fterminaltypefsk,
                     -21379 fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fterminaltypefsk
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     -21379 fhallfsk,
                     -21379 fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk) t """ % self.sql_dict
        self.exe_hql(sql)

        sql = """--结果临时表中插入在线在玩数据
                     SET statement_mem='256MB';
      insert into dcnew.province_user_info_tmp
      select date'%(stat_date)s' fdate,
             fgamefsk,
             fplatformfsk,
             fhallfsk,
             fgame_id fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannel_code fchannelcode,
             fprovince,
             0 fact_unum,
             0 freg_unum,
             fmaxonline fmax_online,
             fmaxplay fmax_play,
             0 fldr_back_unum,
             0 fgame_back_unum,
             0 fhall_back_unum,
             0 fpart_reg_unum,
             0 fpart_back_unum,
             0 fpart_ind_reg_unum,
             0 fpart_ind_back_unum,
             0 fshare_unum,
             0 fshare_reg_unum,
             0 fgame_act_unum,
             0 f30act_unum,
             favgonline favg_online,
             favgplay favg_play
        from (select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fterminaltypefsk,
                        fhallfsk,
                        fgame_id,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fhallfsk,
                        fgame_id,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     -21379 fhallfsk,
                     fgame_id,
                     -1 fchannel_code,
                     fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fgame_id,
                        fprovince
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     fterminaltypefsk,
                     fhallfsk,
                     fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fterminaltypefsk,
                        fgame_id,
                        fhallfsk
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     fhallfsk,
                     fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fgame_id,
                        fhallfsk
               union all
              select fgamefsk,
                     fplatformfsk,
                     -1 fversionfsk,
                     -21379 fterminaltypefsk,
                     -21379 fhallfsk,
                     fgame_id,
                     -1 fchannel_code,
                     '-21379' fprovince,
                     sum(fmaxonline) fmaxonline,
                     sum(fmaxplay) fmaxplay,
                     sum(favgonline) favgonline,
                     sum(favgplay) favgplay,
                     sum(fmaxnoplay) fmaxnoplay,
                     sum(fmaxhall) fmaxhall,
                     sum(fmaxroom) fmaxroom
                from dcnew.province_user_online_tmp uof
               where fgame_id = -21379
               group by fplatformfsk,
                        fgamefsk,
                        fgame_id) t """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
      delete from dcnew.province_user_online_info where fdate = '%(stat_date)s' ;
      insert into dcnew.province_user_online_info
      select fdate,
             fgamefsk,
             fplatformfsk,
             fhallfsk,
             fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannelcode,
             fprovince,
             sum(fmax_online) fmax_online,
             sum(fmax_play) fmax_play,
             sum(favg_online) favg_online,
             sum(favg_play) favg_play
        from dcnew.province_user_info_tmp t
       group by fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fprovince;

                """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
             insert into dcnew.province_user_info_tmp
             select fdate,
                    fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fsubgamefsk,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannelcode,
                    fprovince,
                    fact_unum,
                    freg_unum,
                    0 fmax_online,
                    0 fmax_play,
                    fldr_back_unum,
                    fgame_back_unum,
                    fhall_back_unum,
                    fpart_reg_unum,
                    fpart_back_unum,
                    fpart_ind_reg_unum,
                    fpart_ind_back_unum,
                    fshare_unum,
                    fshare_reg_unum,
                    fgame_act_unum,
                    0 f30act_unum,
                    0 favg_online,
                    0 favg_play
               from dcnew.province_user_info a
              where fdate = '%(stat_date)s';

             insert into dcnew.province_user_info_tmp
             select fdate,
                    fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fsubgamefsk,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannelcode,
                    fprovince,
                    0 fact_unum,
                    0 freg_unum,
                    0 fmax_online,
                    0 fmax_play,
                    0 fldr_back_unum,
                    0 fgame_back_unum,
                    0 fhall_back_unum,
                    0 fpart_reg_unum,
                    0 fpart_back_unum,
                    0 fpart_ind_reg_unum,
                    0 fpart_ind_back_unum,
                    0 fshare_unum,
                    0 fshare_reg_unum,
                    0 fgame_act_unum,
                    f30act_unum,
                    0 favg_online,
                    0 favg_play
               from dcnew.province_user_act30_info a
              where fdate = '%(stat_date)s';

       delete from dcnew.province_user_info where fdate = '%(stat_date)s' ;
       insert into dcnew.province_user_info
       select fdate,
              fgamefsk,
              fplatformfsk,
              fhallfsk,
              fsubgamefsk,
              fterminaltypefsk,
              fversionfsk,
              fchannelcode,
              fprovince,
              sum(fact_unum) fact_unum,
              sum(freg_unum) freg_unum,
              sum(fmax_online) fmax_online,
              sum(fmax_play) fmax_play,
              sum(fldr_back_unum) fldr_back_unum,
              sum(fgame_back_unum) fgame_back_unum,
              sum(fhall_back_unum) fhall_back_unum,
              sum(fpart_reg_unum) fpart_reg_unum,
              sum(fpart_back_unum) fpart_back_unum,
              sum(fpart_ind_reg_unum) fpart_ind_reg_unum,
              sum(fpart_ind_back_unum) fpart_ind_back_unum,
              sum(fshare_unum) fshare_unum,
              sum(fshare_reg_unum) fshare_reg_unum,
              sum(fgame_act_unum) fgame_act_unum,
              sum(f30act_unum) f30act_unum,
              sum(favg_online) favg_online,
              sum(favg_play) favg_play
         from dcnew.province_user_info_tmp t
       group by fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fprovince;

                """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_user_province_online_user_info(stat_date)
    a()
