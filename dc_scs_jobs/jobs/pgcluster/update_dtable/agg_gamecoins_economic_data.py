#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date

class agg_gamecoins_economic_data(BasePGCluster):
    """德州经济系统数据
    """
    def stat(self):
        hql = """
              delete from analysis.gamecoins_economic_sys_day_fct
              where fdate=to_date('%(ld_begin)s', 'yyyy-mm-dd')
        """ % self.sql_dict
        self.append(hql)

        # 活跃用户
        hql = """
          insert into analysis.gamecoins_economic_sys_day_fct
          (fdate, fplatformfsk, fgamefsk, fversionfsk, factcnt)
          select fdate,
                 fplatformfsk,
                 fgamefsk,
                 fversionfsk,
                 avg(coalesce(factcnt, 0)) factcnt
           from analysis.user_true_active_fct
          where fgamefsk = '1396894'
            and fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
            and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
          group by fdate, fplatformfsk, fgamefsk, fversionfsk
        """ % self.sql_dict
        self.append(hql)

        # 充值发放
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set frecharge_in = b.frecharge_in
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) frecharge_in
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('3', '22', '17', '21', '715', '716')
                  and fgamefsk = 1396894
                  and fdirection = 'IN'
                  and fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)

        # 净发放
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set fnet_in = b.fnet_in
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) fnet_in
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('624', '14291','208', '33', '546', '162','285', '5','189', '169', '0',
                                '822', '711', '794', '834', '12331',  '8','14292', '30191', '15921', '15')
                  and fgamefsk = 1396894
                  and fdirection = 'IN'
                  and fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 其他发放
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set fother_in = b.fother_in
          from(select fdate,
                      fplatformfsk,
                      t1.fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) fother_in
                 from analysis.pay_game_coin_finace_fct t1
                 join (select fgamefsk, ftype, fname, fdirection, fis_circulate
                        from analysis.game_coin_type_dim
                       where fdirection = 'IN'
                         and ftype not in ('3','22','17', '21', '715', '716', '624','14291', '208', '33', '546',
                                           '162','285', '5', '189','169', '0', '822', '711', '794', '834', '12331', '8',
                                           '14292','30191', '15921', '15', '964', '482', '173', '992', '993', '650', '655',
                                           '115','264', '955','981', '984',  '14342', '930') -- 增加剔除
                         and fis_circulate = 0 -- 非流通
                         and fis_pay = 0
                         and fgamefsk = 1396894
                         and fcointype = 'GAMECOIN') t2 -- 游戏币
                   on t1.ftype = t2.ftype
                  and t1.fgamefsk = t2.fgamefsk
                where t1.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and t1.fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and t1.fgamefsk = 1396894
                  and t1.fdirection = 'IN'
                  and t1.fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, t1.fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 免费发放
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set free_in = b.free_in
          from(select a.fdate,
                      a.fplatformfsk,
                      a.fgamefsk,
                      a.fversionfsk,
                      sum(case
                            when a.fdirection = 'IN' then
                             fnum
                          end) free_in
                 from analysis.pay_game_coin_finace_fct a
                 left join analysis.game_coin_type_dim b
                   on a.fgamefsk = b.fgamefsk
                  and a.ftype = b.ftype
                  and a.fcointype = b.fcointype
                  and a.fdirection = b.fdirection
                where a.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and a.fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and a.fgamefsk = 1396894
                  and b.ftype <> '7'
                  and b.fis_pay = 0
                  and b.fis_gambling <> 1
                  and b.fis_box <> 1
                  and a.ftype not in ('963', '964', '481', '482', '172', '173','998', '991', '992', '993','649',
                                '650', '655', '115', '128','263','264','955','981','982', '983','984',
                                '14341','929','14345', '14342', '930')
                group by a.fdate, a.fplatformfsk, a.fgamefsk, a.fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 消耗台费
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set fcharge = b.fcharge
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) fcharge
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('27')
                  and fgamefsk = 1396894
                  and fdirection = 'OUT'
                  and fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 系统消耗扣分
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set fsys_out = b.fsys_out
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) fsys_out
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('23')
                  and fgamefsk = 1396894
                  and fdirection = 'OUT'
                  and fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 功能性消耗调整
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set ffunc_out = b.ffunc_out
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) ffunc_out
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('963', '964', '481','482', '172', '173','998', '991','992','993', '649',
                                '650','655', '115','128', '263', '264', '955','981', '982', '983','984',
                                '14341', '929','14345', '14342','930')
                  and fgamefsk = 1396894
                  and fcointype = 'GAMECOIN'
                  and fdirection = 'OUT'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 功能性发放
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set ffunc_in = b.ffunc_in
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) ffunc_in
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('963', '964', '481', '482', '172', '173','998', '991', '992', '993','649',
                                '650', '655', '115', '128','263','264','955','981','982', '983','984',
                                '14341','929','14345', '14342', '930')
                  and fgamefsk = 1396894
                  and fcointype = 'GAMECOIN'
                  and fdirection = 'IN'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 互动消耗
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set finteractive_out = b.finteractive_out
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) finteractive_out
                 from analysis.pay_game_coin_finace_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and ftype in ('562', '791', '759', '18', '6')
                  and fgamefsk = 1396894
                  and fdirection = 'OUT'
                  and fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 其他消耗
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set fother_out = b.fother_out
          from(select fdate,
                      fplatformfsk,
                      t1.fgamefsk,
                      fversionfsk,
                      sum(coalesce(fnum, 0)) fother_out
                 from analysis.pay_game_coin_finace_fct t1
                 join (select fgamefsk, ftype, fname, fdirection, fis_circulate
                        from analysis.game_coin_type_dim
                       where fdirection = 'OUT'
                         and ftype not in ('27','23', '963','964','481', '482', '172', '173', '998', '991', '992',
                                           '993', '649', '650', '655', '115', '128', '263','264', '955', '981', '982',
                                           '983', '984', '562', '791', '759','18', '6','14341', '929','14345')
                         and fis_circulate = 0 -- 非流通
                         and fis_gambling = 0
                         and fis_box = 0
                         and fis_pay = 0
                         and fgamefsk = 1396894
                         and fcointype = 'GAMECOIN') t2
                   on t1.ftype = t2.ftype
                  and t1.fgamefsk = t2.fgamefsk
                where t1.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and t1.fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                  and t1.fgamefsk = 1396894
                  and t1.fdirection = 'OUT'
                  and t1.fcointype = 'GAMECOIN'
                group by fdate, fplatformfsk, t1.fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)


        # 净消耗：台费+功能性消耗+牌桌互动消耗+其他消耗 – 免费发放 - 功能性发放
        hql = """
          update analysis.gamecoins_economic_sys_day_fct
             set fnet_out = coalesce(fcharge, 0) + coalesce(ffunc_out, 0) +
                            coalesce(finteractive_out, 0) + coalesce(fother_out, 0) -
                            coalesce(free_in, 0) - coalesce(ffunc_in, 0)
           where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
             and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
        """ % self.sql_dict
        self.append(hql)


        # 合计
        hql = """
          update analysis.gamecoins_economic_sys_day_fct
          set fin  = coalesce(frecharge_in, 0) + coalesce(fnet_in, 0) + coalesce(fother_in, 0) + coalesce(ffunc_in, 0) - coalesce(ffunc_out, 0),
              fout = coalesce(fcharge, 0) + coalesce(fsys_out, 0) + coalesce(finteractive_out, 0) + coalesce(fother_out, 0)
         where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
           and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
        """ % self.sql_dict
        self.append(hql)


        # 用户结余，保险箱
        hql = """
          update analysis.gamecoins_economic_sys_day_fct a
          set fuser_num = b.fbalance, fbank_num = b.fbalance_bank
          from(select fdate,
                      fplatformfsk,
                      fgamefsk,
                      fversionfsk,
                      sum(fbalance) fbalance,
                      sum(fbalance_bank) fbalance_bank
                 from analysis.gamecoin_balance_fct
                where fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
                  and fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
                group by fdate, fplatformfsk, fgamefsk, fversionfsk) b
          where a.fdate = b.fdate
            and a.fplatformfsk = b.fplatformfsk
            and a.fgamefsk = b.fgamefsk
            and a.fversionfsk = b.fversionfsk
        """ % self.sql_dict
        self.append(hql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    #生成统计实例
    a = agg_gamecoins_economic_data(stat_date)
    a()
