#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class ReloadSubgameFif(BasePGStat):

    """ 更新子游戏和BPID相关的映射
    """

    def stat(self):
        sql = """
            BEGIN;
            UPDATE dcbase.subgame_map_fif
            SET fsubgamename = b.fsubgamename,priority=b.priority
            FROM (
                SELECT fgame_id, fplat_id, fhall_id, fsubgame_id, fver_id, fterminal_id, fsubgamename, priority
                FROM (
                    SELECT distinct
                        uda.fgamefsk as fgame_id,
                        uda.fplatformfsk as fplat_id,
                        uda.fhallfsk as fhall_id,
                        uda.fsubgamefsk as fsubgame_id,
                        uda.fversionfsk as fver_id,
                        uda.fterminaltypefsk as fterminal_id,
                        sd.fsubgamename,
                        sd.priority as priority
                    FROM dcnew.act_user uda
                    JOIN dcbase.subgame_dim sd
                        on uda.fsubgamefsk = sd.fsubgamefsk and  uda.fgamefsk = sd.fgamefsk
                    where uda.fsubgamefsk <> 0 and sd.fis_show=1 and uda.fdate=current_date-1) a
            ) as b
            WHERE subgame_map_fif.fgame_id=b.fgame_id
                and subgame_map_fif.fplat_id=b.fplat_id
                and subgame_map_fif.fhall_id=b.fhall_id
                and subgame_map_fif.fsubgame_id=b.fsubgame_id
                and subgame_map_fif.fver_id=b.fver_id
                and subgame_map_fif.fterminal_id=b.fterminal_id;

            INSERT INTO dcbase.subgame_map_fif
            (SELECT
                fgame_id,
                fplat_id,
                fhall_id,
                fsubgame_id,
                fver_id,
                fterminal_id,
                fsubgamename,
                priority
            FROM (SELECT distinct uda.fgamefsk as fgame_id,uda.fplatformfsk as fplat_id,uda.fhallfsk as fhall_id, uda.fsubgamefsk as fsubgame_id,uda.fversionfsk as fver_id,uda.fterminaltypefsk as fterminal_id,sd.fsubgamename,sd.priority as priority from dcnew.act_user uda join dcbase.subgame_dim sd
                    on uda.fsubgamefsk = sd.fsubgamefsk and  uda.fgamefsk = sd.fgamefsk
                    where uda.fsubgamefsk <> 0 and sd.fis_show=1 and uda.fdate=current_date-1) a
            WHERE(fgame_id,
                fplat_id,
                fhall_id,
                fsubgame_id,
                fver_id,
                fterminal_id) NOT IN (
                SELECT
                    fgame_id,
                    fplat_id,
                    fhall_id,
                    fsubgame_id,
                    fver_id,
                    fterminal_id
                FROM
                    dcbase.subgame_map_fif)
            );
            END;
        """

        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = ReloadSubgameFif(stat_date)
    a()
