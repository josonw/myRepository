#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_poker_tbl_info_sync(BaseStat):

    def create_tab(self):
        hql = """
        use analysis;
        create table if not exists analysis.poker_tbl_info_ext
        (
            fbpid            string,
            ftbl_id          string,
            fmin_coins_limit bigint,
            fdev_limit       bigint,
            fhas_prive       bigint,
            fmax_palyer      bigint,
            fvip_level       bigint,
            flts_at          string,
            fsubname         string,
            fpname           string
        )
        partitioned by(dt date)
        location '/dw/analysis/poker_tbl_info_ext'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        use analysis;
        create table if not exists analysis.poker_tbl_info_dim
        (
            fbpid            string,
              ftbl_id          string,
              fmin_coins_limit bigint,
              fdev_limit       bigint,
              fhas_prive       bigint,
              fmax_palyer      bigint,
              fvip_level       bigint,
              fsubname         string,
              fpname           string
        )
        location '/dw/analysis/poker_tbl_info_dim'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """

        query = { 'statdate':statDate }

        hql = """    -- 一个平台只上报一个bpid的，这里要处理为每个bpid都有
        insert overwrite table analysis.poker_tbl_info_ext
        partition( dt="%(statdate)s" )
            select bpm.fbpid fbpid, ab.ftbl_id ftbl_id, ab.fmin_coins_limit fmin_coins_limit,
                ab.fdev_limit fdev_limit, ab.fhas_prive fhas_prive, ab.fmax_palyer fmax_palyer,
                ab.fvip_level fvip_level, ab.flts_at flts_at, ab.fsubname fsubname, ab.fpname fpname
            from (    -- 先处理每个游戏每个平台一份
                select b.fgamefsk fgamefsk, b.fplatformfsk fplatformfsk, a.ftbl_id ftbl_id,
                    a.fmin_coins_limit fmin_coins_limit, a.fdev_limit fdev_limit,
                    a.fhas_prive fhas_prive, a.fmax_palyer fmax_palyer, a.fvip_level fvip_level,
                    a.flts_at flts_at, a.fsubname fsubname, a.fpname fpname
                from stage.poker_tbl_info a
                join analysis.bpid_platform_game_ver_map b
                      on a.fbpid = b.fbpid
                where a.dt = "%(statdate)s"
                group by b.fgamefsk, b.fplatformfsk, a.ftbl_id, a.fmin_coins_limit, a.fdev_limit, a.fhas_prive,
                    a.fmax_palyer, a.fvip_level, a.flts_at, a.fsubname, a.fpname
            ) ab
            join analysis.bpid_platform_game_ver_map bpm
                on ab.fgamefsk = bpm.fgamefsk and ab.fplatformfsk = bpm.fplatformfsk
            group by bpm.fbpid, ab.ftbl_id, ab.fmin_coins_limit, ab.fdev_limit, ab.fhas_prive,
                ab.fmax_palyer, ab.fvip_level, ab.flts_at, ab.fsubname, ab.fpname
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """    -- 每个bpid、每张桌子取一条数据
        insert overwrite table analysis.poker_tbl_info_dim
            select fbpid, ftbl_id, fmin_coins_limit,
                fdev_limit, fhas_prive, fmax_palyer,
                fvip_level, fsubname, fpname
            from (    -- 新旧数据合在一起排序，新的在前面
                select fbpid, ftbl_id, fmin_coins_limit,
                    fdev_limit, fhas_prive, fmax_palyer,
                    fvip_level, fsubname, fpname,
                    row_number() over(partition by fbpid, ftbl_id order by flag desc) as f
                from
                (
                    select fbpid, ftbl_id, fmin_coins_limit,
                        fdev_limit, fhas_prive, fmax_palyer,
                        fvip_level, fsubname, fpname, 1 as flag
                    from analysis.poker_tbl_info_dim
                    union all
                    select fbpid, ftbl_id, fmin_coins_limit,
                        fdev_limit, fhas_prive, fmax_palyer,
                        fvip_level, fsubname, fpname, 2 as flag
                    from analysis.poker_tbl_info_ext
                    where dt = "%(statdate)s"
                ) t
            ) tt
            where f = 1;
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        return res


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_poker_tbl_info_sync(statDate, eid)
a()
