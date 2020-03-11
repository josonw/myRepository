#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_bankrupt_relieve(BaseStatModel):
    """
    1. 创建每日用户破产救济中间表(bpid)
    2. 创建每日用户破产救济中间表(带组合)
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_bankrupt_relieve
        (
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户游戏ID
          frupt_cnt            int,                 --破产次数
          frlv_cnt             int,                 --救济次数
          frlv_gamecoins       bigint               --救济的金币总数
        )
        partitioned by(dt date)
        stored as orc
        """

        res = self.sql_exe(hql)

        hql = """
        create table if not exists dim.user_bankrupt_array
        (
            fgamefsk             bigint,            --游戏ID
            fplatformfsk         bigint,            --平台ID
            fhallfsk             bigint,            --大厅ID
            fsubgamefsk          bigint,            --子游戏ID
            fterminaltypefsk     bigint,            --终端ID
            fversionfsk          bigint,            --版本ID
            fchannelcode         bigint,            --渠道ID
            fuid                 bigint,            --用户ID
            frupt_cnt            int,               --破产次数
            frlv_cnt             int,               --救济次数
            frlv_gamecoins       bigint             --救济的金币总数
        )
        partitioned by (dt date)
        stored as orc
        """
        res = self.sql_exe(hql)

        return res

    def stat(self):

        hql = """
            drop table if exists work.user_bankrupt_relieve_tmp_%(statdatenum)s;
          create table work.user_bankrupt_relieve_tmp_%(statdatenum)s as
          select
            fbpid, coalesce(fgame_id,cast (0 as bigint)) fgame_id, coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
            fuid, 1 frupt_cnt, 0 frlv_cnt, 0 frlv_gamecoins
          from stage.user_bankrupt_stg
          where dt = '%(statdate)s'
          union all
          select fbpid, coalesce(fgame_id,cast (0 as bigint)) fgame_id, coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
          fuid, 0 frupt_cnt, 1 frlv_cnt, fgamecoins frlv_gamecoins
          from stage.user_bankrupt_relieve_stg
          where dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.user_bankrupt_relieve
        partition(dt="%(statdate)s")
        select /*+ MAPJOIN(ci) */
           fbpid,
           ubr.fgame_id,
           coalesce(max(ci.ftrader_id),'%(null_int_report)d') fchannel_code,
           fuid,
           sum(frupt_cnt) frupt_cnt,
           sum(frlv_cnt) frlv_cnt,
           sum(frlv_gamecoins) frlv_gamecoins
        from work.user_bankrupt_relieve_tmp_%(statdatenum)s ubr
        left join analysis.marketing_channel_pkg_info ci
        on ubr.fchannel_code = ci.fid
        group by fbpid,ubr.fgame_id,fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        extend_group = {
            'fields': ['fuid'],
            'groups': [[1]]
        }

        hql = """
        select  fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                sum(frupt_cnt) frupt_cnt,
                sum(frlv_cnt) frlv_cnt,
                sum(frlv_gamecoins) frlv_gamecoins
           from work.user_bankrupt_relieve_tmp_%(statdatenum)s a
           join dim.bpid_map b
              on a.fbpid = b.fbpid
           where b.hallmode=%(hallmode)s
           group by fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fgame_id,
                    fterminaltypefsk,
                    fversionfsk,
                    fchannel_code,
                    fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dim.user_bankrupt_array
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """drop table if exists work.user_bankrupt_relieve_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

# 生成统计实例
a = load_user_bankrupt_relieve(sys.argv[1:])
a()
