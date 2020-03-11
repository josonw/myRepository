# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_register_enter(BaseStatModel):
    """新用户注册和初次进入子游戏记录，由于原始表数据量大，单独抽出地方棋牌数据，形成轻量表。"""
    def create_tab(self):
        hql = """
            create table if not exists stage_dfqp.dfqp_user_register_enter
            (
              fuid bigint comment '用户游戏ID',
              new_regedit char(1) comment '是否新注册',
              new_enter char(1) comment '是否初进入'
            )
            comment '地方棋牌新用户_轻量表'
            partitioned by (dt date)
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table stage_dfqp.dfqp_user_register_enter
            partition (dt='%(statdate)s')
            select nvl(m.fuid, n.fuid) as fuid,
                   case
                     when m.fuid is null then
                      'N'
                     else
                      'Y'
                   end as new_regedit,
                   case
                     when n.fuid is null then
                      'N'
                     else
                      'Y'
                   end as new_enter
              from (select distinct a.fbpid, a.fuid
                      from stage.user_signup_stg a
                      left semi join dim.bpid_map c
                        on (a.fbpid = c.fbpid and c.fgamename = '地方棋牌')
                     where a.dt = '%(statdate)s') m
              full outer join (select distinct b.fbpid, b.fuid
                                 from stage.user_enter_stg b
                                 left semi join dim.bpid_map c
                                   on (b.fbpid = c.fbpid and c.fgamename = '地方棋牌')
                                where b.dt = '%(statdate)s'
                                  and b.fis_first = 1) n
                on (m.fbpid = n.fbpid and m.fuid = n.fuid)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_register_enter(sys.argv[1:])
a()

