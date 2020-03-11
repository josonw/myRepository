#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_parquet_user_act(BaseStatModel):
    """每日活跃用户维度表，数据从dim.user_act复制，保存为parquet格式"""
    
    def create_tab(self):
        hql = """
            create table if not exists dim.parquet_user_act
            (
              fbpid string comment 'BPID',
              fgame_id bigint comment '子游戏ID',
              fchannel_code bigint comment '渠道ID',
              fuid bigint comment '用户ID',
              flogin_cnt int comment '登录次数',
              fparty_num int comment '牌局数',
              fis_change_gamecoins tinyint comment '金流是否发生变化'
            )
            comment 'dim.user_act的parquet格式副本'
            partitioned by (dt string)
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table dim.parquet_user_act partition (dt='%(statdate)s')
            select fbpid,
                   fgame_id,
                   fchannel_code,
                   fuid,
                   flogin_cnt,
                   fparty_num,
                   fis_change_gamecoins
              from dim.user_act
             where dt='%(statdate)s'
             limit 100000000
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化，运行
a = load_parquet_user_act(sys.argv[1:])
a()