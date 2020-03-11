#! /usr/local/python272/bin/python
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



class load_user_act_array(BaseStatModel):
    """
    创建每日活跃用户维度表(去fbpid, 按照7个维度9种组合汇总后的维度表)
    两部分活跃用户构成。游戏全部活跃，子游戏内活跃。
    全量活跃，分大厅模式，非大厅模式。

    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_act_array
        (
            fgamefsk             bigint,            --游戏ID
            fplatformfsk         bigint,            --平台ID
            fhallfsk             bigint,            --大厅ID
            fsubgamefsk          bigint,            --子游戏ID
            fterminaltypefsk     bigint,            --终端ID
            fversionfsk          bigint,            --版本ID
            fchannelcode         bigint,            --渠道ID
            fuid                 bigint,            --用户ID
            flogin_cnt           int,               --登录次数
            fparty_num           int,               --牌局数
            fis_change_gamecoins tinyint            --金流是否发生变化
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):

        extend_group = {
            'fields':['fuid'],
            'groups':[[1],  ]
        }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """drop table if exists work.user_act_array_tmp_%(statdatenum)s;
        create table work.user_act_array_tmp_%(statdatenum)s as
        select
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code,
            fuid,
            flogin_cnt,
            fparty_num,
            fis_change_gamecoins,
            b.hallmode
        from dim.user_act a
        join dim.bpid_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert overwrite table dim.user_act_array partition (dt='%(statdate)s')
        select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fuid,
                sum(flogin_cnt) flogin_cnt,
                sum(fparty_num) fparty_num,
                max(fis_change_gamecoins) fis_change_gamecoins
        from work.user_act_array_tmp_%(statdatenum)s
        where fgame_id != -13658
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fchannel_code, fuid
        grouping sets (
            (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid),
            (fgamefsk,fplatformfsk,fhallfsk,fgame_id,fuid),
            (fgamefsk,fgame_id,fuid)
        )
        union all
        select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                %(null_int_group_rule)d fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fuid,
                sum(flogin_cnt) flogin_cnt,
                sum(fparty_num) fparty_num,
                max(fis_change_gamecoins) fis_change_gamecoins
        from work.user_act_array_tmp_%(statdatenum)s
        where fgame_id = -13658 and hallmode=1
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fchannel_code, fuid
        grouping sets (
            (fgamefsk,fplatformfsk,fhallfsk,fterminaltypefsk,fversionfsk,fuid),
            (fgamefsk,fplatformfsk,fhallfsk,fuid),
            (fgamefsk,fplatformfsk,fuid)
        )
        union all
        select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                %(null_int_group_rule)d fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fuid,
                sum(flogin_cnt) flogin_cnt,
                sum(fparty_num) fparty_num,
                max(fis_change_gamecoins) fis_change_gamecoins
        from work.user_act_array_tmp_%(statdatenum)s
        where fgame_id = -13658 and hallmode = 0
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fchannel_code, fuid
        grouping sets (
            (fgamefsk,fplatformfsk,fterminaltypefsk,fversionfsk,fuid),
            (fgamefsk,fplatformfsk,fuid)
        )
        """

        res = self.sql_exe(hql)
        if res != 0: return res

        hql = """
        drop table if exists work.user_act_array_tmp_%(statdatenum)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


#生成统计实例
a = load_user_act_array(sys.argv[1:])
a()
