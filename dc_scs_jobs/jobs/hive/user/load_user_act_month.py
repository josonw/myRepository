# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_act_month(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dim.user_act_month (
                fbpid                    varchar(50)     comment 'BPID',
                fgamefsk                 bigint          comment '游戏id',
                fgamename                string          comment '游戏名称',
                fplatformfsk             bigint          comment '平台id',
                fplatformname            string          comment '平台名称',
                fhallfsk                 bigint          comment '大厅id',
                fhallname                string          comment '大厅名称',
                fterminaltypefsk         bigint          comment '终端类型id',
                fterminaltypename        string          comment '终端类型名称',
                fversionfsk              bigint          comment '版本id',
                fversionname             string          comment '版本名称',
                hallmode                 smallint        comment '大厅模式',
                fgame_id                 bigint          comment '子游戏ID',
                fchannel_code            bigint          comment '渠道ID',
                fuid                     bigint          comment '用户ID',
                fall_login_cnt           bigint          comment '总登录次数',
                fall_party_num           bigint          comment '总牌局数',
                fall_is_change_gamecoins bigint          comment '金流是否发生变化'
               )comment '用户活跃月表'
               partitioned by(dt string);

        create table if not exists dim.user_act_month_array (
                fgamefsk                 bigint          comment '游戏id',
                fplatformfsk             bigint          comment '平台id',
                fhallfsk                 bigint          comment '大厅id',
                fgame_id                 bigint          comment '子游戏ID',
                fterminaltypefsk         bigint          comment '终端类型id',
                fversionfsk              bigint          comment '版本id',
                fuid                     bigint          comment '用户ID',
                fall_login_cnt           bigint          comment '总登录次数',
                fall_party_num           bigint          comment '总牌局数',
                fall_is_change_gamecoins bigint          comment '金流是否发生变化'
               )comment '用户活跃月表'
               partitioned by(dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {
            'fields': ['fuid'],
            'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--
        insert overwrite table dim.user_act_month partition ( dt='%(ld_month_begin)s' )
        select t.fbpid
               ,tt.fgamefsk
               ,tt.fgamename
               ,tt.fplatformfsk
               ,tt.fplatformname
               ,tt.fhallfsk
               ,tt.fhallname
               ,tt.fterminaltypefsk
               ,tt.fterminaltypename
               ,tt.fversionfsk
               ,tt.fversionname
               ,tt.hallmode
               ,t.fgame_id
               ,t.fchannel_code
               ,t.fuid
               ,sum(flogin_cnt) fall_login_cnt
               ,sum(fparty_num) fall_party_num
               ,max(fis_change_gamecoins) fall_is_change_gamecoins
          from dim.user_act t
          join dim.bpid_map tt
            on t.fbpid = tt.fbpid
         where t.dt >= '%(ld_month_begin)s'
           and t.dt <= '%(ld_month_end)s'
        group by t.fbpid
               ,tt.fgamefsk
               ,tt.fgamename
               ,tt.fplatformfsk
               ,tt.fplatformname
               ,tt.fhallfsk
               ,tt.fhallname
               ,tt.fterminaltypefsk
               ,tt.fterminaltypename
               ,tt.fversionfsk
               ,tt.fversionname
               ,tt.hallmode
               ,t.fgame_id
               ,t.fchannel_code
               ,t.fuid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
          select fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuid
                 ,sum(fall_login_cnt) fall_login_cnt
                 ,sum(fall_party_num) fall_party_num
                 ,max(fall_is_change_gamecoins) fall_is_change_gamecoins
            from dim.user_act_month t
           where hallmode=%(hallmode)s
             and t.dt = '%(ld_month_begin)s'
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                 ,fuid
         """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dim.user_act_month_array partition ( dt='%(ld_month_begin)s' )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_act_month(sys.argv[1:])
a()
