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


class load_user_act_week(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dim.user_act_week (
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
                fall_is_change_gamecoins bigint          comment '金流是否发生变化',
                f1_login_cnt             bigint          comment '第1天总登录次数',
                f1_party_num             bigint          comment '第1天总牌局数',
                f1_is_change_gamecoins   bigint          comment '第1天金流是否发生变化',
                f2_login_cnt             bigint          comment '第2天总登录次数',
                f2_party_num             bigint          comment '第2天总牌局数',
                f2_is_change_gamecoins   bigint          comment '第2天金流是否发生变化',
                f3_login_cnt             bigint          comment '第3天总登录次数',
                f3_party_num             bigint          comment '第3天总牌局数',
                f3_is_change_gamecoins   bigint          comment '第3天金流是否发生变化',
                f4_login_cnt             bigint          comment '第4天总登录次数',
                f4_party_num             bigint          comment '第4天总牌局数',
                f4_is_change_gamecoins   bigint          comment '第4天金流是否发生变化',
                f5_login_cnt             bigint          comment '第5天总登录次数',
                f5_party_num             bigint          comment '第5天总牌局数',
                f5_is_change_gamecoins   bigint          comment '第5天金流是否发生变化',
                f6_login_cnt             bigint          comment '第6天总登录次数',
                f6_party_num             bigint          comment '第6天总牌局数',
                f6_is_change_gamecoins   bigint          comment '第6天金流是否发生变化',
                f7_login_cnt             bigint          comment '第7天总登录次数',
                f7_party_num             bigint          comment '第7天总牌局数',
                f7_is_change_gamecoins   bigint          comment '第7天金流是否发生变化'
               )comment '用户活跃周表'
               partitioned by(dt string);

        create table if not exists dim.user_act_week_array (
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
               )comment '用户活跃周表'
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

        hql = """
        insert overwrite table dim.user_act_week partition ( dt='%(ld_week_begin)s' )
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
               ,sum(case when dt = '%(ld_week_begin)s' then flogin_cnt end) f1_login_cnt
               ,sum(case when dt = '%(ld_week_begin)s' then fparty_num end) f1_party_num
               ,max(case when dt = '%(ld_week_begin)s' then fis_change_gamecoins end) f1_is_change_gamecoins
               ,sum(case when dt = date_add('%(ld_week_begin)s',1) then flogin_cnt end) f2_login_cnt
               ,sum(case when dt = date_add('%(ld_week_begin)s',1) then fparty_num end) f2_party_num
               ,max(case when dt = date_add('%(ld_week_begin)s',1) then fis_change_gamecoins end) f2_is_change_gamecoins
               ,sum(case when dt = date_add('%(ld_week_begin)s',2) then flogin_cnt end) f3_login_cnt
               ,sum(case when dt = date_add('%(ld_week_begin)s',2) then fparty_num end) f3_party_num
               ,max(case when dt = date_add('%(ld_week_begin)s',2) then fis_change_gamecoins end) f3_is_change_gamecoins
               ,sum(case when dt = date_add('%(ld_week_begin)s',3) then flogin_cnt end) f4_login_cnt
               ,sum(case when dt = date_add('%(ld_week_begin)s',3) then fparty_num end) f4_party_num
               ,max(case when dt = date_add('%(ld_week_begin)s',3) then fis_change_gamecoins end) f4_is_change_gamecoins
               ,sum(case when dt = date_add('%(ld_week_begin)s',4) then flogin_cnt end) f5_login_cnt
               ,sum(case when dt = date_add('%(ld_week_begin)s',4) then fparty_num end) f5_party_num
               ,max(case when dt = date_add('%(ld_week_begin)s',4) then fis_change_gamecoins end) f5_is_change_gamecoins
               ,sum(case when dt = date_add('%(ld_week_begin)s',5) then flogin_cnt end) f6_login_cnt
               ,sum(case when dt = date_add('%(ld_week_begin)s',5) then fparty_num end) f6_party_num
               ,max(case when dt = date_add('%(ld_week_begin)s',5) then fis_change_gamecoins end) f6_is_change_gamecoins
               ,sum(case when dt = date_add('%(ld_week_begin)s',6) then flogin_cnt end) f7_login_cnt
               ,sum(case when dt = date_add('%(ld_week_begin)s',6) then fparty_num end) f7_party_num
               ,max(case when dt = date_add('%(ld_week_begin)s',6) then fis_change_gamecoins end) f7_is_change_gamecoins
          from dim.user_act t
          join dim.bpid_map tt
            on t.fbpid = tt.fbpid
         where t.dt between '%(ld_week_begin)s' and date_add('%(ld_week_begin)s',6)
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
        insert overwrite table dim.user_act_week_array partition ( dt='%(ld_week_begin)s' )
        select fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                fuid,
                sum(fall_login_cnt) fall_login_cnt,
                sum(fall_party_num) fall_party_num,
                max(fall_is_change_gamecoins) fall_is_change_gamecoins
            from dim.user_act_week t
        where fgame_id != -13658 and dt='%(ld_week_begin)s'
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fuid
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
                fuid,
                sum(fall_login_cnt) fall_login_cnt,
                sum(fall_party_num) fall_party_num,
                max(fall_is_change_gamecoins) fall_is_change_gamecoins
            from dim.user_act_week t
        where fgame_id = -13658 and hallmode=1 and dt='%(ld_week_begin)s'
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fuid
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
                fuid,
                sum(fall_login_cnt) fall_login_cnt,
                sum(fall_party_num) fall_party_num,
                max(fall_is_change_gamecoins) fall_is_change_gamecoins
            from dim.user_act_week t
        where fgame_id = -13658 and hallmode = 0 and dt='%(ld_week_begin)s'
        group by fgamefsk,fplatformfsk,fhallfsk, fgame_id,fterminaltypefsk,fversionfsk, fuid
        grouping sets (
            (fgamefsk,fplatformfsk,fterminaltypefsk,fversionfsk,fuid),
            (fgamefsk,fplatformfsk,fuid)
        )
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_user_act_week(sys.argv[1:])
a()
