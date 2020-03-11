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


class agg_bud_appoint_room_user_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_appoint_room_user_info (
                fdate                 string,
                fgamefsk              bigint,
                fplatformfsk          bigint,
                fhallfsk              bigint,
                fsubgamefsk           bigint,
                fterminaltypefsk      bigint,
                fversionfsk           bigint,
                fnew_room_unum        bigint     comment '新增用户',
                fnew_play_unum        bigint     comment '新增用户玩牌',
                fact_room_unum        bigint     comment '活跃用户',
                fact_play_unum        bigint     comment '活跃玩牌用户',
                fplay_room_unum       bigint     comment '玩牌玩家',
                fplay_room_cnt        bigint     comment '约牌房玩牌人次',
                fcreate_room_num      bigint     comment '约牌房开房次数',
                fplay_room_num        bigint     comment '约牌房开局房间数',
                fparty_room_num       bigint     comment '约牌房牌局数'
                )comment '约牌房信息'
                partitioned by(dt string)
        location '/dw/bud_dm/bud_appoint_room_user_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_appoint_room_user_info_tmp_1_%(statdatenum)s;
          create table work.bud_appoint_room_user_info_tmp_1_%(statdatenum)s as
           select fgamefsk
                  ,tt.fplatformfsk
                  ,tt.fhallfsk
                  ,t.fgame_id
                  ,tt.fterminaltypefsk
                  ,tt.fversionfsk
                  ,t.fuid
                  ,t.fis_first
                  ,t1.fuid play_uid
                  ,tt.hallmode
             from dim.enter_card_room t
             left join dim.card_room_party t1
               on t.fbpid = t1.fbpid
              and t.fuid = t1.fuid
              and t1.dt = '%(statdate)s'
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s'
              and t.fsubname = '约牌房'
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--棋牌室玩牌用户
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,count(distinct case when fis_first = 1 then fuid end) fnew_room_unum
                  ,count(distinct case when fis_first = 1 then play_uid end) fnew_play_unum
                  ,count(distinct fuid) fact_room_unum
                  ,count(distinct play_uid) fact_play_unum
                  ,cast (0 as bigint) fplay_room_unum
                  ,cast (0 as bigint) fplay_room_cnt
                  ,cast (0 as bigint) fcreate_room_num
                  ,cast (0 as bigint) fplay_room_num
                  ,cast (0 as bigint) fparty_room_num
             from work.bud_appoint_room_user_info_tmp_1_%(statdatenum)s
            where hallmode = %(hallmode)s
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fgame_id
                    ,fterminaltypefsk
                    ,fversionfsk
        """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """
            drop table if exists work.bud_appoint_room_user_info_tmp_%(statdatenum)s;
          create table work.bud_appoint_room_user_info_tmp_%(statdatenum)s as
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--约牌房
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,0 fnew_room_unum
                  ,0 fnew_play_unum
                  ,0 fact_room_unum
                  ,0 fact_play_unum
                  ,count(distinct fuid) fplay_room_unum
                  ,count(fuid) fplay_room_cnt
                  ,0 fcreate_room_num
                  ,0 fplay_room_num
                  ,count(distinct concat_ws('0', ftbl_id, finning_id)) fparty_room_num
             from dim.card_room_party t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fsubname = '约牌房' and tt.hallmode = %(hallmode)s
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fgame_id
                    ,fterminaltypefsk
                    ,fversionfsk
        """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """
            insert into table work.bud_appoint_room_user_info_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室用户
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,0 fnew_room_unum
                  ,0 fnew_play_unum
                  ,0 fact_room_unum
                  ,0 fact_play_unum
                  ,0 fplay_room_unum
                  ,0 fplay_room_cnt
                  ,count(distinct froom_id) fcreate_room_num
                  ,count(distinct case when fparty_num >0 then froom_id end) fplay_room_num
                  ,0 fparty_room_num
             from dim.card_room_play t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fsubname = '约牌房' and tt.hallmode = %(hallmode)s
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fgame_id
                    ,fterminaltypefsk
                    ,fversionfsk
        """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """
            insert into table work.bud_appoint_room_user_info_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
          insert overwrite table bud_dm.bud_appoint_room_user_info partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t1.fgamefsk
                  ,t1.fplatformfsk
                  ,t1.fhallfsk
                  ,t1.fgame_id fsubgamefsk
                  ,t1.fterminaltypefsk
                  ,t1.fversionfsk
                  ,sum(fnew_room_unum) fnew_room_unum
                  ,sum(fnew_play_unum) fnew_play_unum
                  ,sum(fact_room_unum) fact_room_unum
                  ,sum(fact_play_unum) fact_play_unum
                  ,sum(fplay_room_unum) fplay_room_unum
                  ,sum(fplay_room_cnt) fplay_room_cnt
                  ,sum(fcreate_room_num) fcreate_room_num
                  ,sum(fplay_room_num) fplay_room_num
                  ,sum(fparty_room_num) fparty_room_num
             from work.bud_appoint_room_user_info_tmp_%(statdatenum)s t1
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fgame_id
                    ,fterminaltypefsk
                    ,fversionfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_appoint_room_user_info(sys.argv[1:])
a()
