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


class agg_bud_subgame_card_room_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_subgame_card_room_info (
                fdate                          date,
                fgamefsk                       bigint,
                fplatformfsk                   bigint,
                fhallfsk                       bigint,
                fsubgamefsk                    bigint,
                fterminaltypefsk               bigint,
                fversionfsk                    bigint,
                fall_cardroom_game_num         bigint        comment '棋牌室累计开局房间数',
                fall_jt_cardroom_game_num      bigint        comment '棋牌室累计开局房间数（均摊）',
                fall_gj_cardroom_game_num      bigint        comment '棋牌室累计开局房间数（冠军）',
                fall_fz_cardroom_game_num      bigint        comment '棋牌室累计开局房间数（房主）',
                fall_cardroom_party_num        bigint        comment '棋牌室累计牌局数',
                fall_jt_cardroom_party_num     bigint        comment '棋牌室累计牌局数（均摊）',
                fall_gj_cardroom_party_num     bigint        comment '棋牌室累计牌局数（冠军）',
                fall_fz_cardroom_party_num     bigint        comment '棋牌室累计牌局数（房主）',
                fcardroom_game_num             bigint        comment '棋牌室累计开局房间数',
                fjt_cardroom_game_num          bigint        comment '棋牌室累计开局房间数（均摊）',
                fgj_cardroom_game_num          bigint        comment '棋牌室累计开局房间数（冠军）',
                ffz_cardroom_game_num          bigint        comment '棋牌室累计开局房间数（房主）',
                fcardroom_party_num            bigint        comment '棋牌室累计牌局数',
                fjt_cardroom_party_num         bigint        comment '棋牌室累计牌局数（均摊）',
                fgj_cardroom_party_num         bigint        comment '棋牌室累计牌局数（冠军）',
                ffz_cardroom_party_num         bigint        comment '棋牌室累计牌局数（房主）'
                )comment '棋牌室子游戏汇总'
                partitioned by(dt date)
        location '/dw/bud_dm/bud_subgame_card_room_info';
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
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_cardroom_game_num   --棋牌室累计开局房间数
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_jt_cardroom_game_num   --棋牌室累计开局房间数（均摊）
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_gj_cardroom_game_num   --棋牌室累计开局房间数（冠军）
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_fz_cardroom_game_num   --棋牌室累计开局房间数（房主）
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fcardroom_game_num  --棋牌室开局房间数
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fjt_cardroom_game_num  --棋牌室开局房间数（均摊）
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fgj_cardroom_game_num  --棋牌室开局房间数（冠军）
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) ffz_cardroom_game_num  --棋牌室开局房间数（房主）
                  ,cast (0 as bigint) fall_cardroom_party_num  --棋牌室累计牌局数
                  ,cast (0 as bigint) fall_jt_cardroom_party_num  --棋牌室累计牌局数（均摊）
                  ,cast (0 as bigint) fall_gj_cardroom_party_num  --棋牌室累计牌局数（冠军）
                  ,cast (0 as bigint) fall_fz_cardroom_party_num  --棋牌室累计牌局数（房主）
                  ,cast (0 as bigint) fcardroom_party_num  --棋牌室牌局数
                  ,cast (0 as bigint) fjt_cardroom_party_num  --棋牌室牌局数（均摊）
                  ,cast (0 as bigint) fgj_cardroom_party_num  --棋牌室牌局数（冠军）
                  ,cast (0 as bigint) ffz_cardroom_party_num  --棋牌室牌局数（房主）
             from dim.card_room_play t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
              and t.fsubname = '棋牌馆'
              and tt.hallmode = %(hallmode)s
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
            drop table if exists work.bud_subgame_card_room_info_tmp_%(statdatenum)s;
          create table work.bud_subgame_card_room_info_tmp_%(statdatenum)s as
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
                  ,0 fall_cardroom_game_num   --棋牌室累计开局房间数
                  ,0 fall_jt_cardroom_game_num   --棋牌室累计开局房间数（均摊）
                  ,0 fall_gj_cardroom_game_num   --棋牌室累计开局房间数（冠军）
                  ,0 fall_fz_cardroom_game_num   --棋牌室累计开局房间数（房主）
                  ,0 fcardroom_game_num  --棋牌室开局房间数
                  ,0 fjt_cardroom_game_num  --棋牌室开局房间数（均摊）
                  ,0 fgj_cardroom_game_num  --棋牌室开局房间数（冠军）
                  ,0 ffz_cardroom_game_num  --棋牌室开局房间数（房主）
                  ,sum(fparty_num) fall_cardroom_party_num  --棋牌室累计牌局数
                  ,sum(fparty_num) fall_jt_cardroom_party_num  --棋牌室累计牌局数（均摊）
                  ,sum(fparty_num) fall_gj_cardroom_party_num  --棋牌室累计牌局数（冠军）
                  ,sum(fparty_num) fall_fz_cardroom_party_num  --棋牌室累计牌局数（房主）
                  ,sum(case when dt = '%(statdate)s' then fparty_num end) fcardroom_party_num  --棋牌室牌局数
                  ,sum(case when dt = '%(statdate)s' then fparty_num end) fjt_cardroom_party_num  --棋牌室牌局数（均摊）
                  ,sum(case when dt = '%(statdate)s' then fparty_num end) fgj_cardroom_party_num  --棋牌室牌局数（冠军）
                  ,sum(case when dt = '%(statdate)s' then fparty_num end) ffz_cardroom_party_num  --棋牌室牌局数（房主）
             from dim.card_room_play t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
              and t.fsubname = '棋牌馆'
              and tt.hallmode = %(hallmode)s
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
            insert into table work.bud_subgame_card_room_info_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
          insert overwrite table bud_dm.bud_subgame_card_room_info partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t2.fgamefsk
                  ,t2.fplatformfsk
                  ,t2.fhallfsk
                  ,t2.fgame_id
                  ,t2.fterminaltypefsk
                  ,t2.fversionfsk
                  ,sum(fall_cardroom_game_num) fall_cardroom_game_num
                  ,sum(fall_jt_cardroom_game_num) fall_jt_cardroom_game_num
                  ,sum(fall_gj_cardroom_game_num) fall_gj_cardroom_game_num
                  ,sum(fall_fz_cardroom_game_num) fall_fz_cardroom_game_num
                  ,sum(fall_cardroom_party_num) fall_cardroom_party_num
                  ,sum(fall_jt_cardroom_party_num) fall_jt_cardroom_party_num
                  ,sum(fall_gj_cardroom_party_num) fall_gj_cardroom_party_num
                  ,sum(fall_fz_cardroom_party_num) fall_fz_cardroom_party_num
                  ,sum(fcardroom_game_num) fcardroom_game_num
                  ,sum(fjt_cardroom_game_num) fjt_cardroom_game_num
                  ,sum(fgj_cardroom_game_num) fgj_cardroom_game_num
                  ,sum(ffz_cardroom_game_num) ffz_cardroom_game_num
                  ,sum(fcardroom_party_num) fcardroom_party_num
                  ,sum(fjt_cardroom_party_num) fjt_cardroom_party_num
                  ,sum(fgj_cardroom_party_num) fgj_cardroom_party_num
                  ,sum(ffz_cardroom_party_num) ffz_cardroom_party_num
             from work.bud_subgame_card_room_info_tmp_%(statdatenum)s t2
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

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_subgame_card_room_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_subgame_card_room_info(sys.argv[1:])
a()
