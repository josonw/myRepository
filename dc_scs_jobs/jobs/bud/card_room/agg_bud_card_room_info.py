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


class agg_bud_card_room_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_info (
                fdate                     date,
                fgamefsk                  bigint,
                fplatformfsk              bigint,
                fhallfsk                  bigint,
                fsubgamefsk               bigint,
                fterminaltypefsk          bigint,
                fversionfsk               bigint,
                fnew_cardroom_unum        bigint    comment '棋牌室新增用户',
                fcardroom_unum            bigint    comment '棋牌室累计用户',
                fplay_cardroom_unum       bigint    comment '棋牌室玩牌用户',
                fnew_paly_cardroom_unum   bigint    comment '棋牌室新增玩牌用户',
                fact_cardroom_unum        bigint    comment '棋牌室活跃用户',
                ff_create_unum            bigint    comment '首次创建棋牌室用户',
                fall_create_unum          bigint    comment '累计创建棋牌室用户',
                fnew_cardroom_num         bigint    comment '新增棋牌室数',
                fdis_cardroom_num         bigint    comment '解散棋牌室数',
                ffbd_cardroom_num         bigint    comment '禁用棋牌室数',
                fall_cardroom_num         bigint    comment '棋牌室总数',
                fplay_cardroom_num        bigint    comment '棋牌室玩牌人次',
                fcardroom_begin_num       bigint    comment '棋牌室开房次数',
                fcardroom_game_num        bigint    comment '棋牌室开局房间数',
                fcardroom_party_num       bigint    comment '棋牌室牌局数'
                )comment '棋牌室'
                partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_info';
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
        hql = """--棋牌室数量
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,cast(0 as bigint) fnew_cardroom_unum       --棋牌室新增用户
                  ,cast(0 as bigint) fcardroom_unum           --棋牌室累计用户
                  ,cast(0 as bigint) fplay_cardroom_unum      --棋牌室玩牌用户
                  ,cast(0 as bigint) fnew_paly_cardroom_unum  --棋牌室新增玩牌用户
                  ,cast(0 as bigint) fact_cardroom_unum       --棋牌室活跃用户
                  ,count(distinct case when fact_id = 1 and fis_first = 1 and t.dt = '%(statdate)s' then fpartner_uid end) ff_create_unum           --首次创建棋牌室用户
                  ,count(case when fact_id = 1 then fpartner_uid end) fall_create_unum         --累计创建棋牌室用户
                  ,count(distinct case when t.dt = '%(statdate)s' and fact_id = 1 then froom_id end) fnew_cardroom_num        --新增棋牌室数
                  ,count(distinct case when t.dt = '%(statdate)s' and fact_id = 2 then froom_id end) fdis_cardroom_num        --解散棋牌室数
                  ,count(distinct case when t.dt = '%(statdate)s' and fact_id = 3 then froom_id end) ffbd_cardroom_num        --禁用棋牌室数
                  ,count(distinct case when fact_id = 1 then froom_id end) fall_cardroom_num        --棋牌室总数
                  ,cast(0 as bigint) fplay_cardroom_num       --棋牌室玩牌人次
                  ,cast(0 as bigint) fcardroom_begin_num      --棋牌室开房次数
                  ,cast(0 as bigint) fcardroom_game_num       --棋牌室开局房间数
                  ,cast(0 as bigint) fcardroom_party_num      --棋牌室牌局数
             from dim.card_room_partner_act t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s' and tt.hallmode = %(hallmode)s
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
            drop table if exists work.bud_card_room_info_tmp_%(statdatenum)s;
          create table work.bud_card_room_info_tmp_%(statdatenum)s as
          %(sql_template)s;
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
                  ,0 fnew_cardroom_unum       --棋牌室新增用户
                  ,0 fcardroom_unum           --棋牌室累计用户
                  ,count(distinct case when t.dt = '%(statdate)s' then t.fuid end) fplay_cardroom_unum      --棋牌室玩牌用户
                  ,count(distinct case when t.dt = '%(statdate)s' and t1.fuid is not null then t.fuid end) fnew_paly_cardroom_unum  --棋牌室新增玩牌用户
                  ,0 fact_cardroom_unum       --棋牌室活跃用户
                  ,0 ff_create_unum           --首次创建棋牌室用户
                  ,0 fall_create_unum         --累计创建棋牌室用户
                  ,0 fnew_cardroom_num        --新增棋牌室数
                  ,0 fdis_cardroom_num        --解散棋牌室数
                  ,0 ffbd_cardroom_num        --禁用棋牌室数
                  ,0 fall_cardroom_num        --棋牌室总数
                  ,0 fplay_cardroom_num       --棋牌室玩牌人次
                  ,0 fcardroom_begin_num      --棋牌室开房次数
                  ,0 fcardroom_game_num       --棋牌室开局房间数
                  ,0 fcardroom_party_num      --棋牌室牌局数
             from dim.card_room_party t
             left join (select distinct fbpid,fuid
                          from dim.join_exit_room t1
                         where t1.fjoin_dt = '%(statdate)s'
                           and t1.dt = '%(statdate)s'
                       ) t1
               on t.fbpid = t1.fbpid
              and t.fuid = t1.fuid
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fsubname = '棋牌馆' and tt.hallmode = %(hallmode)s
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
            insert into table work.bud_card_room_info_tmp_%(statdatenum)s
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
                  ,0 fnew_cardroom_unum       --棋牌室新增用户
                  ,0 fcardroom_unum           --棋牌室累计用户
                  ,0 fplay_cardroom_unum      --棋牌室玩牌用户
                  ,0 fnew_paly_cardroom_unum  --棋牌室新增玩牌用户
                  ,count(distinct fuid) fact_cardroom_unum       --棋牌室活跃用户
                  ,0 ff_create_unum           --首次创建棋牌室用户
                  ,0 fall_create_unum         --累计创建棋牌室用户
                  ,0 fnew_cardroom_num        --新增棋牌室数
                  ,0 fdis_cardroom_num        --解散棋牌室数
                  ,0 ffbd_cardroom_num        --禁用棋牌室数
                  ,0 fall_cardroom_num        --棋牌室总数
                  ,0 fplay_cardroom_num       --棋牌室玩牌人次
                  ,0 fcardroom_begin_num      --棋牌室开房次数
                  ,0 fcardroom_game_num       --棋牌室开局房间数
                  ,0 fcardroom_party_num      --棋牌室牌局数
             from dim.enter_card_room t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fsubname = '棋牌馆' and tt.hallmode = %(hallmode)s
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
            insert into table work.bud_card_room_info_tmp_%(statdatenum)s
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
                  ,count(distinct case when fjoin_dt = '%(statdate)s' then fuid end) fnew_cardroom_unum       --棋牌室新增用户
                  ,count(distinct case when fstatus = 1 then fuid end) fcardroom_unum           --棋牌室累计用户
                  ,0 fplay_cardroom_unum      --棋牌室玩牌用户
                  ,0 fnew_paly_cardroom_unum  --棋牌室新增玩牌用户
                  ,0 fact_cardroom_unum       --棋牌室活跃用户
                  ,0 ff_create_unum           --首次创建棋牌室用户
                  ,0 fall_create_unum         --累计创建棋牌室用户
                  ,0 fnew_cardroom_num        --新增棋牌室数
                  ,0 fdis_cardroom_num        --解散棋牌室数
                  ,0 ffbd_cardroom_num        --禁用棋牌室数
                  ,0 fall_cardroom_num        --棋牌室总数
                  ,0 fplay_cardroom_num       --棋牌室玩牌人次
                  ,0 fcardroom_begin_num      --棋牌室开房次数
                  ,0 fcardroom_game_num       --棋牌室开局房间数
                  ,0 fcardroom_party_num      --棋牌室牌局数
             from dim.join_exit_room t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and tt.hallmode = %(hallmode)s
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
            insert into table work.bud_card_room_info_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,0 fnew_cardroom_unum       --棋牌室新增用户
                  ,0 fcardroom_unum           --棋牌室累计用户
                  ,0 fplay_cardroom_unum      --棋牌室玩牌用户
                  ,0 fnew_paly_cardroom_unum  --棋牌室新增玩牌用户
                  ,0 fact_cardroom_unum       --棋牌室活跃用户
                  ,0 ff_create_unum           --首次创建棋牌室用户
                  ,0 fall_create_unum         --累计创建棋牌室用户
                  ,0 fnew_cardroom_num        --新增棋牌室数
                  ,0 fdis_cardroom_num        --解散棋牌室数
                  ,0 ffbd_cardroom_num        --禁用棋牌室数
                  ,0 fall_cardroom_num        --棋牌室总数
                  ,0 fplay_cardroom_num       --棋牌室玩牌人次
                  ,count(distinct case when fparty_num > 0 then froom_id end) fcardroom_begin_num      --棋牌室开房次数
                  ,count(distinct froom_id) fcardroom_game_num       --棋牌室开局房间数
                  ,sum(fparty_num) fcardroom_party_num      --棋牌室牌局数
             from dim.card_room_play t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fsubname = '棋牌馆' and tt.hallmode = %(hallmode)s
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
            insert into table work.bud_card_room_info_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,0 fnew_cardroom_unum       --棋牌室新增用户
                  ,0 fcardroom_unum           --棋牌室累计用户
                  ,0 fplay_cardroom_unum      --棋牌室玩牌用户
                  ,0 fnew_paly_cardroom_unum  --棋牌室新增玩牌用户
                  ,0 fact_cardroom_unum       --棋牌室活跃用户
                  ,0 ff_create_unum           --首次创建棋牌室用户
                  ,0 fall_create_unum         --累计创建棋牌室用户
                  ,0 fnew_cardroom_num        --新增棋牌室数
                  ,0 fdis_cardroom_num        --解散棋牌室数
                  ,0 ffbd_cardroom_num        --禁用棋牌室数
                  ,0 fall_cardroom_num        --棋牌室总数
                  ,count(fuid) fplay_cardroom_num       --棋牌室玩牌人次
                  ,0 fcardroom_begin_num      --棋牌室开房次数
                  ,0 fcardroom_game_num       --棋牌室开局房间数
                  ,0 fcardroom_party_num      --棋牌室牌局数
             from dim.card_room_party t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fsubname = '棋牌馆' and tt.hallmode = %(hallmode)s
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
            insert into table work.bud_card_room_info_tmp_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
          insert overwrite table bud_dm.bud_card_room_info partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t1.fgamefsk
                  ,t1.fplatformfsk
                  ,t1.fhallfsk
                  ,t1.fgame_id fsubgamefsk
                  ,t1.fterminaltypefsk
                  ,t1.fversionfsk
                  ,sum(t1.fnew_cardroom_unum) fnew_cardroom_unum
                  ,sum(t1.fcardroom_unum) fcardroom_unum
                  ,sum(t1.fplay_cardroom_unum) fplay_cardroom_unum
                  ,sum(t1.fnew_paly_cardroom_unum) fnew_paly_cardroom_unum
                  ,sum(t1.fact_cardroom_unum) fact_cardroom_unum
                  ,sum(t1.ff_create_unum) ff_create_unum
                  ,sum(t1.fall_create_unum) fall_create_unum
                  ,sum(t1.fnew_cardroom_num) fnew_cardroom_num
                  ,sum(t1.fdis_cardroom_num) fdis_cardroom_num
                  ,sum(t1.ffbd_cardroom_num) ffbd_cardroom_num
                  ,sum(t1.fall_cardroom_num) fall_cardroom_num
                  ,sum(t1.fplay_cardroom_num) fplay_cardroom_num
                  ,sum(t1.fcardroom_begin_num) fcardroom_begin_num
                  ,sum(t1.fcardroom_game_num) fcardroom_game_num
                  ,sum(t1.fcardroom_party_num) fcardroom_party_num
             from work.bud_card_room_info_tmp_%(statdatenum)s t1
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
        hql = """drop table if exists work.bud_card_room_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_card_room_info(sys.argv[1:])
a()
