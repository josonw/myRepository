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


class agg_bud_platform_card_room_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_platform_card_room_info (
                fdate                          date,
                fgamefsk                       bigint,
                fplatformfsk                   bigint,
                fhallfsk                       bigint,
                fsubgamefsk                    bigint,
                fterminaltypefsk               bigint,
                fversionfsk                    bigint,
                fpromoter_unum       bigint     comment '推广员人数',
                fpartner_unum        bigint     comment '代理商人数',
                fall_unum            bigint     comment '棋牌室用户',
                fcard_room_num       bigint     comment '棋牌室总数',
                fgame_num            bigint     comment '棋牌室累计开局房间数',
                fbuy_card_num        bigint     comment '代理商累计购买房卡数',
                fbuy_card_cnt        bigint     comment '代理商累计购买房卡次数',
                fgive_card_num       bigint     comment '代理商累计赠送房卡数',
                fleft_card_num       bigint     comment '代理商剩余房卡数',
                fuser_card_num       bigint     comment '累计消耗房卡数',
                fuser_left_card_num  bigint     comment '剩余房卡数'
                )comment '平台总数据'
                partitioned by(dt date)
        location '/dw/bud_dm/bud_platform_card_room_info';
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
        hql = """--新增\累计代理商
            drop table if exists work.bud_platform_card_room_info_tmp_1_%(statdatenum)s;
          create table work.bud_platform_card_room_info_tmp_1_%(statdatenum)s as
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,%(null_int_group_rule)d fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,count(distinct fpromoter) fpromoter_unum       --推广员人数
                  ,count(distinct fpartner_uid) fpartner_unum     --代理商人数
                  ,cast(0 as bigint) fall_unum                    --棋牌室用户
                  ,cast(0 as bigint) fcard_room_num               --棋牌室总数
                  ,cast(0 as bigint) fgame_num                    --棋牌室累计开局房间数
                  ,cast(0 as bigint) fbuy_card_num                --代理商累计购买房卡数
                  ,cast(0 as bigint) fbuy_card_cnt                --代理商累计购买房卡次数
                  ,cast(0 as bigint) fgive_card_num               --代理商累计赠送房卡数
                  ,cast(0 as bigint) fleft_card_num               --代理商剩余房卡数
                  ,cast(0 as bigint) fuser_card_num               --累计消耗房卡数
                  ,cast(0 as bigint) fuser_left_card_num          --剩余房卡数
             from dim.card_room_partner_new t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fterminaltypefsk
                    ,fversionfsk
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) )
         """

        res = self.sql_exe(hql)
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
                  ,0 fpromoter_unum       --推广员人数
                  ,0 fpartner_unum     --代理商人数
                  ,0 fall_unum                    --棋牌室用户
                  ,count(distinct case when fact_id = 1 then froom_id end) fcard_room_num   --所有棋牌室数量
                  ,0 fgame_num                    --棋牌室累计开局房间数
                  ,0 fbuy_card_num                --代理商累计购买房卡数
                  ,0 fbuy_card_cnt                --代理商累计购买房卡次数
                  ,0 fgive_card_num               --代理商累计赠送房卡数
                  ,0 fleft_card_num               --代理商剩余房卡数
                  ,0 fuser_card_num               --累计消耗房卡数
                  ,0 fuser_left_card_num          --剩余房卡数
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
            insert into table work.bud_platform_card_room_info_tmp_1_%(statdatenum)s
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
                  ,0 fpromoter_unum       --推广员人数
                  ,0 fpartner_unum     --代理商人数
                  ,count(distinct case when fstatus = 1 then fuid end) fall_unum                    --棋牌室用户
                  ,0 fcard_room_num   --所有棋牌室数量
                  ,0 fgame_num                    --棋牌室累计开局房间数
                  ,0 fbuy_card_num                --代理商累计购买房卡数
                  ,0 fbuy_card_cnt                --代理商累计购买房卡次数
                  ,0 fgive_card_num               --代理商累计赠送房卡数
                  ,0 fleft_card_num               --代理商剩余房卡数
                  ,0 fuser_card_num               --累计消耗房卡数
                  ,0 fuser_left_card_num          --剩余房卡数
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
            insert into table work.bud_platform_card_room_info_tmp_1_%(statdatenum)s
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
                  ,0 fpromoter_unum       --推广员人数
                  ,0 fpartner_unum     --代理商人数
                  ,0 fall_unum                    --棋牌室用户
                  ,0 fcard_room_num   --所有棋牌室数量
                  ,count(distinct case when fparty_num > 0 then froom_id end) fgame_num                    --棋牌室累计开局房间数
                  ,0 fbuy_card_num                --代理商累计购买房卡数
                  ,0 fbuy_card_cnt                --代理商累计购买房卡次数
                  ,0 fgive_card_num               --代理商累计赠送房卡数
                  ,0 fleft_card_num               --代理商剩余房卡数
                  ,0 fuser_card_num               --累计消耗房卡数
                  ,0 fuser_left_card_num          --剩余房卡数
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
            insert into table work.bud_platform_card_room_info_tmp_1_%(statdatenum)s
          %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--充值（购买房卡）
            insert into table work.bud_platform_card_room_info_tmp_1_%(statdatenum)s
           select fgamefsk
                  ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                  ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                  ,%(null_int_group_rule)d fgame_id
                  ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                  ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                  ,0 fpromoter_unum       --推广员人数
                  ,0 fpartner_unum     --代理商人数
                  ,0 fall_unum                    --棋牌室用户
                  ,0 fcard_room_num   --所有棋牌室数量
                  ,0 fgame_num                    --棋牌室累计开局房间数
                  ,sum(fcard_num) fbuy_card_num                --代理商累计购买房卡数
                  ,sum(fcard_cnt) fbuy_card_cnt                --代理商累计购买房卡次数
                  ,0 fgive_card_num               --代理商累计赠送房卡数
                  ,0 fleft_card_num               --代理商剩余房卡数
                  ,0 fuser_card_num               --累计消耗房卡数
                  ,0 fuser_left_card_num          --剩余房卡数
             from dim.card_room_partner_pay t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s' and tt.hallmode = %(hallmode)s
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fterminaltypefsk
                    ,fversionfsk
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) )
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--棋牌室汇总
          insert overwrite table bud_dm.bud_platform_card_room_info partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t1.fgamefsk
                  ,t1.fplatformfsk
                  ,t1.fhallfsk
                  ,t1.fgame_id fsubgamefsk
                  ,t1.fterminaltypefsk
                  ,t1.fversionfsk
                  ,sum(fpromoter_unum) fpromoter_unum
                  ,sum(fpartner_unum) fpartner_unum
                  ,sum(fall_unum) fall_unum
                  ,sum(fcard_room_num) fcard_room_num
                  ,sum(fgame_num) fgame_num
                  ,sum(fbuy_card_num) fbuy_card_num
                  ,sum(fbuy_card_cnt) fbuy_card_cnt
                  ,sum(fgive_card_num) fgive_card_num
                  ,sum(fleft_card_num) fleft_card_num
                  ,sum(fuser_card_num) fuser_card_num
                  ,sum(fuser_left_card_num) fuser_left_card_num
             from work.bud_platform_card_room_info_tmp_1_%(statdatenum)s t1
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
        hql = """drop table if exists work.bud_platform_card_room_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_platform_card_room_info(sys.argv[1:])
a()
