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


class agg_bud_card_room_partner_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_partner_detail (
                fdate                      date,
                fgamefsk                   bigint,
                fplatformfsk               bigint,
                fhallfsk                   bigint,
                fsubgamefsk                bigint,
                fterminaltypefsk           bigint,
                fversionfsk                bigint,
                fpartner_uid               bigint            comment '代理商mid',
                fpartner_level             bigint            comment '群主等级',
                fpromoter                  varchar(50)       comment '推广员ID（员工）',
                fall_cardroom_num          bigint            comment '棋牌室数量',
                fall_cardroom_unum         bigint            comment '棋牌室用户',
                fall_cardroom_play_unum    bigint            comment '棋牌室累计玩牌用户',
                fall_cardroom_game_num     bigint            comment '棋牌室累计开局房间数',
                fall_cardroom_party_num    bigint            comment '棋牌室累计牌局数',
                fall_left_room_num         bigint            comment '剩余开房局数',
                fall_card_income           decimal(20,4)     comment '累计充值总额（购买房卡）',
                fall_give_card_num         bigint            comment '累计赠送房卡数',
                fall_left_card_num         bigint            comment '剩余房卡数',
                fpartner_time              varchar(50)       comment '权限开通时间',
                fnew_cardroom_unum         bigint            comment '棋牌室新增用户',
                fplay_cardroom_unum        bigint            comment '棋牌室玩牌用户',
                fcardroom_begin_num        bigint            comment '棋牌室开局房间数',
                fcardroom_partynum         bigint            comment '棋牌室牌局数'
                )comment '棋牌室代理商（群主）明细'
        partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_partner_detail';
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
            drop table if exists work.bud_card_room_partner_detail_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_1_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpartner_uid
                  ,fpromoter
                  ,flts_at fpartner_time
             from dim.card_room_partner_new t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--等级、剩余次数
            drop table if exists work.bud_card_room_partner_detail_tmp_2_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_2_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpartner_uid
                  ,fgame_level fpartner_level
                  ,fleft_num fall_left_room_num
             from dim.card_room_partner_level t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--棋牌室玩牌用户
            drop table if exists work.bud_card_room_partner_detail_tmp_3_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_3_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpartner_uid
                  ,count(distinct fuid) fall_cardroom_play_unum   --棋牌室累计玩牌用户
                  ,count(distinct case when t.dt = '%(statdate)s' then fuid end) fplay_cardroom_unum --棋牌室当日玩牌用户
             from dim.card_room_party t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
              and fsubname = '棋牌馆'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpartner_uid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--棋牌室数量
            drop table if exists work.bud_card_room_partner_detail_tmp_4_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_4_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpartner_uid
                  ,count(distinct case when fact_id = 1 then froom_id end) fall_cardroom_num   --所有棋牌室数量
             from dim.card_room_partner_act t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpartner_uid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室用户
            drop table if exists work.bud_card_room_partner_detail_tmp_5_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_5_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,froom_promoter_id fpartner_uid
                  ,count(distinct case when fstatus = 1 then fuid end) fall_cardroom_unum      --棋牌室用户
                  ,count(distinct case when fjoin_dt = '%(statdate)s' then fuid end) fnew_cardroom_unum   --新增棋牌室用户
             from dim.join_exit_room t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,froom_promoter_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
            drop table if exists work.bud_card_room_partner_detail_tmp_6_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_6_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpartner_uid
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fact_partner_num           --有效活跃代理商
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_cardroom_game_num   --棋牌室累计开局房间数
                  ,sum(fparty_num) fall_cardroom_party_num  --棋牌室累计牌局数
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fcardroom_begin_num  --棋牌室成功开房次数
                  ,sum(case when dt = '%(statdate)s' then fparty_num end) fcardroom_partynum  --棋牌室玩牌局数
             from dim.card_room_play t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
              and fsubname = '棋牌馆'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpartner_uid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--充值（购买房卡）
            drop table if exists work.bud_card_room_partner_detail_tmp_7_%(statdatenum)s;
          create table work.bud_card_room_partner_detail_tmp_7_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpartner_uid
                  ,sum(fcard_income) fall_card_income --累计充值总额（购买房卡）
                  ,sum(case when t.dt = '%(statdate)s' then fcard_income end) fcard_income     --充值总额（购买房卡）
             from dim.card_room_partner_pay t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpartner_uid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
          insert overwrite table bud_dm.bud_card_room_partner_detail partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t1.fgamefsk
                  ,t1.fplatformfsk
                  ,t1.fhallfsk
                  ,t1.fsubgamefsk
                  ,t1.fterminaltypefsk
                  ,t1.fversionfsk
                  ,t1.fpartner_uid
                  ,t2.fpartner_level
                  ,t1.fpromoter
                  ,coalesce(t4.fall_cardroom_num, 0) fall_cardroom_num
                  ,coalesce(t5.fall_cardroom_unum, 0) fall_cardroom_unum
                  ,coalesce(t3.fall_cardroom_play_unum, 0) fall_cardroom_play_unum
                  ,coalesce(t6.fall_cardroom_game_num, 0) fall_cardroom_game_num
                  ,coalesce(t6.fall_cardroom_party_num, 0) fall_cardroom_party_num
                  ,coalesce(t2.fall_left_room_num, 0) fall_left_room_num
                  ,coalesce(t7.fall_card_income, 0) fall_card_income
                  ,0 fall_give_card_num
                  ,0 fall_left_card_num
                  ,coalesce(t1.fpartner_time, '0') fpartner_time
                  ,coalesce(t5.fnew_cardroom_unum, 0) fnew_cardroom_unum
                  ,coalesce(t3.fplay_cardroom_unum, 0) fplay_cardroom_unum
                  ,coalesce(t6.fcardroom_begin_num, 0) fcardroom_begin_num
                  ,coalesce(t6.fcardroom_partynum, 0) fcardroom_partynum
             from work.bud_card_room_partner_detail_tmp_1_%(statdatenum)s t1
             left join work.bud_card_room_partner_detail_tmp_2_%(statdatenum)s t2
               on t1.fgamefsk = t2.fgamefsk
              and t1.fplatformfsk = t2.fplatformfsk
              and t1.fhallfsk = t2.fhallfsk
              and t1.fpartner_uid = t2.fpartner_uid
             left join work.bud_card_room_partner_detail_tmp_3_%(statdatenum)s t3
               on t1.fgamefsk = t3.fgamefsk
              and t1.fplatformfsk = t3.fplatformfsk
              and t1.fhallfsk = t3.fhallfsk
              and t1.fpartner_uid = t3.fpartner_uid
             left join work.bud_card_room_partner_detail_tmp_4_%(statdatenum)s t4
               on t1.fgamefsk = t4.fgamefsk
              and t1.fplatformfsk = t4.fplatformfsk
              and t1.fhallfsk = t4.fhallfsk
              and t1.fpartner_uid = t4.fpartner_uid
             left join work.bud_card_room_partner_detail_tmp_5_%(statdatenum)s t5
               on t1.fgamefsk = t5.fgamefsk
              and t1.fplatformfsk = t5.fplatformfsk
              and t1.fhallfsk = t5.fhallfsk
              and t1.fpartner_uid = t5.fpartner_uid
             left join work.bud_card_room_partner_detail_tmp_6_%(statdatenum)s t6
               on t1.fgamefsk = t6.fgamefsk
              and t1.fplatformfsk = t6.fplatformfsk
              and t1.fhallfsk = t6.fhallfsk
              and t1.fpartner_uid = t6.fpartner_uid
             left join work.bud_card_room_partner_detail_tmp_7_%(statdatenum)s t7
               on t1.fgamefsk = t7.fgamefsk
              and t1.fplatformfsk = t7.fplatformfsk
              and t1.fhallfsk = t7.fhallfsk
              and t1.fpartner_uid = t7.fpartner_uid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_partner_detail_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partner_detail_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partner_detail_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partner_detail_tmp_4_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partner_detail_tmp_5_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partner_detail_tmp_6_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partner_detail_tmp_7_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_card_room_partner_detail(sys.argv[1:])
a()
