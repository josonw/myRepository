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


class agg_bud_card_room_id_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_id_info (
                fdate                          date,
                fgamefsk                       bigint,
                fplatformfsk                   bigint,
                fhallfsk                       bigint,
                fsubgamefsk                    bigint,
                fterminaltypefsk               bigint,
                fversionfsk                    bigint,
                froom_id             bigint        comment '房间id',
                fcard_room_id        bigint        comment '棋牌室id',
                fcard_room_name      varchar(50)   comment '棋牌室名称',
                fpartner_uid         bigint        comment '所属代理商',
                fcreate_uid          bigint        comment '房间创建人',
                fall_uid             varchar(500)  comment '玩牌玩家',
                fend_time            varchar(50)   comment '牌局结束时间',
                fgame_id             bigint        comment '子游戏',
                fmode                int           comment '模式：固定3种（0.冠军、1.分摊、2.房主）',
                fgame_num            bigint        comment '房间局数',
                fplay_type           varchar(50)   comment '房间玩法'
                )comment '棋牌室房间列表'
                partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_id_info';
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
            drop table if exists work.bud_card_room_id_info_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_id_info_tmp_1_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpromoter
                  ,count(distinct fpartner_uid) fall_partner_unum   --累计新增代理商
                  ,count(distinct case when dt = '%(statdate)s' then fpartner_uid end) fnew_partner_num   --新增代理商
             from dim.card_room_partner_new t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室数量
            drop table if exists work.bud_card_room_id_info_tmp_2_%(statdatenum)s;
          create table work.bud_card_room_id_info_tmp_2_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpromoter
                  ,count(distinct case when fact_id = 1 then froom_id end) fall_cardroom_num   --所有棋牌室数量
                  ,count(distinct case when fact_id = 2 then froom_id end) fall_dis_cardroom_num   --解散棋牌室数量
                  ,count(distinct case when fact_id = 3 then froom_id end) fall_fbd_cardroom_num   --禁用棋牌室数量
             from dim.card_room_partner_act t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室用户
            drop table if exists work.bud_card_room_id_info_tmp_3_%(statdatenum)s;
          create table work.bud_card_room_id_info_tmp_3_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,froom_promoter_id promoter
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
            drop table if exists work.bud_card_room_id_info_tmp_4_%(statdatenum)s;
          create table work.bud_card_room_id_info_tmp_4_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,t.fpromoter
                  ,count(distinct case when t.dt = '%(statdate)s' and fparty_num > 0 then t1.fpartner_uid end) fact_partner_num           --有效活跃代理商
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_cardroom_game_num   --棋牌室累计开局房间数
                  ,sum(fparty_num) fall_cardroom_party_num  --棋牌室累计牌局数
                  ,count(distinct fuid) fact_cardroom_unum  --棋牌室活跃成员
                  ,count(distinct case when t.dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fcardroom_begin_num  --棋牌室成功开房次数
                  ,sum(case when t.dt = '%(statdate)s' then fparty_num end) fcardroom_partynum  --棋牌室玩牌局数
             from dim.card_room_play t
             left join dim.card_room_partner_new t1
               on t.fpartner_uid = t1.fpartner_uid
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,t.fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--充值（购买房卡）
            drop table if exists work.bud_card_room_id_info_tmp_5_%(statdatenum)s;
          create table work.bud_card_room_id_info_tmp_5_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,fpromoter
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,sum(fcard_income) fall_card_income --累计充值总额（购买房卡）
                  ,sum(case when t.dt = '%(statdate)s' then fcard_income end) fcard_income     --充值总额（购买房卡）
             from dim.card_room_partner_pay t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--充值（购买房卡）
            drop table if exists work.bud_card_room_id_info_tmp_6_%(statdatenum)s;
          create table work.bud_card_room_id_info_tmp_6_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,t.fpromoter
                  ,count(distinct fuid) fact_cardroom_unum  --棋牌室活跃成员
             from dim.enter_card_room t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,t.fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
          insert overwrite table bud_dm.bud_card_room_id_info partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t.fgamefsk
                  ,t.fplatformfsk
                  ,t.fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fpromoter
                  ,coalesce(t.fall_partner_unum, 0 ) fall_partner_unum
                  ,coalesce(t1.fall_cardroom_num, 0 ) fall_cardroom_num
                  ,coalesce(t1.fall_dis_cardroom_num, 0 ) fall_dis_cardroom_num
                  ,coalesce(t1.fall_fbd_cardroom_num, 0 ) fall_fbd_cardroom_num
                  ,coalesce(t2.fall_cardroom_unum, 0 ) fall_cardroom_unum
                  ,coalesce(t3.fall_cardroom_game_num, 0 ) fall_cardroom_game_num
                  ,coalesce(t3.fall_cardroom_party_num, 0 ) fall_cardroom_party_num
                  ,coalesce(t4.fall_card_income, 0 ) fall_card_income
                  ,coalesce(t.fnew_partner_num, 0 ) fnew_partner_num
                  ,coalesce(t3.fact_partner_num, 0 ) fact_partner_num
                  ,coalesce(t2.fnew_cardroom_unum, 0 ) fnew_cardroom_unum
                  ,coalesce(t3.fact_cardroom_unum, 0 ) fact_cardroom_unum
                  ,coalesce(t3.fcardroom_begin_num, 0 ) fcardroom_begin_num
                  ,coalesce(t3.fcardroom_partynum, 0 ) fcardroom_partynum
                  ,coalesce(t4.fcard_income, 0 ) fcard_income
             from work.bud_card_room_id_info_tmp_1_%(statdatenum)s t
             left join work.bud_card_room_id_info_tmp_2_%(statdatenum)s t1
               on t.fgamefsk = t1.fgamefsk
              and t.fplatformfsk = t1.fplatformfsk
              and t.fhallfsk = t1.fhallfsk
              and t.fpromoter = t1.fpromoter
             left join work.bud_card_room_id_info_tmp_3_%(statdatenum)s t2
               on t.fgamefsk = t2.fgamefsk
              and t.fplatformfsk = t2.fplatformfsk
              and t.fhallfsk = t2.fhallfsk
              and t.fpromoter = t2.fpromoter
             left join work.bud_card_room_id_info_tmp_4_%(statdatenum)s t3
               on t.fgamefsk = t3.fgamefsk
              and t.fplatformfsk = t3.fplatformfsk
              and t.fhallfsk = t3.fhallfsk
              and t.fpromoter = t3.fpromoter
             left join work.bud_card_room_id_info_tmp_5_%(statdatenum)s t4
               on t.fgamefsk = t4.fgamefsk
              and t.fplatformfsk = t4.fplatformfsk
              and t.fhallfsk = t4.fhallfsk
              and t.fpromoter = t4.fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_card_room_id_info(sys.argv[1:])
a()
