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


class agg_bud_card_room_uid_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_uid_detail (
                fdate                          date,
                fgamefsk                       bigint,
                fplatformfsk                   bigint,
                fhallfsk                       bigint,
                fsubgamefsk                    bigint,
                fterminaltypefsk               bigint,
                fversionfsk                    bigint,
                fuid                           bigint        comment '成员mid',
                fcard_room_id                  bigint        comment '棋牌室id',
                fcard_room_name                varchar(50)   comment '棋牌室名称',
                fpartner_uid                   bigint        comment '群主id',
                ffjob                          varchar(50)   comment '职务',
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
                )comment '棋牌室成员明细'
                partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_uid_detail';
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
        hql = """--棋牌室成员id
            drop table if exists work.bud_card_room_uid_detail_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_uid_detail_tmp_1_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fuid
                  ,froom_id fcard_room_id
                  ,froom_name fcard_room_name
                  ,froom_partner_id fpartner_uid
                  ,froom_user_job ffjob
             from dim.join_exit_room t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt = '%(statdate)s' and t.fstatus = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室数量
            drop table if exists work.bud_card_room_uid_detail_tmp_2_%(statdatenum)s;
          create table work.bud_card_room_uid_detail_tmp_2_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fcard_room_id
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_cardroom_game_num   --棋牌室累计开局房间数
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_jt_cardroom_game_num   --棋牌室累计开局房间数（均摊）
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_gj_cardroom_game_num   --棋牌室累计开局房间数（冠军）
                  ,count(distinct case when fparty_num > 0 then froom_id end) fall_fz_cardroom_game_num   --棋牌室累计开局房间数（房主）
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fcardroom_game_num  --棋牌室开局房间数
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fjt_cardroom_game_num  --棋牌室开局房间数（均摊）
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) fgj_cardroom_game_num  --棋牌室开局房间数（冠军）
                  ,count(distinct case when dt = '%(statdate)s' and fparty_num > 0 then froom_id end) ffz_cardroom_game_num  --棋牌室开局房间数（房主）
             from dim.card_room_play t
             join dim.bpid_map_bud tt
               on t.fbpid = tt.fbpid
            where t.dt <= '%(statdate)s'
              and t.fsubname = '棋牌馆'
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fcard_room_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室用户
            drop table if exists work.bud_card_room_uid_detail_tmp_3_%(statdatenum)s;
          create table work.bud_card_room_uid_detail_tmp_3_%(statdatenum)s as
           select fgamefsk
                  ,fplatformfsk
                  ,fhallfsk
                  ,-21379 fsubgamefsk
                  ,-21379 fterminaltypefsk
                  ,-21379 fversionfsk
                  ,fcard_room_id
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
            group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fcard_room_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--棋牌室开房数量
          insert overwrite table bud_dm.bud_card_room_uid_detail partition(dt='%(statdate)s')
           select '%(statdate)s' fdate
                  ,t1.fgamefsk
                  ,t1.fplatformfsk
                  ,t1.fhallfsk
                  ,t1.fsubgamefsk
                  ,t1.fterminaltypefsk
                  ,t1.fversionfsk
                  ,t1.fuid
                  ,t1.fcard_room_id
                  ,t1.fcard_room_name
                  ,t1.fpartner_uid
                  ,t1.ffjob
                  ,coalesce(t2.fall_cardroom_game_num, 0) fall_cardroom_game_num
                  ,coalesce(t2.fall_jt_cardroom_game_num, 0) fall_jt_cardroom_game_num
                  ,coalesce(t2.fall_gj_cardroom_game_num, 0) fall_gj_cardroom_game_num
                  ,coalesce(t2.fall_fz_cardroom_game_num, 0) fall_fz_cardroom_game_num
                  ,coalesce(t3.fall_cardroom_party_num, 0) fall_cardroom_party_num
                  ,coalesce(t3.fall_jt_cardroom_party_num, 0) fall_jt_cardroom_party_num
                  ,coalesce(t3.fall_gj_cardroom_party_num, 0) fall_gj_cardroom_party_num
                  ,coalesce(t3.fall_fz_cardroom_party_num, 0) fall_fz_cardroom_party_num
                  ,coalesce(t2.fcardroom_game_num, 0) fcardroom_game_num
                  ,coalesce(t2.fjt_cardroom_game_num, 0) fjt_cardroom_game_num
                  ,coalesce(t2.fgj_cardroom_game_num, 0) fgj_cardroom_game_num
                  ,coalesce(t2.ffz_cardroom_game_num, 0) ffz_cardroom_game_num
                  ,coalesce(t3.fcardroom_party_num, 0) fcardroom_party_num
                  ,coalesce(t3.fjt_cardroom_party_num, 0) fjt_cardroom_party_num
                  ,coalesce(t3.fgj_cardroom_party_num, 0) fgj_cardroom_party_num
                  ,coalesce(t3.ffz_cardroom_party_num, 0) ffz_cardroom_party_num
             from work.bud_card_room_uid_detail_tmp_1_%(statdatenum)s t1
             left join work.bud_card_room_uid_detail_tmp_2_%(statdatenum)s t2
               on t1.fgamefsk = t2.fgamefsk
              and t1.fplatformfsk = t2.fplatformfsk
              and t1.fhallfsk = t2.fhallfsk
              and t1.fcard_room_id = t2.fcard_room_id
             left join work.bud_card_room_uid_detail_tmp_3_%(statdatenum)s t3
               on t1.fgamefsk = t3.fgamefsk
              and t1.fplatformfsk = t3.fplatformfsk
              and t1.fhallfsk = t3.fhallfsk
              and t1.fcard_room_id = t3.fcard_room_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_uid_detail_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_card_room_uid_detail_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_card_room_uid_detail_tmp_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_card_room_uid_detail(sys.argv[1:])
a()
