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

class load_card_room_play(BaseStatModel):
    def create_tab(self):
        """ 棋牌室代理商玩牌中间表 and 牌局中间表"""
        hql = """create table if not exists dim.card_room_play
            (fbpid                varchar(50)   comment 'BPID',
             froom_id             bigint        comment '房间id',
             fcard_room_name      varchar(50)   comment '棋牌室名称',
             fcreate_uid          bigint        comment '房间创建人',
             fpartner_uid         bigint        comment '代理商UID',
             fpromoter            varchar(50)   comment '推广员',
             fgame_id             bigint        comment '子游戏',
             fpname               varchar(50)   comment '一级场次',
             fsubname             varchar(50)   comment '二级场次',
             fcard_room_id        varchar(50)   comment '三级场次（棋牌室id）',
             fmode                int           comment '模式：固定3种（0.冠军、1.分摊、2.房主）',
             fgame_num            bigint        comment '房间局数',
             fplay_type           varchar(500)  comment '房间玩法',
             fparty_num           bigint        comment '实际局数',
             fplay_unum           bigint        comment '实际人数'
            )
            partitioned by (dt string);

        create table if not exists dim.card_room_party
            (fbpid                varchar(50)   comment 'BPID',
             fuid                 int           comment '用户id',
             fgame_id             bigint        comment '子游戏',
             fpname               varchar(50)   comment '一级场次',
             fsubname             varchar(50)   comment '二级场次',
             fcard_room_id        varchar(50)   comment '三级场次（棋牌室id）',
             froom_id             bigint        comment '房间id',
             ftbl_id              varchar(64)   comment '桌子编号',
             finning_id           varchar(64)   comment '牌局编号',
             fs_timer             string        comment '开始时间戳',
             fe_timer             string        comment '开始时间戳',
             fcoins_type          int           comment '货币类型',
             fpartner_uid         bigint        comment '代理商UID'
            )
            partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.card_room_party partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fuid
                       ,t1.fgame_id
                       ,t1.fpname
                       ,case when t1.fsubname = '2' then '棋牌馆' else '约牌房' end fsubname
                       ,t1.fgsubname fcard_room_id
                       ,t1.froom_id
                       ,t1.ftbl_id
                       ,t1.finning_id
                       ,t1.fs_timer
                       ,t1.fe_timer
                       ,t1.fcoins_type
                       ,t1.fpartner_info fpartner_uid
                  from stage.user_gameparty_stg t1
                  join dim.bpid_map tt
                    on t1.fbpid = tt.fbpid
                   and tt.fgamename = '地方棋牌'
                 where t1.dt = '%(statdate)s'
                   and t1.fsubname in ('2', '1') --1约牌房2棋牌室
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.card_room_play partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.froom_id
                       ,t1.fcard_room_name
                       ,t1.fuid fcreate_uid
                       ,t1.fpartner_info fpartner_uid
                       ,coalesce(t2.fpromoter, '%(null_str_report)s') fpromoter
                       ,t1.fgame_id
                       ,t1.fpname
                       ,fsubname
                       ,t1.fgsubname
                       ,t1.fmode
                       ,t1.fgame_num
                       ,t1.fplay_type
                       ,coalesce(t3.fparty_num, 0) fparty_num
                       ,coalesce(t3.fplay_unum, 0) fplay_unum
                  from stage.open_room_record_stg t1
                  left join dim.card_room_partner_new t2
                    on t1.fbpid = t2.fbpid
                   and t1.fpartner_info = t2.fpartner_uid
                   and t2.dt <= '%(statdate)s'
                  left join (select froom_id,
                                   count(distinct concat_ws('0', ftbl_id, finning_id)) fparty_num,
                                   count(distinct fuid) fplay_unum
                              from dim.card_room_party t2
                             where dt = '%(statdate)s'
                             group by froom_id
                            ) t3
                    on t1.froom_id = t3.froom_id
                 where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_card_room_play(sys.argv[1:])
a()
