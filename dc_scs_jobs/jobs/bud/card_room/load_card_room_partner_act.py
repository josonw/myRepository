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

class load_card_room_partner_act(BaseStatModel):
    def create_tab(self):
        """ 棋牌室代理商活跃中间表 """
        hql = """create table if not exists dim.card_room_partner_act
            (fbpid           varchar(50)    comment  'BPID',
             fgame_id        bigint         comment  '子游戏id',
             fpartner_uid    bigint         comment  '代理商UID',
             fpartner_name   varchar(50)    comment  '代理商名称/昵称',
             fpromoter       varchar(50)    comment  '推广员',
             froom_id        string         comment  '棋牌室ID',
             fact_id         int            comment  '棋牌室操作（1.创建，2.解散，3.禁用）',
             fis_first       int            comment  '是否首次（0.否，1.是）'
            )
            partitioned by (dt date)
        stored as orc;
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
        insert overwrite table dim.card_room_partner_act partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fgame_id
                       ,t1.fuid fpartner_uid
                       ,t2.fpartner_name
                       ,coalesce(t1.fpromoter_id, %(null_int_report)d) fpromoter_id
                       ,t1.froom_id
                       ,t1.fact_id
                       ,t1.fis_first
                  from stage.create_dissolve_room_stg t1
                  left join dim.card_room_partner_new t2
                    on t1.fbpid = t2.fbpid
                   and t1.fuid = t2.fpartner_uid
                   and t2.dt <= '%(statdate)s'
                 where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_card_room_partner_act(sys.argv[1:])
a()
