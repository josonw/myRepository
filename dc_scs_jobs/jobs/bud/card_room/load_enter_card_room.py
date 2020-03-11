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

class load_enter_card_room(BaseStatModel):
    def create_tab(self):
        """ 进入棋牌室房间"""
        hql = """create table if not exists dim.enter_card_room
            (fbpid                varchar(50)   comment 'BPID',
             fuid                 bigint        comment '用户id',
             fis_first            bigint        comment '是否首次进入',
             fgame_id             bigint        comment '子游戏id',
             fpname               varchar(50)   comment '一级场次',
             fsubname             varchar(50)   comment '二级场次',
             fcard_room_id        varchar(50)   comment '棋牌室id',
             fuser_from           varchar(50)   comment '用户来源',
             fpartner_uid         bigint        comment '代理商UID',
             fpromoter            bigint        comment '推广员',
             fenter_cnt           bigint        comment '进入次数',
             fopen_cnt            bigint        comment '开房次数'
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
        insert overwrite table dim.enter_card_room partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fuid
                       ,case when t1.fsubname = '棋牌馆' and t3.fuid is null then 1
                             when t1.fsubname = '约牌房' and t4.fuid is null then 1
                        else 0 end fis_first
                       ,t1.fgame_id
                       ,t1.fpname
                       ,t1.fsubname
                       ,t1.fgsubname fcard_room_id
                       ,t1.fuser_from
                       ,t2.fpartner_uid
                       ,coalesce(t2.fpromoter, '%(null_str_report)s') fpromoter
                       ,sum(fenter_cnt) fenter_cnt
                       ,sum(fopen_cnt) fopen_cnt
                  from (select t1.fbpid
                               ,t1.fuid
                               ,t1.fgame_id
                               ,t1.fpname
                               ,t1.fsubname
                               ,t1.fgsubname
                               ,t1.fuser_from
                               ,count(t1.fuid) fenter_cnt
                               ,0 fopen_cnt
                          from stage.enter_room_stg t1
                         where t1.dt = '%(statdate)s'
                         group by t1.fbpid, t1.fuid, t1.fgame_id, t1.fpname, t1.fsubname, t1.fgsubname, t1.fuser_from
                         union all
                        select t1.fbpid
                               ,t1.fuid
                               ,t1.fgame_id
                               ,t1.fpname
                               ,t1.fsubname
                               ,t1.fgsubname
                               ,t1.fuser_from
                               ,0 fenter_cnt
                               ,count(t1.fuid) fopen_cnt
                          from stage.open_room_record_stg t1
                         where t1.dt = '%(statdate)s'
                         group by t1.fbpid, t1.fuid, t1.fgame_id, t1.fpname, t1.fsubname, t1.fgsubname, t1.fuser_from
                       ) t1
                  left join dim.card_room_stat t2
                    on t1.fgsubname = t2.froom_id
                   and t2.dt = '%(statdate)s'
                  left join (select distinct fcard_room_id,fuid from dim.enter_card_room t where dt = date_sub('%(statdate)s',1)) t3
                    on t1.fgsubname = t3.fcard_room_id
                   and t1.fuid = t3.fuid
                  left join (select distinct fsubname,fuid from dim.enter_card_room t where dt = date_sub('%(statdate)s',1)) t4
                    on t1.fsubname = t4.fsubname
                   and t1.fuid = t4.fuid
                 group by t1.fbpid
                       ,t1.fuid
                       ,case when t1.fsubname = '棋牌馆' and t3.fuid is null then 1
                             when t1.fsubname = '约牌房' and t4.fuid is null then 1
                        else 0 end
                       ,t1.fgame_id
                       ,t1.fpname
                       ,t1.fsubname
                       ,t1.fgsubname
                       ,t1.fuser_from
                       ,t2.fpartner_uid
                       ,t2.fpromoter
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_enter_card_room(sys.argv[1:])
a()
