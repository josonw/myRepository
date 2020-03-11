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

class load_card_room_stat(BaseStatModel):
    def create_tab(self):
        """ 棋牌室状态表 """
        hql = """create table if not exists dim.card_room_stat
            (fbpid           varchar(50)    comment  'BPID',
             fgame_id        bigint         comment  '子游戏id',
             fpartner_uid    bigint         comment  '代理商UID',
             fpartner_name   varchar(50)    comment  '代理商名称/昵称',
             fpromoter       varchar(50)    comment  '推广员',
             froom_id        string         comment  '棋牌室ID',
             froom_name      string         comment  '棋牌室ID',
             fact_id         int            comment  '棋牌室状态（1.创建，2.解散，3.禁用）',
             fcreate_time    varchar(50)    comment  '创建时间',
             fdisband_time   varchar(50)    comment  '解散时间',
             fforbidden_time varchar(50)    comment  '最近一次禁用时间'
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

        hql = """--先取当日创建，然后更新解散和禁用时间，最后确定状态
        insert overwrite table dim.card_room_stat partition (dt = "%(statdate)s" )
        select fbpid
               ,fgame_id
               ,fpartner_uid
               ,fpartner_name
               ,coalesce(fpromoter, %(null_int_report)d) fpromoter
               ,froom_id
               ,froom_name
               ,case when fforbidden_time is null and fdisband_time is not null then 2
                     when fdisband_time is null and fforbidden_time is not null then 3
                     when fdisband_time is not null and fforbidden_time is not null and fforbidden_time >= fdisband_time then 3
                     when fdisband_time is not null and fforbidden_time is not null and fforbidden_time < fdisband_time then 2
                else 0 end fact_id
               ,fcreate_time
               ,fdisband_time
               ,fforbidden_time
          from (select  t1.fbpid
                       ,t1.fgame_id
                       ,t1.fpartner_uid
                       ,t1.fpartner_name
                       ,t1.fpromoter
                       ,t1.froom_id
                       ,t1.froom_name
                       ,t1.fact_id
                       ,t1.fcreate_time
                       ,coalesce(t2.fdisband_time,t1.fdisband_time) fdisband_time
                       ,coalesce(t2.fforbidden_time,t1.fforbidden_time) fforbidden_time
                  from (select t.fbpid
                               ,t.fgame_id
                               ,t.fuid fpartner_uid
                               ,t2.fpartner_name
                               ,t.fpromoter_id fpromoter
                               ,t.froom_id
                               ,t.froom_name
                               ,t.fact_id
                               ,t.fcreate_time
                               ,null fdisband_time
                               ,null fforbidden_time
                          from stage.create_dissolve_room_stg t
                          left join dim.card_room_partner_new t2
                            on t.fbpid = t2.fbpid
                           and t.fuid = t2.fpartner_uid
                           and t2.dt <= '%(statdate)s'
                         where t.dt = '%(statdate)s'
                           and fact_id = 1
                         union all
                        select  t.fbpid,t.fgame_id,t.fpartner_uid,t.fpartner_name,t.fpromoter,t.froom_id,t.froom_name,t.fact_id,t.fcreate_time,t.fdisband_time,t.fforbidden_time
                          from dim.card_room_stat t
                         where dt = date_sub('%(statdate)s' ,1)
                       ) t1
                  left join (select froom_id
                                    ,max(case when fact_id = 2 then flts_at else null end) fdisband_time
                                    ,max(case when fact_id = 3 then flts_at else null end) fforbidden_time
                               from stage.create_dissolve_room_stg t
                              where dt = '%(statdate)s'
                                and fact_id in (2, 3)
                              group by froom_id
                            ) t2
                    on t1.froom_id = t2.froom_id ) t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_card_room_stat(sys.argv[1:])
a()
