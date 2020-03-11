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


class agg_stage_table_info_day(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.stage_table_info_day (
               ftable              varchar(100)     comment '接口名',
               fpoint              varchar(100)     comment '检查点',
               fbpid_gameid        varchar(100)     comment 'bpid_子游戏id，大厅名称_uid',
               fnum                bigint           comment '次数'
               )comment '地方棋牌相关接口表日入库数据监控'
               partitioned by(dt string)
        location '/dw/bud_dm/stage_table_info_day';
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

        hql = """
         insert overwrite table bud_dm.stage_table_info_day
         partition( dt="%(statdate)s" )
         select 'user_act' ftable
                ,'当日活跃用户在全量注册用户中不存在-大厅' fpoint
                ,concat_ws('_', t1.fhallname, cast (t1.fuid as string)) fbpid_gameid
                ,count(1) fnum
           from (select distinct fhallfsk,fhallname,fuid
                   from dim.user_act t1
                   join dim.bpid_map_bud tt
                     on t1.fbpid = tt.fbpid
                    and tt.fgamename = '地方棋牌'
                  where t1.dt = "%(statdate)s") t1
           left join (select fhallfsk,fuid
                        from dim.reg_user_main_additional t1
                        join dim.bpid_map_bud tt
                          on t1.fbpid = tt.fbpid
                         and tt.fgamename = '地方棋牌') t2
             on t1.fhallfsk = t2.fhallfsk
            and t2.fuid = t1.fuid
          where t2.fuid is null
          group by concat_ws('_', t1.fhallname, cast (t1.fuid as string))
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
         insert into table bud_dm.stage_table_info_day
         partition( dt="%(statdate)s" )
         select 'user_gameparty_stg' ftable
                ,'牌局没有开始时间或结束时间' fpoint
                ,concat_ws('_', t1.fbpid, cast (t1.fgame_id as string)) fbpid_gameid
                ,count(1) fnum
           from stage_dfqp.user_gameparty_stg t1
           join dim.bpid_map_bud tt
             on t1.fbpid = tt.fbpid
            and tt.fgamename = '地方棋牌'
          where t1.dt = "%(statdate)s"
            and (t1.fs_timer = '1970-01-01 00:00:00'
                 or t1.fe_timer = '1970-01-01 00:00:00')
          group by concat_ws('_', t1.fbpid, cast (t1.fgame_id as string))
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
         insert into table bud_dm.stage_table_info_day
         partition( dt="%(statdate)s" )
         select 'user_gameparty_stg' ftable
                ,'牌局时长超过一天' fpoint
                ,concat_ws('_', t1.fbpid, cast (t1.fgame_id as string)) fbpid_gameid
                ,count(1) fnum
           from stage_dfqp.user_gameparty_stg t1
           join dim.bpid_map_bud tt
             on t1.fbpid = tt.fbpid
            and tt.fgamename = '地方棋牌'
          where t1.dt = "%(statdate)s"
            and t1.fs_timer <> '1970-01-01 00:00:00'
            and t1.fe_timer <> '1970-01-01 00:00:00'
            and unix_timestamp(fe_timer)-unix_timestamp(fs_timer) >=86400
          group by concat_ws('_', t1.fbpid, cast (t1.fgame_id as string))
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_stage_table_info_day(sys.argv[1:])
a()
