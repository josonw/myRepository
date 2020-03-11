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


class agg_bud_card_room_user_from_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_user_from_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)      comment '用户类型:reg_user,act_user',
               fuser_from          bigint           comment '来源',
               num                 bigint           comment '数量'
               )comment '棋牌室用户来源分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_user_from_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_from'],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名用户
            drop table if exists work.bud_card_room_user_from_dis_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_user_from_dis_tmp_1_%(statdatenum)s as
          select tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.fuid
                 ,t2.fuser_from
                 ,'reg_user' fuser_type
            from dim.join_exit_room t2  --当日加入棋牌室
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s'
             and fjoin_dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.fuid
                 ,t2.fuser_from
                 ,'act_user' fuser_type
            from dim.enter_card_room t2  --进入棋牌室
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s'
             and t2.fsubname = '棋牌馆'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --报名用户
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,fuser_from
                 ,count(distinct fuid) num
            from work.bud_card_room_user_from_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fuser_type
                    ,fuser_from
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.bud_card_room_user_from_dis
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_user_from_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_user_from_dis(sys.argv[1:])
a()
