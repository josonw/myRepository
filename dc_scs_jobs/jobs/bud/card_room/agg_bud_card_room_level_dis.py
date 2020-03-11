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


class agg_bud_card_room_level_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_level_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)      comment '约牌房：pa_room，棋牌室：card_room，代理商：fpartner',
               flevel              bigint           comment '等级',
               num                 bigint           comment '数量'
               )comment '棋牌室等级分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_level_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fgame_level', 'fuser_type'],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名用户
            drop table if exists work.bud_card_room_level_dis_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_level_dis_tmp_1_%(statdatenum)s as
          select tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.fcard_room_id
                 ,t2.fgame_level
                 ,'card_room' fuser_type
            from dim.card_room_level t2
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,cast (t2.fpartner_uid as string) fcard_room_id
                 ,t2.fgame_level
                 ,'fpartner' fuser_type
            from dim.card_room_partner_level t2
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s';
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
                 ,fgame_level
                 ,count(distinct fcard_room_id) num
            from work.bud_card_room_level_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fgame_level
                    ,fuser_type
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.bud_card_room_level_dis
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_level_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_level_dis(sys.argv[1:])
a()
