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


class agg_bud_card_room_play_actret_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_play_actret_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10) comment '约牌房：pa_room，棋牌室：card_room，代理商：fpartner',
               flast_date          date        comment '玩牌日期',
               fplay_unum          bigint      comment '当日玩牌用户',
               fret_unum           bigint      comment '留存用户'
               )comment '棋牌室玩牌用户玩牌留存'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_play_actret_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['flast_date', 'fuser_type'],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--新增
            drop table if exists work.bud_card_room_play_actret_info_tmp_%(statdatenum)s;
          create table work.bud_card_room_play_actret_info_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid play_uid
                 ,t2.fuid ret_uid
                 ,t1.dt flast_date
                 ,case when t1.fsubname = '棋牌馆' then 'card_room'
                       when t1.fsubname = '约牌房' then 'pa_room'
                  else 'uknown' end fuser_type
            from dim.card_room_party t1
            left join dim.enter_card_room t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t1.fsubname = t2.fsubname
             and t2.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
              or t1.dt='%(ld_14day_ago)s'
              or t1.dt='%(ld_30day_ago)s'
              or t1.dt='%(ld_60day_ago)s'
              or t1.dt='%(ld_90day_ago)s');
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,flast_date
                 ,count(distinct play_uid) fplay_unum
                 ,count(distinct ret_uid) fret_unum
            from work.bud_card_room_play_actret_info_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flast_date
                    ,fuser_type
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_card_room_play_actret_info
                      partition(dt='%(statdate)s')
                      %(sql_template)s  """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_play_actret_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_play_actret_info(sys.argv[1:])
a()
