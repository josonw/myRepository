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


class agg_bud_card_room_partynum_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_partynum_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type            varchar(10)      comment '约牌房：pa_room，棋牌室：card_room，代理商：fpartner',
               fgame_num             bigint           comment '局数',
               num                   bigint           comment '数量'
               )comment '棋牌室局数分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_partynum_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['froom_id', 'fuser_type'],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名用户
            drop table if exists work.bud_card_room_partynum_dis_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_partynum_dis_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.froom_id
                 ,t2.ftbl_id
                 ,t2.finning_id
                 ,case when t2.fsubname = '棋牌馆' then 'card_room'
                       when t2.fsubname = '约牌房' then 'pa_room'
                  else 'uknown' end fuser_type
            from dim.card_room_party t2  --报名
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --报名用户
        hql = """
          select fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,froom_id
                 ,fuser_type
                 ,count(distinct concat_ws('0', finning_id, ftbl_id)) num
            from work.bud_card_room_partynum_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,froom_id
                    ,fuser_type
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            drop table if exists work.bud_card_room_partynum_dis_tmp_%(statdatenum)s;
          create table work.bud_card_room_partynum_dis_tmp_%(statdatenum)s as
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_card_room_partynum_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,num fgame_num
                 ,count(distinct froom_id) num
            from work.bud_card_room_partynum_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,num
                    ,fuser_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_partynum_dis_tmp_%(statdatenum)s;
                 drop table if exists work.bud_card_room_partynum_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_partynum_dis(sys.argv[1:])
a()
