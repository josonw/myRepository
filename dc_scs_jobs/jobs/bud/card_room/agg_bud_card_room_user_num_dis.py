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


class agg_bud_card_room_user_num_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_user_num_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type            varchar(10)      comment '用户类型:reg_user,act_user',
               num_1                 bigint           comment '1',
               num_2                 bigint           comment '[2,50]',
               num_50                bigint           comment '[50,100]',
               num_101               bigint           comment '[101,200]',
               num_201               bigint           comment '[201,300]',
               num_301               bigint           comment '[301,400]',
               num_401               bigint           comment '[401,500]',
               num_501               bigint           comment '[501,600]',
               num_601               bigint           comment '>=601'
               )comment '棋牌室用户数分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_user_num_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'froom_id'],
                        'groups': [[1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--进入用户
            drop table if exists work.bud_card_room_user_num_dis_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_user_num_dis_tmp_1_%(statdatenum)s as
          select tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t2.fuid
                 ,t2.froom_id
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
                 ,t2.froom_id
                 ,'act_user' fuser_type
            from dim.join_exit_room t2  --进入棋牌室
            join dim.bpid_map_bud tt
              on t2.fbpid = tt.fbpid
           where t2.dt = '%(statdate)s'
             and t2.fstatus <> 2;
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
                 ,fuser_type
                 ,froom_id
                 ,count(distinct fuid) num
            from work.bud_card_room_user_num_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fuser_type
                    ,froom_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            drop table if exists work.bud_card_room_user_num_dis_tmp_%(statdatenum)s;
          create table work.bud_card_room_user_num_dis_tmp_%(statdatenum)s as
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_card_room_user_num_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fuser_type
                 ,count(distinct case when num = 1 then froom_id end) num_1
                 ,count(distinct case when num between 2 and 50 then froom_id end) num_2
                 ,count(distinct case when num between 50 and 100 then froom_id end) num_50
                 ,count(distinct case when num between 101 and 200 then froom_id end) num_101
                 ,count(distinct case when num between 201 and 300 then froom_id end) num_201
                 ,count(distinct case when num between 301 and 400 then froom_id end) num_301
                 ,count(distinct case when num between 401 and 500 then froom_id end) num_401
                 ,count(distinct case when num between 501 and 600 then froom_id end) num_501
                 ,count(distinct case when num >=601 then froom_id end) num_601
            from work.bud_card_room_user_num_dis_tmp_%(statdatenum)s t
           where num > 0
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_user_num_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_card_room_user_num_dis_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_user_num_dis(sys.argv[1:])
a()
