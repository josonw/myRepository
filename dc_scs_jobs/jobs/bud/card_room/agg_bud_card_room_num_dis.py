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


class agg_bud_card_room_num_dis(BaseStatModel):
    def create_tab(self):
        hql = """--暂时不用这个表
        create table if not exists bud_dm.bud_card_room_num_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type            varchar(10)      comment '用户类型:reg_user,act_user',
               num_0                 bigint           comment '0',
               num_1                 bigint           comment '[1,500]',
               num_501               bigint           comment '[501,1000]',
               num_1001              bigint           comment '[1001,1500]',
               num_1501              bigint           comment '[1501,2000]',
               num_2001              bigint           comment '[2001,2500]',
               num_2501              bigint           comment '[2501,3000]',
               num_3001              bigint           comment '>=3001'
               )comment '棋牌室数量分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_num_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['froom_id'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_card_room_num_dis_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_num_dis_tmp_1_%(statdatenum)s as
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
            from dim.card_room_party t2
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
                 ,count(distinct concat_ws('0', finning_id, ftbl_id)) num
            from work.bud_card_room_num_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,froom_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            drop table if exists work.bud_card_room_num_dis_tmp_%(statdatenum)s;
          create table work.bud_card_room_num_dis_tmp_%(statdatenum)s as
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_card_room_num_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'all_user' fuser_type
                 ,count(distinct case when num = 0 then froom_id end) num_0
                 ,count(distinct case when num between 1 and 500 then froom_id end) num_1
                 ,count(distinct case when num between 501 and 1000 then froom_id end) num_501
                 ,count(distinct case when num between 1001 and 1500 then froom_id end) num_1001
                 ,count(distinct case when num between 1501 and 2000 then froom_id end) num_1501
                 ,count(distinct case when num between 2001 and 2500 then froom_id end) num_2001
                 ,count(distinct case when num between 2501 and 3000 then froom_id end) num_2501
                 ,count(distinct case when num >=3001 then froom_id end) num_3001
            from work.bud_card_room_num_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_num_dis_tmp_%(statdatenum)s;
                 drop table if exists work.bud_card_room_num_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_num_dis(sys.argv[1:])
a()
