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


class agg_bud_user_win_point_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_win_point_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fregion             int              comment '区间',
               funum               bigint           comment '人数',
               fnum                bigint           comment '总分',
               fparty_num          bigint           comment '牌局数',
               fwin_party_num      bigint           comment '胜局数'
               )comment '胜点相关信息'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_win_point_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fregion'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--胜点相关信息
            drop table if exists work.bud_user_win_point_info_tmp_%(statdatenum)s;
          create table work.bud_user_win_point_info_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fsubgamefsk,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,t1.fpoint
                 ,t1.frank
                 ,t1.fwin_point
                 ,t1.fparty_num
                 ,t1.fwin_party_num
                 ,case when nvl(fwin_point,0) = 0 then 1
                  else ceil(fwin_point/100) end fregion  --胜点向上取整
            from dim.user_point_rank_balance t1  --胜点
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fregion, %(null_int_group_rule)s) fregion   --区间
                 ,count(distinct fuid) funum            --人数
                 ,sum(fwin_point) fnum             --总分
                 ,sum(fparty_num) fparty_num       --牌局数
                 ,sum(fwin_party_num) fwin_party_num   --胜局数
            from work.bud_user_win_point_info_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fregion
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.bud_user_win_point_info
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_win_point_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_win_point_info(sys.argv[1:])
a()
