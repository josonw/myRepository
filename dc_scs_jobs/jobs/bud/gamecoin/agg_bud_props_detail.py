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


class agg_bud_props_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_props_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fscene              varchar(100)   comment '场景',
               fpname              varchar(256)   comment '牌局场次一级分类',
               fsubname            varchar(256)   comment '牌局场次二级分类',
               fdirection          varchar(50)    comment '操作类型:IN\OUT',
               act_id              varchar(32)    comment '操作编号(变化原因)',
               ftype               varchar(50)    comment '道具编号',
               ctype               int            comment '币种id',
               fprops_unum         bigint         comment '道具变化用户数',
               fprops_num          bigint         comment '道具变化数量',
               fprops_cnt          bigint         comment '道具变化次数'
               )comment '道具分析'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_props_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fpname', 'fsubname', 'fdirection', 'act_id', 'ftype', 'ctype'],
                        'groups': [[1, 1, 1, 1, 1, 1, 1],
                                   [0, 0, 0, 1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取基础指标
            drop table if exists work.bud_props_detail_tmp_1_%(statdatenum)s;
          create table work.bud_props_detail_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fscene
                 ,coalesce(t1.fpname, '%(null_str_report)s') fpname
                 ,coalesce(t1.fsubname, '%(null_str_report)s') fsubname
                 ,t1.act_type fdirection
                 ,t1.act_id
                 ,t1.prop_id ftype
                 ,t1.c_type ctype
                 ,abs(t1.act_num) fnum
                 ,t1.fuid
            from stage.pb_props_stream_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
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
                 ,'0' fscene  --场景
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname  --场景
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname  --场景
                 ,fdirection                                       --操作类型:IN\OUT
                 ,act_id                                           --操作编号(变化原因)
                 ,ftype                                            --道具编号
                 ,ctype                                            --币种id
                 ,count(distinct fuid) fprops_unum                 --道具变化用户数
                 ,sum(fnum) fprops_num                             --道具变化数量
                 ,count(fuid) fprops_cnt                           --道具变化次数
            from work.bud_props_detail_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fpname, fsubname ,fdirection ,act_id ,ftype ,ctype
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_props_detail partition(dt='%(statdate)s')
             %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_props_detail_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_props_detail(sys.argv[1:])
a()
