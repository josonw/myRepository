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


class agg_match_recommend_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.match_recommend_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fparty_type         varchar(10)      comment '牌局类型',
               fpname              varchar(100)     comment '一级场次',
               fsubname            varchar(100)     comment '二级场次',
               fgsubname           varchar(100)     comment '三级场次',
               frecom_range        varchar(100)     comment '推荐区间',
               frecom_match_name   varchar(100)     comment '推荐比赛名称',
               frecom_sence        int              comment '推荐场景(1比赛大厅，2比赛内)',
               frecom_pro          int              comment '推荐属性(1成功，2失败无效)',
               frecom_click        int              comment '推荐点击(1返回大厅，2点击关闭，3前往报名)',
               frecom_res          int              comment '推荐结果(1报名，2点击关闭)',
               funum               bigint           comment '人数',
               fnum                bigint           comment '人次'
               )comment '推荐相关信息'
               partitioned by(dt date)
        location '/dw/bud_dm/match_recommend_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fparty_type', 'fpname', 'fsubname', 'fgsubname', 'frecom_range', 'frecom_sence', 'frecom_pro', 'frecom_click', 'frecom_res', 'frecom_match_name'],
                            'groups':[[1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                                      [0, 0, 0, 0, 1, 1, 1, 1, 1, 1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--推荐相关信息
            drop table if exists work.match_recommend_info_tmp_%(statdatenum)s;
          create table work.match_recommend_info_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,coalesce(t1.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t1.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t1.fgsubname,'%(null_str_report)s')  fgsubname
                 ,coalesce(t1.frecom_range, '%(null_str_report)s') frecom_range
                 ,coalesce(t1.frecom_sence, %(null_int_report)s) frecom_sence
                 ,coalesce(t1.frecom_pro, %(null_int_report)s) frecom_pro
                 ,coalesce(t1.frecom_click, %(null_int_report)s) frecom_click
                 ,coalesce(t1.frecom_res, %(null_int_report)s) frecom_res
                 ,coalesce(t1.frecom_match_name, '%(null_str_report)s') frecom_match_name
            from stage.match_recommend_stg t1  --推荐
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
                 ,coalesce(fparty_type,'%(null_str_group_rule)s')  fparty_type
                 ,coalesce(fpname,'%(null_str_group_rule)s')  fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s')  fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,coalesce(frecom_range, '%(null_str_group_rule)s') frecom_range
                 ,coalesce(frecom_match_name, '%(null_str_group_rule)s') frecom_match_name
                 ,coalesce(frecom_sence, %(null_int_group_rule)s) frecom_sence
                 ,coalesce(frecom_pro, %(null_int_group_rule)s) frecom_pro
                 ,coalesce(frecom_click, %(null_int_group_rule)s) frecom_click
                 ,coalesce(frecom_res, %(null_int_group_rule)s) frecom_res
                 ,count(distinct fuid) funum             --用户数
                 ,count(fuid) fcnt                       --次数
            from work.match_recommend_info_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
                    ,fpname
                    ,fsubname
                    ,fgsubname
                    ,frecom_range
                    ,frecom_sence
                    ,frecom_pro
                    ,frecom_click
                    ,frecom_res
                    ,frecom_match_name
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.match_recommend_info
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.match_recommend_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_match_recommend_info(sys.argv[1:])
a()
