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


class agg_user_gamestyle_tag_daily(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists veda.user_gamestyle_tag_daily (
            uid                            bigint        comment 'uid',
            total_duration                 bigint        comment '总时长',
            total_index                    int           comment '总时长整体指数',
            total_adjust_index             int           comment '总时长修正指数',
            gamestyle_tag                  varchar(10)   comment '游戏类型标签：占比最高类型1普通；2金流；3比赛',
            gamestyle_proportion           int           comment '游戏类型占比：最高占比*100，取整',
            gamestyle_duration             bigint        comment '游戏类型时长',
            gamestyle_index                int           comment '游戏类型整体指数',
            gamestyle_adjust_index         int           comment '游戏类型修正指数',
            lv1style_tag                   varchar(20)   comment '一级分类标签：占比最高子游戏id或者比赛类型',
            lv1style_proportion            int           comment '一级分类占比：最高占比*100，取整',
            lv1style_total_proportion      int           comment '一级分类总占比',
            lv1style_duration              bigint        comment '一级分类时长',
            lv1style_index                 int           comment '一级分类整体指数',
            lv1style_adjust_index          int           comment '一级分类修正指数',
            lv2style_tag                   varchar(50)   comment '二级分类标签：占比最高场次id或者比赛场次名称',
            lv2style_proportion            int           comment '二级分类占比：最高占比*100，取整',
            lv2style_total_proportion      int           comment '二级分类总占比',
            lv2style_duration              bigint        comment '二级分类时长',
            lv2style_index                 int           comment '二级分类整体指数',
            lv2style_adjust_index          int           comment '二级分类修正指数',
            fbpid                          varchar(50)   comment 'bpid',
            fgamefsk                       bigint        comment '游戏id',
            fgamename                      string        comment '游戏名称',
            fplatformfsk                   bigint        comment '平台id',
            fplatformname                  string        comment '平台名称',
            fhallfsk                       bigint        comment '大厅id',
            fhallname                      string        comment '大厅名称',
            fterminaltypefsk               bigint        comment '终端类型id',
            fterminaltypename              string        comment '终端类型名称',
            fversionfsk                    bigint        comment '版本id',
            fversionname                   string        comment '版本名称'
        )comment '用户玩牌风格标签表'
        partitioned by(dt string);
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

        hql = """--总体
            drop table if exists work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s as
          select t1.uid                       --uid
                 ,t1.total_duration_uid total_duration       --总时长
                 ,t2.total_adjust_index      --总时长修正指数
                 ,t1.style gamestyle_tag             --游戏类型标签：占比最高类型1普通；2金流；3比赛
                 ,t1.style_proportion gamestyle_proportion      --游戏类型占比：最高占比*100，取整
                 ,t1.style_duration gamestyle_duration        --游戏类型时长
                 ,t3.gamestyle_adjust_index    --游戏类型修正指数
                 ,t1.fbpid
                 ,t1.fgamefsk
                 ,t1.fgamename
                 ,t1.fplatformfsk
                 ,t1.fplatformname
                 ,t1.fhallfsk
                 ,t1.fhallname
                 ,t1.fterminaltypefsk
                 ,t1.fterminaltypename
                 ,t1.fversionfsk
                 ,t1.fversionname
            from (select t1.uid
                         ,t1.style
                         ,t1.style_proportion
                         ,t1.style_duration
                         ,sum(t1.style_duration) over(partition by t1.uid) total_duration_uid
                         ,sum(t1.style_duration) over(partition by t1.style) total_duration_style
                         ,sum(t1.style_duration) over() total_duration
                         ,row_number() over(partition by t1.uid order by t1.style_proportion desc) row_num
                         ,t1.fbpid
                         ,t1.fgamefsk
                         ,t1.fgamename
                         ,t1.fplatformfsk
                         ,t1.fplatformname
                         ,t1.fhallfsk
                         ,t1.fhallname
                         ,t1.fterminaltypefsk
                         ,t1.fterminaltypename
                         ,t1.fversionfsk
                         ,t1.fversionname
                    from veda.user_gamestyle_info_daily t1
                   where dt = '%(statdate)s'
                 ) t1
            left join (select 1 rownum
                              ,sum(case when dt = '%(statdate)s' then t1.style_duration end) * 100
                              /sum(case when dt = date_sub('%(statdate)s',1) then t1.style_duration end) total_adjust_index
                         from veda.user_gamestyle_info_daily t1
                        where dt >= date_sub('%(statdate)s',1)
                          and dt <= '%(statdate)s'
                 ) t2
              on t1.row_num = t2.rownum
            left join (select style
                              ,sum(case when dt = '%(statdate)s' then t1.style_duration end) * 100
                              /sum(case when dt = date_sub('%(statdate)s',1) then t1.style_duration end) gamestyle_adjust_index
                         from veda.user_gamestyle_info_daily t1
                        where dt >= date_sub('%(statdate)s',1)
                          and dt <= '%(statdate)s'
                        group by style
                 ) t3
              on t1.style = t3.style
           where row_num = 1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_gamestyle_tag_daily_tmp_2_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_2_%(statdatenum)s as
          select  t1.uid                       --uid
                 ,t1.lv1style_classification lv1style_tag              --一级分类标签：占比最高子游戏id或者比赛类型
                 ,t1.lv1style_proportion lv1style_proportion           --一级分类占比：最高占比*100，取整
                 ,t1.lv1style_duration* 100/t1.lv1style_total_proportion lv1style_total_proportion --一级分类总占比
                 ,t1.lv1style_duration lv1style_duration         --一级分类时长
                 ,t3.lv1style_adjust_index     --一级分类修正指数
            from (select t1.uid
                         ,t1.style
                         ,t1.lv1style_classification
                         ,t1.lv1style_proportion
                         ,t1.lv1style_duration
                         ,sum(t1.lv1style_duration) over(partition by t1.uid) lv1style_total_proportion
                         ,sum(t1.lv1style_duration) over(partition by t1.lv1style_classification) total_duration_style
                         ,row_number() over(partition by t1.uid order by t1.lv1style_proportion desc) row_num
                         ,t1.fbpid
                         ,t1.fgamefsk
                         ,t1.fgamename
                         ,t1.fplatformfsk
                         ,t1.fplatformname
                         ,t1.fhallfsk
                         ,t1.fhallname
                         ,t1.fterminaltypefsk
                         ,t1.fterminaltypename
                         ,t1.fversionfsk
                         ,t1.fversionname
                    from veda.user_gamelv1style_info_daily t1
                    join work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s t2
                      on t1.uid = t2.uid
                     and t1.style = t2.gamestyle_tag
                   where dt = '%(statdate)s'
                 ) t1
            left join (select lv1style_classification
                              ,sum(case when dt = '%(statdate)s' then t1.lv1style_duration end) * 100
                              /sum(case when dt = date_sub('%(statdate)s',1) then t1.lv1style_duration end) lv1style_adjust_index
                         from veda.user_gamelv1style_info_daily t1
                        where dt >= date_sub('%(statdate)s',1)
                          and dt <= '%(statdate)s'
                        group by lv1style_classification
                 ) t3
              on t1.lv1style_classification = t3.lv1style_classification
           where row_num = 1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_gamestyle_tag_daily_tmp_3_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_3_%(statdatenum)s as
          select  t1.uid  --uid
                 ,t1.lv2style_classification lv2style_tag          --二级分类标签：占比最高子游戏id或者比赛类型
                 ,t1.lv2style_proportion lv2style_proportion       --二级分类占比：最高占比*100，取整
                 ,t1.lv2style_duration* 100/t1.lv2style_total_proportion lv2style_total_proportion --二级分类总占比
                 ,t1.lv2style_duration lv2style_duration           --二级分类时长
                 ,t3.lv2style_adjust_index     --二级分类修正指数
            from (select t1.uid
                         ,t1.style
                         ,t1.lv1style_classification
                         ,t1.lv2style_classification
                         ,t1.lv2style_proportion
                         ,t1.lv2style_duration
                         ,sum(t1.lv2style_duration) over(partition by t1.uid) lv2style_total_proportion
                         ,sum(t1.lv2style_duration) over(partition by t1.lv2style_classification) total_duration_style
                         ,row_number() over(partition by t1.uid order by t1.lv2style_duration desc) row_num
                         ,t1.fbpid
                         ,t1.fgamefsk
                         ,t1.fgamename
                         ,t1.fplatformfsk
                         ,t1.fplatformname
                         ,t1.fhallfsk
                         ,t1.fhallname
                         ,t1.fterminaltypefsk
                         ,t1.fterminaltypename
                         ,t1.fversionfsk
                         ,t1.fversionname
                    from veda.user_gamelv2style_info_daily t1
                    join work.user_gamestyle_tag_daily_tmp_2_%(statdatenum)s t2
                      on t1.uid = t2.uid
                     and t1.lv1style_classification = t2.lv1style_tag
                   where dt = '%(statdate)s'
                 ) t1
            left join (select lv2style_classification
                              ,sum(case when dt = '%(statdate)s' then t1.lv2style_duration end) * 100
                              /sum(case when dt = date_sub('%(statdate)s',1) then t1.lv2style_duration end) lv2style_adjust_index
                         from veda.user_gamelv2style_info_daily t1
                        where dt >= date_sub('%(statdate)s',1)
                          and dt <= '%(statdate)s'
                        group by lv2style_classification
                 ) t3
              on t1.lv2style_classification = t3.lv2style_classification
           where row_num = 1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_gamestyle_tag_daily_tmp_4_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_4_%(statdatenum)s as
          select total_duration
                 ,floor(total_index/max(total_index) over()*100) total_index
            from (select  total_duration       --总时长
                         ,sum(total_duration) over(order by total_duration rows between unbounded preceding and current row) total_index
                    from (select total_duration       --总时长
                                 ,sum(total_duration) total_index
                            from work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s t1
                           group by total_duration
                          ) t1
                 ) t1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_gamestyle_tag_daily_tmp_5_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_5_%(statdatenum)s as
          select gamestyle_duration
                 ,floor(gamestyle_index/max(gamestyle_index) over()*100) gamestyle_index
            from (select  gamestyle_duration       --时长
                         ,sum(gamestyle_duration) over(order by gamestyle_duration rows between unbounded preceding and current row) gamestyle_index
                    from (select gamestyle_duration       --时长
                                 ,sum(gamestyle_duration) gamestyle_index
                            from work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s t1
                           group by gamestyle_duration
                          ) t1
                 ) t1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_gamestyle_tag_daily_tmp_6_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_6_%(statdatenum)s as
          select lv1style_duration
                 ,floor(lv1style_index/max(lv1style_index) over()*100) lv1style_index
            from (select  lv1style_duration       --时长
                         ,sum(lv1style_duration) over(order by lv1style_duration rows between unbounded preceding and current row) lv1style_index
                    from (select lv1style_duration       --时长
                                 ,sum(lv1style_duration) lv1style_index
                            from work.user_gamestyle_tag_daily_tmp_2_%(statdatenum)s t1
                           group by lv1style_duration
                          ) t1
                 ) t1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            drop table if exists work.user_gamestyle_tag_daily_tmp_7_%(statdatenum)s;
          create table work.user_gamestyle_tag_daily_tmp_7_%(statdatenum)s as
          select lv2style_duration
                 ,floor(lv2style_index/max(lv2style_index) over()*100) lv2style_index
            from (select  lv2style_duration       --时长
                         ,sum(lv2style_duration) over(order by lv2style_duration rows between unbounded preceding and current row) lv2style_index
                    from (select lv2style_duration       --时长
                                 ,sum(lv2style_duration) lv2style_index
                            from work.user_gamestyle_tag_daily_tmp_3_%(statdatenum)s t1
                           group by lv2style_duration
                          ) t1
                 ) t1
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table veda.user_gamestyle_tag_daily partition (dt = '%(statdate)s')
        select  t1.uid
               ,t1.total_duration
               ,t4.total_index
               ,t1.total_adjust_index
               ,t1.gamestyle_tag
               ,t1.gamestyle_proportion
               ,t1.gamestyle_duration
               ,t5.gamestyle_index
               ,t1.gamestyle_adjust_index
               ,t2.lv1style_tag
               ,t2.lv1style_proportion
               ,t2.lv1style_total_proportion
               ,t2.lv1style_duration
               ,t6.lv1style_index
               ,t2.lv1style_adjust_index
               ,t3.lv2style_tag
               ,t3.lv2style_proportion
               ,t3.lv2style_total_proportion
               ,t3.lv2style_duration
               ,t7.lv2style_index
               ,t3.lv2style_adjust_index
               ,t1.fbpid
               ,t1.fgamefsk
               ,t1.fgamename
               ,t1.fplatformfsk
               ,t1.fplatformname
               ,t1.fhallfsk
               ,t1.fhallname
               ,t1.fterminaltypefsk
               ,t1.fterminaltypename
               ,t1.fversionfsk
               ,t1.fversionname
          from work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s t1
          left join work.user_gamestyle_tag_daily_tmp_2_%(statdatenum)s t2
            on t1.uid = t2.uid
          left join work.user_gamestyle_tag_daily_tmp_3_%(statdatenum)s t3
            on t1.uid = t3.uid
          left join work.user_gamestyle_tag_daily_tmp_4_%(statdatenum)s t4
            on t1.total_duration = t4.total_duration
          left join work.user_gamestyle_tag_daily_tmp_5_%(statdatenum)s t5
            on t1.gamestyle_duration = t5.gamestyle_duration
          left join work.user_gamestyle_tag_daily_tmp_6_%(statdatenum)s t6
            on t2.lv1style_duration = t6.lv1style_duration
          left join work.user_gamestyle_tag_daily_tmp_7_%(statdatenum)s t7
            on t3.lv2style_duration = t7.lv2style_duration
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--删除临时表
            drop table if exists work.user_gamestyle_tag_daily_tmp_1_%(statdatenum)s;
            drop table if exists work.user_gamestyle_tag_daily_tmp_2_%(statdatenum)s;
            drop table if exists work.user_gamestyle_tag_daily_tmp_3_%(statdatenum)s;
            drop table if exists work.user_gamestyle_tag_daily_tmp_4_%(statdatenum)s;
            drop table if exists work.user_gamestyle_tag_daily_tmp_5_%(statdatenum)s;
            drop table if exists work.user_gamestyle_tag_daily_tmp_6_%(statdatenum)s;
            drop table if exists work.user_gamestyle_tag_daily_tmp_7_%(statdatenum)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_gamestyle_tag_daily(sys.argv[1:])
a()
