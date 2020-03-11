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


class agg_user_gamestyle_info_daily(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists veda.user_gamestyle_info_daily (
            uid                  bigint        comment 'uid',
            style                string        comment '游戏类型：1普通；2金流；3比赛',
            style_proportion     int           comment '游戏类型占比：占比*100，取整',
            style_duration       bigint        comment '游戏类型时长',
            fbpid                varchar(50)   comment 'bpid',
            fgamefsk             bigint        comment '游戏id',
            fgamename            string        comment '游戏名称',
            fplatformfsk         bigint        comment '平台id',
            fplatformname        string        comment '平台名称',
            fhallfsk             bigint        comment '大厅id',
            fhallname            string        comment '大厅名称',
            fterminaltypefsk     bigint        comment '终端类型id',
            fterminaltypename    string        comment '终端类型名称',
            fversionfsk          bigint        comment '版本id',
            fversionname         string        comment '版本名称'
        )comment '用户玩牌风格信息表'
        partitioned by(dt string);

        create table if not exists veda.user_gamelv1style_info_daily(
            uid                           bigint        comment 'uid',
            style                         string        comment '游戏类型：1普通；2金流；3比赛',
            lv1style_classification       string        comment '一级分类：子游戏id或者比赛类型',
            lv1style_proportion           int           comment '一级分类占比：占比*100，取整',
            lv1style_duration             bigint        comment '一级分类时长',
            fbpid                         varchar(50)   comment 'bpid',
            fgamefsk                      bigint        comment '游戏id',
            fgamename                     string        comment '游戏名称',
            fplatformfsk                  bigint        comment '平台id',
            fplatformname                 string        comment '平台名称',
            fhallfsk                      bigint        comment '大厅id',
            fhallname                     string        comment '大厅名称',
            fterminaltypefsk              bigint        comment '终端类型id',
            fterminaltypename             string        comment '终端类型名称',
            fversionfsk                   bigint        comment '版本id',
            fversionname                  string        comment '版本名称'
        )comment '用户玩牌风格一级分类信息表'
        partitioned by(dt string);

        create table if not exists veda.user_gamelv2style_info_daily(
            uid                        bigint         comment 'uid',
            style                      string         comment '游戏类型：1普通；2金流；3比赛',
            lv1style_classification    string         comment '一级分类：子游戏id或者比赛类型',
            lv2style_classification    string         comment '二级分类：场次id或者比赛场次名称',
            lv2style_proportion        int            comment '二级分类占比：占比*100，取整',
            lv2style_duration          bigint         comment '二级分类时长',
            fbpid                      varchar(50)    comment 'bpid',
            fgamefsk                   bigint         comment '游戏id',
            fgamename                  string         comment '游戏名称',
            fplatformfsk               bigint         comment '平台id',
            fplatformname              string         comment '平台名称',
            fhallfsk                   bigint         comment '大厅id',
            fhallname                  string         comment '大厅名称',
            fterminaltypefsk           bigint         comment '终端类型id',
            fterminaltypename          string         comment '终端类型名称',
            fversionfsk                bigint         comment '版本id',
            fversionname               string         comment '版本名称'
        )comment '用户玩牌风格二级分类信息表'
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

        hql = """--流水
            drop table if exists work.user_gamestyle_info_daily_tmp_1_%(statdatenum)s;
          create table work.user_gamestyle_info_daily_tmp_1_%(statdatenum)s as
          select fuid
                 ,style
                 ,lv1style_classification
                 ,lv2style_classification
                 ,style_duration
                 ,tt.fbpid
                 ,tt.fgamefsk
                 ,tt.fgamename
                 ,tt.fplatformfsk
                 ,tt.fplatformname
                 ,tt.fhallfsk
                 ,tt.fhallname
                 ,tt.fterminaltypefsk
                 ,tt.fterminaltypename
                 ,tt.fversionfsk
                 ,tt.fversionname
            from (select fuid
                         ,case when fmatch_cfg_id is not null or fmatch_id is not null then '比赛'
                               when t1.subgame_id is not null then '金流'
                               else '普通' end style
                         ,case when fmatch_cfg_id is not null or fmatch_id is not null then fsubname
                               else coalesce(fpname, '未知') end lv1style_classification
                         ,case when fmatch_cfg_id is not null or fmatch_id is not null then fgsubname
                               else coalesce(fsubname, '未知') end lv2style_classification
                         ,sum(case when fs_timer = '1970-01-01 00:00:00' then 0
                                   when fe_timer = '1970-01-01 00:00:00' then 0
                                   when unix_timestamp(fe_timer)-unix_timestamp(fs_timer) >=3600 then 0
                                   else unix_timestamp(fe_timer)-unix_timestamp(fs_timer)
                                   end) style_duration
                    from stage_dfqp.user_gameparty_stg t
                    left join dw_dfqp.conf_subgame_properties t1
                      on t.fgame_id = t1.subgame_id
                   where dt = '%(statdate)s'
                   group by fuid
                            ,case when fmatch_cfg_id is not null or fmatch_id is not null then '比赛'
                                  when t1.subgame_id is not null then '金流'
                                  else '普通' end
                            ,case when fmatch_cfg_id is not null or fmatch_id is not null then fsubname
                                  else coalesce(fpname, '未知') end
                            ,case when fmatch_cfg_id is not null or fmatch_id is not null then fgsubname
                                  else coalesce(fsubname, '未知') end
                   grouping sets ((fuid
                                  ,case when fmatch_cfg_id is not null or fmatch_id is not null then '比赛'
                                        when t1.subgame_id is not null then '金流'
                                        else '普通' end
                                  ,case when fmatch_cfg_id is not null or fmatch_id is not null then fsubname
                                        else coalesce(fpname, '未知') end
                                  ,case when fmatch_cfg_id is not null or fmatch_id is not null then fgsubname
                                        else coalesce(fsubname, '未知') end),
                                  (fuid
                                  ,case when fmatch_cfg_id is not null or fmatch_id is not null then '比赛'
                                        when t1.subgame_id is not null then '金流'
                                        else '普通' end
                                  ,case when fmatch_cfg_id is not null or fmatch_id is not null then fsubname
                                        else coalesce(fpname, '未知') end),
                                  (fuid
                                  ,case when fmatch_cfg_id is not null or fmatch_id is not null then '比赛'
                                        when t1.subgame_id is not null then '金流'
                                        else '普通' end))
                 ) t1
          left join veda.dfqp_user_portrait_basic t2
            on t1.fuid = t2.mid
          join dim.bpid_map tt
            on t2.signup_bpid = tt.fbpid
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table veda.user_gamestyle_info_daily partition (dt = '%(statdate)s')
        select t1.fuid
               ,t1.style
               ,sum(t1.style_proportion) style_proportion
               ,sum(t1.style_duration) style_duration
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
          from (select t1.fuid
                      ,t1.style
                      ,round(t1.style_duration/sum(style_duration) over(partition by t1.fuid)*100) style_proportion
                      ,t1.style_duration style_duration
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
                 from work.user_gamestyle_info_daily_tmp_1_%(statdatenum)s t1
                where lv2style_classification is null
                  and lv1style_classification is null
               ) t1
         group by t1.fuid
                  ,t1.style
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
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table veda.user_gamelv1style_info_daily partition (dt = '%(statdate)s')
        select t1.fuid
               ,t1.style
               ,t1.lv1style_classification
               ,sum(t1.lv1style_proportion) lv1style_proportion
               ,sum(t1.lv1style_duration) lv1style_duration
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
          from (select t1.fuid
                       ,t1.style
                       ,t1.lv1style_classification
                       ,round(t1.style_duration/sum(style_duration) over(partition by t1.fuid,t1.style)*100) lv1style_proportion
                       ,t1.style_duration lv1style_duration
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
                 from work.user_gamestyle_info_daily_tmp_1_%(statdatenum)s t1
                where lv2style_classification is null
                  and lv1style_classification is not null
               ) t1
         group by t1.fuid
                  ,t1.style
                  ,t1.lv1style_classification
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
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table veda.user_gamelv2style_info_daily partition (dt = '%(statdate)s')
        select t1.fuid
               ,t1.style
               ,t1.lv1style_classification
               ,t1.lv2style_classification
               ,sum(t1.lv2style_proportion) lv2style_proportion
               ,sum(t1.lv2style_duration) lv2style_duration
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
          from (select t1.fuid
                      ,t1.style
                      ,t1.lv1style_classification
                      ,t1.lv2style_classification
                      ,round(t1.style_duration/sum(style_duration) over(partition by t1.fuid,t1.style,t1.lv1style_classification)*100) lv2style_proportion
                      ,t1.style_duration lv2style_duration
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
                 from work.user_gamestyle_info_daily_tmp_1_%(statdatenum)s t1
                where lv2style_classification is not null
                  and lv1style_classification is not null
               ) t1
         group by t1.fuid
                  ,t1.style
                  ,t1.lv1style_classification
                  ,t1.lv2style_classification
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

         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--删除临时表
            drop table if exists work.user_gamestyle_info_daily_tmp_1_%(statdatenum)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_gamestyle_info_daily(sys.argv[1:])
a()
