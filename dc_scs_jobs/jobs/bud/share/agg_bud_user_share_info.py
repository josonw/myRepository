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


class agg_bud_user_share_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_share_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fshare_unum         bigint      comment '分享人数',
               fshare_num          bigint      comment '分享次数',
               fcall_num           bigint      comment '访问次数',
               fclick_num          bigint      comment '点击下载次数',
               fdown_num           bigint      comment '下载次数',
               fshare_reg_unum     bigint      comment '分享新增人数'
               ) comment '用户分享信息'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_share_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        # 两组组合，共4种
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) )"""

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--分享人数与次数
            drop table if exists work.bud_user_share_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_share_info_tmp_1_%(statdatenum)s as
          select  /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
            from stage.share_send_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fresult = '2';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) fshare_unum      --分享人数
                 ,count(fuid) fshare_num                --分享次数
                 ,0 fcall_num           --访问次数
                 ,0 fclick_num          --点击下载次数
                 ,0 fdown_num           --下载次数
                 ,0 fshare_reg_unum     --分享新增人数
            from work.bud_user_share_info_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """drop table if exists work.bud_user_share_info_tmp_%(statdatenum)s;
          create table work.bud_user_share_info_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--点击分享
            drop table if exists work.bud_user_share_info_tmp_2_%(statdatenum)s;
          create table work.bud_user_share_info_tmp_2_%(statdatenum)s as
          select  /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t2.fuid
                 ,t1.fshare_act
            from stage.share_clicked_stg t1
            left join dim.reg_user_main_additional t2
              on t1.fbpid = t2.fbpid
             and t1.fshare_key = t2.fshare_key
             and t2.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,0 fshare_unum      --分享人数
                 ,0 fshare_num                --分享次数
                 ,count(case when fshare_act = 0 then 1 end) fcall_num           --访问次数
                 ,count(case when fshare_act = 1 then 1 end) fclick_num          --点击下载次数
                 ,count(case when fshare_act = 2 then 1 end) fdown_num           --下载次数
                 ,count(distinct fuid) fshare_reg_unum     --分享新增人数
            from work.bud_user_share_info_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_user_share_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """insert overwrite table bud_dm.bud_user_share_info
            partition(dt='%(statdate)s')
          select '%(statdate)s' fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,max(fshare_unum) fshare_unum          --分享人数
                 ,max(fshare_num) fshare_num            --分享次数
                 ,max(fcall_num) fcall_num              --访问次数
                 ,max(fclick_num) fclick_num            --点击下载次数
                 ,max(fdown_num) fdown_num              --下载次数
                 ,max(fshare_reg_unum) fshare_reg_unum  --分享新增人数
            from work.bud_user_share_info_tmp_%(statdatenum)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_share_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_share_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_share_info_tmp_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_share_info(sys.argv[1:])
a()
