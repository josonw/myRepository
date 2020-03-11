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


class agg_bud_user_bankrupt_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_bankrupt_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               frupt_unum          bigint     comment '破产人数',
               frupt_num           bigint     comment '破产人次',
               frlv_unum           bigint     comment '救济人数',
               frlv_num            bigint     comment '救济人次',
               frlv_gamecoins      bigint     comment '救济金币数',
               frupt_pay_unum      bigint     comment '破产(十分钟)付费人数',
               frupt_pay_cnt       bigint     comment '破产(十分钟)付费次数',
               frupt_pay_income    bigint     comment '破产(十分钟)付费额度'
               )comment '破产救济用户信息表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_bankrupt_info';
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
                               (fgamefsk, fplatformfsk) ) """

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
        hql = """--取破产救济相关指标
            drop table if exists work.bud_user_bankrupt_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_bankrupt_info_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t1.frupt_cnt           --破产次数
                 ,t1.frlv_cnt            --救济次数
                 ,t1.frlv_gamecoins      --救济金币数
            from dim.user_bankrupt_relieve t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct case when frupt_cnt >0 then fuid end) frupt_unum     --破产人数
                 ,sum(frupt_cnt) frupt_num                                            --破产人次
                 ,count(distinct case when frlv_cnt >0 then fuid end) frlv_unum       --救济人数
                 ,sum(frlv_cnt) frlv_num                                              --救济人次
                 ,sum(frlv_gamecoins) frlv_gamecoins                                  --救济金币数
                 ,0 frupt_pay_unum         --破产付费人数
                 ,0 frupt_pay_cnt          --破产付费次数
                 ,0 frupt_pay_income       --破产付费额度
            from work.bud_user_bankrupt_info_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """drop table if exists work.bud_user_bankrupt_info_tmp_%(statdatenum)s;
          create table work.bud_user_bankrupt_info_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取破产付费相关指标
            drop table if exists work.bud_user_bankrupt_info_tmp_2_%(statdatenum)s;
          create table work.bud_user_bankrupt_info_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t3.fplatform_uid
                 ,t3.fcoins_num * t3.frate fbank_30min_income
                 ,t3.forder_id
                 ,case when unix_timestamp(t3.fdate)-600 <= unix_timestamp(t1.frupt_at) then 1 else 0 end is_10m
            from stage.user_bankrupt_stg t1
            left join stage.payment_stream_stg t3
              on t1.fuid = t3.fuid
             and t1.fbpid = t3.fbpid
             and t3.dt='%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总2
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,0 frupt_unum     --破产人数
                 ,0 frupt_num      --破产人次
                 ,0 frlv_unum      --救济人数
                 ,0 frlv_num       --救济人次
                 ,0 frlv_gamecoins --救济金币数
                 ,count(distinct case when is_10m = 1 then fplatform_uid end) frupt_pay_unum    --破产付费人数
                 ,count(distinct case when is_10m = 1 then forder_id end) frupt_pay_cnt         --破产付费次数
                 ,sum(case when is_10m = 1 then fbank_30min_income end) frupt_pay_income        --破产付费额度
            from work.bud_user_bankrupt_info_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_user_bankrupt_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table bud_dm.bud_user_bankrupt_info  partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fgame_id
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,max(frupt_unum) frupt_unum               --破产人数
                          ,max(frupt_num) frupt_num                 --破产人次
                          ,max(frlv_unum) frlv_unum                 --救济人数
                          ,max(frlv_num) frlv_num                   --救济人次
                          ,max(frlv_gamecoins) frlv_gamecoins       --救济金币数
                          ,max(frupt_pay_unum) frupt_pay_unum       --破产付费人数
                          ,max(frupt_pay_cnt) frupt_pay_cnt         --破产付费次数
                          ,max(frupt_pay_income) frupt_pay_income   --破产付费额度
                     from work.bud_user_bankrupt_info_tmp_%(statdatenum)s
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_bankrupt_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_bankrupt_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_bankrupt_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_bankrupt_info(sys.argv[1:])
a()
