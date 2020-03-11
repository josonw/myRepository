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


class agg_bud_user_apply_ret_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_apply_ret_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               flast_date            date        comment '当日日期',
               fapply_unum           bigint      comment '当日报名用户',
               fmatch_unum           bigint      comment '当日参赛用户',
               ffapply_unum          bigint      comment '当日首次报名用户',
               ffmatch_unum          bigint      comment '当日首次参赛用户',
               fmatch_co_unum        bigint      comment '当日有偿参赛用户',
               fmatch_fr_unum        bigint      comment '当日无偿参赛用户',
               fapply_co_unum        bigint      comment '当日有偿报名用户',
               fapply_fr_unum        bigint      comment '当日无偿报名用户',
               fapply_ret_unum       bigint      comment '当日报名用户今日也报名',
               fmatch_ret_unum       bigint      comment '当日参赛用户今日也报名',
               ffapply_ret_unum      bigint      comment '当日首次报名用户今日也报名',
               ffmatch_ret_unum      bigint      comment '当日首次参赛用户今日也报名',
               fmatch_co_ret_unum    bigint      comment '当日有偿参赛用户今日也报名',
               fmatch_fr_ret_unum    bigint      comment '当日无偿参赛用户今日也报名',
               fapply_co_ret_unum    bigint      comment '当日有偿报名用户今日也报名',
               fapply_fr_ret_unum    bigint      comment '当日无偿报名用户今日也报名'
               )comment '报名留存'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_apply_ret_info';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, flast_date),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, flast_date),
                               (fgamefsk, fgame_id, flast_date) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, flast_date),
                               (fgamefsk, fplatformfsk, fhallfsk, flast_date),
                               (fgamefsk, fplatformfsk, flast_date) ) """

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
        hql = """--
            drop table if exists work.bud_user_apply_ret_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_apply_ret_info_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fmatch_id
                 ,t1.fuid
                 ,t1.fentry_fee        --报名费
                 ,t1.ffirst_apply      --是否首次报名
                 ,t1.ffirst_party      --是否首次参赛
                 ,t1.fjoin_flag        --报名标识
                 ,t1.fparty_flag       --参赛标识
                 ,t3.fuid apply_uid
                 ,t1.dt flast_date
            from dim.match_user t1
            left join dim.match_user t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
             and t3.fjoin_flag = 1 --今日报名
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
              or t1.dt='%(ld_14day_ago)s'
              or t1.dt='%(ld_30day_ago)s'
              or t1.dt='%(ld_60day_ago)s'
              or t1.dt='%(ld_90day_ago)s');
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
                 ,flast_date            --当日日期
                 ,count(distinct case when fjoin_flag = 1 then fuid end) fapply_unum           --当日报名用户
                 ,count(distinct case when fparty_flag = 1 then fuid end) fmatch_unum           --当日参赛用户
                 ,count(distinct case when ffirst_apply = 1 then fuid end) ffapply_unum          --当日首次报名用户
                 ,count(distinct case when ffirst_party = 1 then fuid end) ffmatch_unum          --当日首次参赛用户
                 ,count(distinct case when fparty_flag = 1 and fentry_fee > 0 then fuid end) fmatch_co_unum        --当日有偿参赛用户
                 ,count(distinct case when fparty_flag = 1 and fentry_fee = 0 then fuid end) fmatch_fr_unum        --当日无偿参赛用户
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee > 0 then fuid end) fapply_co_unum        --当日有偿报名用户
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee = 0 then fuid end) fapply_fr_unum        --当日无偿报名用户
                 ,count(distinct case when fjoin_flag = 1 then apply_uid end) fapply_ret_unum       --当日报名用户今日也报名
                 ,count(distinct case when fparty_flag = 1 then apply_uid end) fmatch_ret_unum       --当日参赛用户今日也报名
                 ,count(distinct case when ffirst_apply = 1 then apply_uid end) ffapply_ret_unum      --当日首次报名用户今日也报名
                 ,count(distinct case when ffirst_party = 1 then apply_uid end) ffmatch_ret_unum      --当日首次参赛用户今日也报名
                 ,count(distinct case when fparty_flag = 1 and fentry_fee > 0 then apply_uid end) fmatch_co_ret_unum    --当日有偿参赛用户今日也报名
                 ,count(distinct case when fparty_flag = 1 and fentry_fee = 0 then apply_uid end) fmatch_fr_ret_unum    --当日无偿参赛用户今日也报名
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee > 0 then apply_uid end) fapply_co_ret_unum    --当日有偿报名用户今日也报名
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee = 0 then apply_uid end) fapply_fr_ret_unum    --当日无偿报名用户今日也报名
            from work.bud_user_apply_ret_info_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flast_date
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_apply_ret_info
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_apply_ret_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_apply_ret_info(sys.argv[1:])
a()
