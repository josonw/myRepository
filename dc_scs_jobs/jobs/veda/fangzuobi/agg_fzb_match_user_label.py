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


class agg_fzb_match_user_label(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists veda.fzb_match_user_label (
                fhallfsk              bigint           comment '大厅id',
                fuid                  bigint           comment '用户id',
                free_party_num        bigint           comment '免费比赛牌局',
                award_party_num       bigint           comment '有偿比赛牌局',
                free_match_num        bigint           comment '免费比赛次数',
                award_match_num       bigint           comment '有偿比赛次数',
                fnor_party_num        bigint           comment '普通牌局数',
                fpay_income           decimal(20,2)    comment '当日付费',
                fpay_income_gold      decimal(20,2)    comment '当日金条付费',
                flogin_day            bigint           comment '累计登录天数'
               )comment '比赛用户标签'
               partitioned by(dt string)
        location '/dw/veda/fzb_match_user_label';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--金条报名比赛以及金条报名比赛中的免费次数
            drop table if exists work.fzb_match_user_label_tmp_1_%(statdatenum)s;
          create table work.fzb_match_user_label_tmp_1_%(statdatenum)s as
            select t.fbpid,t.fmatch_id,t.fuid,0 is_free
              from dim.join_gameparty t
              join (select distinct fmatch_cfg_id
                      from dim.join_gameparty t
                     where dt = '%(statdate)s'
                       and fitem_id = '1') t1  --当天金条报名比赛
                on t.fmatch_cfg_id = t1.fmatch_cfg_id
               join dim.bpid_map_bud tt
                 on t.fbpid = tt.fbpid
               and tt.fgamefsk = 4132314431
             where t.dt = '%(statdate)s'
               and t.fentry_fee = 0
               and t.fis_fail = 0 --金条报名比赛中的免费次数
             union
            select t.fbpid,t.fmatch_id,t.fuid,1 is_free
              from dim.join_gameparty t
               join dim.bpid_map_bud tt
                 on t.fbpid = tt.fbpid
               and tt.fgamefsk = 4132314431
             where t.dt = '%(statdate)s'
               and t.fitem_id = '1'
               and t.fentry_fee > 0
               and t.fis_fail = 0;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--比赛牌局及免费次数牌局
            drop table if exists work.fzb_match_user_label_tmp_2_%(statdatenum)s;
          create table work.fzb_match_user_label_tmp_2_%(statdatenum)s as
            select t1.fbpid
                   ,t1.fuid
                   ,count(case when is_free = 0 then 1 end) free_party_num
                   ,count(case when is_free = 1 then 1 end) award_party_num
                   ,count(distinct case when is_free = 0 then t1.fmatch_id end) free_match_num
                   ,count(distinct case when is_free = 1 then t1.fmatch_id end) award_match_num
              from dim.match_gameparty t1
              join work.fzb_match_user_label_tmp_1_%(statdatenum)s t2
                on t1.fmatch_id = t2.fmatch_id
               and t1.fuid = t2.fuid
               join dim.bpid_map_bud tt
                 on t1.fbpid = tt.fbpid
               and tt.fgamefsk = 4132314431
             where t1.dt = '%(statdate)s'
             group by t1.fbpid
                   ,t1.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--普通牌局
            drop table if exists work.fzb_match_user_label_tmp_3_%(statdatenum)s;
          create table work.fzb_match_user_label_tmp_3_%(statdatenum)s as
            select t1.fbpid,t1.fuid,count(1) fnor_party_num
              from stage_dfqp.user_gameparty_stg t1
              join dim.bpid_map_bud tt
                on t1.fbpid = tt.fbpid
               and tt.fgamefsk = 4132314431
             where t1.dt = '%(statdate)s'
               and concat_ws('_', cast (t1.fmatch_cfg_id as string), cast (t1.fmatch_log_id as string))<>'0_0'
             group by t1.fbpid,t1.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--当日数据汇总
            drop table if exists work.fzb_match_user_label_tmp_4_%(statdatenum)s;
          create table work.fzb_match_user_label_tmp_4_%(statdatenum)s as
            select tt.fhallfsk,
                   t.fuid,
                   sum(nvl(t1.free_party_num,0)) free_party_num,
                   sum(nvl(t1.award_party_num,0)) award_party_num,
                   sum(nvl(t1.free_match_num,0)) free_match_num,
                   sum(nvl(t1.award_match_num,0)) award_match_num,
                   sum(nvl(t2.fnor_party_num,0)) fnor_party_num,
                   sum(nvl(t3.fpay_income,0)) fpay_income,
                   sum(case when t4.ftype = 2 then nvl(t3.fpay_income,0) else 0 end) fpay_income_gold
              from dim.user_act_main t
              left join work.fzb_match_user_label_tmp_2_%(statdatenum)s t1
                on t.fbpid = t1.fbpid
               and t.fuid = t1.fuid
              left join work.fzb_match_user_label_tmp_3_%(statdatenum)s t2
                on t.fbpid = t2.fbpid
               and t.fuid = t2.fuid
              left join veda.user_pay_cube t3
                on t.fuid = t3.fuid
               and t3.dt = '%(statdate)s'
              left join veda.dfqp_product_dim t4
                on t4.fp_id = t3.fproduct_id
              join dim.bpid_map_bud tt
                on t1.fbpid = tt.fbpid
               and tt.fgamefsk = 4132314431
             where t.dt = '%(statdate)s'
             group by tt.fhallfsk,
                      t.fuid
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--当日数据汇总+登录天数数据
            insert overwrite table veda.fzb_match_user_label
                partition(dt='%(statdate)s')
            select t.fhallfsk,
                   t.fuid,
                   t.free_party_num,
                   t.award_party_num,
                   t.free_match_num,
                   t.award_match_num,
                   t.fnor_party_num,
                   t.fpay_income,
                   t.fpay_income_gold,
                   1 + nvl(t1.flogin_day,0) flogin_day
              from work.fzb_match_user_label_tmp_4_%(statdatenum)s t
              left join veda.fzb_match_user_label t1
                on t.fhallfsk = t1.fhallfsk
               and t.fuid = t1.fuid
               and t1.dt = date_sub('%(statdate)s', 1)
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.fzb_match_user_label_tmp_1_%(statdatenum)s;
                 drop table if exists work.fzb_match_user_label_tmp_2_%(statdatenum)s;
                 drop table if exists work.fzb_match_user_label_tmp_3_%(statdatenum)s;
                 drop table if exists work.fzb_match_user_label_tmp_4_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_match_user_label(sys.argv[1:])
a()
