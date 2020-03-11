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


class agg_fzb_match_user_label_all(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists veda.fzb_match_user_label_all (
                fhallfsk              bigint           comment '大厅id',
                fuid                  bigint           comment '用户id',
                is_new                bigint           comment '是否登陆未满7天',
                rule_1                bigint           comment '规则1：比赛牌局数比普通牌局达0.058',
                rule_2                bigint           comment '规则2：只存在比赛牌局',
                rule_3                bigint           comment '规则3：日均比赛次数大于3',
                is_first              bigint           comment '是否首次打上比赛标签'
               )comment '比赛用户标签'
               partitioned by(dt string)
        location '/dw/veda/fzb_match_user_label_all';


        create table if not exists veda.fzb_match_user_label_info (
                fhallfsk                 bigint           comment '大厅id',
                flabel_unum              bigint           comment '比赛标签用户',
                fact_unum                bigint           comment '活跃用户',
                ffirst_label_unum        bigint           comment '首次打上比赛标签',
                flabel_income            decimal(20,2)    comment '比赛标签用户付费',
                fpay_income              decimal(20,2)    comment '总付费',
                flabel_income_gold       decimal(20,2)    comment '比赛标签用户购买金条付费',
                fpay_income_gold         decimal(20,2)    comment '总金条付费',
                fnew_label_unum          bigint           comment '登陆未满7天-比赛标签用户',
                fnew_label_income        decimal(20,2)    comment '登陆未满7天-比赛标签用户付费',
                fnew_label_income_gold   decimal(20,2)    comment '登陆未满7天-比赛标签用户金条付费',
                ffree_match_unum         bigint           comment '只参加免费比赛场次用户',
                ffree_match_income       decimal(20,2)    comment '只参加免费比赛场次用户付费'
               )comment '比赛用户标签'
               partitioned by(dt string)
        location '/dw/veda/fzb_match_user_label_info';
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
        hql = """--取用户除本次外最近七次登陆信息，不满七次全取
            drop table if exists work.fzb_match_user_label_all_tmp_1_%(statdatenum)s;
          create table work.fzb_match_user_label_all_tmp_1_%(statdatenum)s as
            select t.fhallfsk,
                   t.fuid,
                   sum(t1.free_party_num) free_party_num,
                   sum(t1.award_party_num) award_party_num,
                   sum(t1.free_match_num) free_match_num,
                   sum(t1.award_match_num) award_match_num,
                   sum(t1.fnor_party_num) fnor_party_num,
                   count(1) login_day
              from veda.fzb_match_user_label t
              left join veda.fzb_match_user_label t1
                on t.fhallfsk = t1.fhallfsk
               and t.fuid = t1.fuid
               and t1.dt < '%(statdate)s'
             where t.dt = '%(statdate)s'
               and t1.flogin_day <= t.flogin_day - 1
               and t1.flogin_day >= t.flogin_day - 7
             group by t.fhallfsk,
                      t.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--按三个规则匹配所有用户
            drop table if exists work.fzb_match_user_label_all_tmp_2_%(statdatenum)s;
          create table work.fzb_match_user_label_all_tmp_2_%(statdatenum)s as
            select t1.fhallfsk
                   ,t1.fuid
                   ,case when login_day < 7 then 1 else 0 end is_new
                   ,case when (fnor_party_num + free_party_num) > 0
                          and award_party_num * 1.0 /(fnor_party_num + free_party_num) > 0.058 then 1 else 0 end rule_1
                   ,case when fnor_party_num + free_party_num = 0 and award_party_num >0 then 1 else 0 end rule_2
                   ,case when award_match_num * 1.0 / login_day >3 then 1 else 0 end rule_3
              from work.fzb_match_user_label_all_tmp_1_%(statdatenum)s t1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--将满足规则的用户打上标签
            insert overwrite table veda.fzb_match_user_label_all
                partition(dt='%(statdate)s')
            select t.fhallfsk
                   ,t.fuid
                   ,t.is_new
                   ,t.rule_1
                   ,t.rule_2
                   ,t.rule_3
                   ,case when t1.fuid is null then 1 else 0 end is_first
              from work.fzb_match_user_label_all_tmp_2_%(statdatenum)s t
              left join (select distinct fhallfsk,fuid from veda.fzb_match_user_label_all where dt < '%(statdate)s') t1
                on t.fhallfsk = t1.fhallfsk
               and t.fuid = t1.fuid
             where rule_1 + rule_2 + rule_3 > 0
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取出所有所需数据
            insert overwrite table veda.fzb_match_user_label_info
                partition(dt='%(statdate)s')
        select fhallfsk
               ,sum(flabel_unum) flabel_unum
               ,sum(fact_unum) fact_unum
               ,sum(ffirst_label_unum) ffirst_label_unum
               ,sum(flabel_income) flabel_income
               ,sum(fpay_income) fpay_income
               ,sum(flabel_income_gold) flabel_income_gold
               ,sum(fpay_income_gold) fpay_income_gold
               ,sum(fnew_label_unum) fnew_label_unum
               ,sum(fnew_label_income) fnew_label_income
               ,sum(fnew_label_income_gold) fnew_label_income_gold
               ,sum(ffree_match_unum) ffree_match_unum
               ,sum(ffree_match_income) ffree_match_income
          from (select t.fhallfsk
                       ,count(distinct t.fuid) flabel_unum
                       ,0 fact_unum
                       ,count(distinct case when is_first = 1 then t.fuid end) ffirst_label_unum
                       ,sum(t1.fpay_income) flabel_income
                       ,cast (0 as decimal(20,2)) fpay_income
                       ,sum(t1.fpay_income_gold) flabel_income_gold
                       ,cast (0 as decimal(20,2)) fpay_income_gold
                       ,count(distinct case when is_new = 1 then t.fuid end) fnew_label_unum
                       ,sum(case when is_new = 1 then t1.fpay_income end) fnew_label_income
                       ,sum(case when is_new = 1 then t1.fpay_income_gold end) fnew_label_income_gold
                       ,0 ffree_match_unum
                       ,cast (0 as decimal(20,2)) ffree_match_income
                  from veda.fzb_match_user_label_all t
                  left join veda.fzb_match_user_label t1
                    on t.fhallfsk = t1.fhallfsk
                   and t.fuid = t1.fuid
                   and t1.dt = '%(statdate)s'
                 where t.dt = '%(statdate)s'
                 group by t.fhallfsk
                 union all
                select t.fhallfsk
                       ,0 flabel_unum
                       ,sum(fact_unum) fact_unum
                       ,0 ffirst_label_unum
                       ,0 flabel_income
                       ,0 fpay_income
                       ,0 flabel_income_gold
                       ,0 fpay_income_gold
                       ,0 fnew_label_unum
                       ,0 fnew_label_income
                       ,0 fnew_label_income_gold
                       ,0 ffree_match_unum
                       ,0 ffree_match_income
                  from bud_dm.bud_user_act_info t
                 where t.dt = '%(statdate)s'
                   and fgamefsk = 4132314431
                   and fhallfsk <> -21379
                   and fsubgamefsk = -21379
                   and fversionfsk = -21379
                 group by fhallfsk
                 union all
                select t1.fhallfsk
                       ,0 flabel_unum
                       ,0 fact_unum
                       ,0 ffirst_label_unum
                       ,0 flabel_income
                       ,sum(fpay_income) fpay_income
                       ,0 flabel_income_gold
                       ,sum(case when t2.ftype = 2 then t1.fpay_income end) fpay_income_gold
                       ,0 fnew_label_unum
                       ,0 fnew_label_income
                       ,0 fnew_label_income_gold
                       ,0 ffree_match_unum
                       ,0 ffree_match_income
                  from bud_dm.bud_user_payscene_product_detail t1
                  left join veda.dfqp_product_dim t2
                    on t1.fproduct_id = t2.fp_id
                 where t1.dt = '%(statdate)s'
                   and t1.fgamefsk = 4132314431
                   and t1.fhallfsk <> -21379
                   and fsubgamefsk = -21379
                   and fversionfsk = -21379
                   and fpay_scene = '-21379'
                 group by fhallfsk
                 union all
                select t.fhallfsk
                       ,0 flabel_unum
                       ,0 fact_unum
                       ,0 ffirst_label_unum
                       ,0 flabel_income
                       ,0 fpay_income
                       ,0 flabel_income_gold
                       ,0 fpay_income_gold
                       ,0 fnew_label_unum
                       ,0 fnew_label_income
                       ,0 fnew_label_income_gold
                       ,count(distinct fuid) ffree_match_unum
                       ,sum(fpay_income) ffree_match_income
                  from veda.fzb_match_user_label t
                 where t.dt = '%(statdate)s'
                   and free_party_num > 0
                   and award_party_num = 0
                   and fnor_party_num = 0
                 group by fhallfsk
              ) t
          group by fhallfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.fzb_match_user_label_all_tmp_1_%(statdatenum)s;
                 drop table if exists work.fzb_match_user_label_all_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_match_user_label_all(sys.argv[1:])
a()
