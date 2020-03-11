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


class agg_bud_promoter_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_promoter_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fsource_path        varchar(100)   comment  '来源路径',
               fpromoter           varchar(100)   comment  '推广员id',
               fnew_partner_unum   bigint         comment  '新增代理商',
               fact_partner_unum   bigint         comment  '活跃代理商',
               freg_unum           bigint         comment  '新增用户数',
               fact_unum           bigint         comment  '活跃用户数',
               fpay_unum           bigint         comment  '付费用户数',
               freg_play_unum      bigint         comment  '新增玩牌用户数',
               fact_play_unum      bigint         comment  '活跃玩牌用户数',
               fincome             decimal(16,2)  comment  '付费额度',
               freg_back_unum      bigint         comment  '昨注回头',
               fact_back_unum      bigint         comment  '昨日活跃回头'
               )comment '推广员明细'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_promoter_detail';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fsource_path, fpromoter),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fsource_path, fpromoter),
                               (fgamefsk, fgame_id, fsource_path, fpromoter) ) """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fsource_path, fpromoter),
                               (fgamefsk, fplatformfsk, fhallfsk, fsource_path, fpromoter),
                               (fgamefsk, fplatformfsk, fsource_path, fpromoter) ) """

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
        hql = """--取新增代理商
            drop table if exists work.bud_promoter_detail_tmp_1_%(statdatenum)s;
          create table work.bud_promoter_detail_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,'%(null_str_report)s' fsource_path
                 ,t1.fuid
                 ,t1.ftype
                 ,t1.fpromoter
            from (select fbpid, fuid, fpromoter,1 ftype
                    from stage.partner_join_stg where dt = '%(statdate)s'
                   union all
                  select fbpid, fuid, fpromoter,2 ftype
                    from dim.user_login_additional t1
                   where dt = '%(statdate)s' and fuid = fpartner_info and fpartner_info is not null
                 ) t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总一代理商系统
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fsource_path        --来源路径
                 ,fpromoter           --推广员id
                 ,count(distinct case when ftype = 1 then fuid end) fnew_partner_unum      --新增代理商
                 ,count(distinct case when ftype = 2 then fuid end) fact_partner_unum  --活跃代理商
                 ,0 freg_unum           --新增用户数
                 ,0 fact_unum           --活跃用户数
                 ,0 fpay_unum           --付费用户数
                 ,0 freg_play_unum      --新增玩牌用户数
                 ,0 fact_play_unum      --活跃玩牌用户数
                 ,cast (0 as decimal(16,2)) fincome             --付费额度
                 ,0 freg_back_unum      --昨注回头
                 ,0 fact_back_unum      --昨日活跃回头
            from work.bud_promoter_detail_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fsource_path, fpromoter """

        # 组合
        hql = (
            """drop table if exists work.bud_promoter_detail_tmp_%(statdatenum)s;
          create table work.bud_promoter_detail_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取昨日新增活跃用户
            drop table if exists work.bud_promoter_detail_tmp_2_%(statdatenum)s;
          create table work.bud_promoter_detail_tmp_2_%(statdatenum)s as
          select t1.fbpid
                 ,t1.fuid all_fuid          --所有发展的用户
                 ,t2.fuid reg_fuid          --其中昨日新增
                 ,t3.fuid act_fuid          --其中昨日活跃
                 ,t3.flogin_cnt             --登陆次数
                 ,t3.fparty_num             --玩牌局数
            from dim.reg_user_share t1
            left join dim.reg_user_main_additional t2  --注册
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(ld_1day_ago)s'
            left join dim.user_act t3       --活跃
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(ld_1day_ago)s'
           where t1.dt <= '%(ld_1day_ago)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取当日新增活跃玩牌付费用户
            drop table if exists work.bud_promoter_detail_tmp_3_%(statdatenum)s;
          create table work.bud_promoter_detail_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t3.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fsource_path
                 ,coalesce(t1.fpartner_info,'0') fpartner
                 ,t1.fpromoter
                 ,t1.fuid all_fuid          --所有发展的用户
                 ,t2.fuid reg_fuid          --其中当日新增
                 ,t3.fuid act_fuid          --其中当日活跃
                 ,t4.fuid pay_fuid          --其中当日付费
                 ,t5.reg_fuid y_reg_fuid    --其中昨日新增当日活跃
                 ,t5.act_fuid y_act_fuid    --其中昨日活跃当日活跃
                 ,t4.ftotal_usd_amt         --其中当日付费额度
                 ,t3.flogin_cnt             --登陆次数
                 ,t3.fparty_num             --玩牌局数
                 ,t3.fis_change_gamecoins   --金流是否发生变化
            from dim.reg_user_share t1
            left join dim.reg_user_main_additional t2  --注册
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            left join dim.user_act t3       --活跃
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            left join dim.user_pay_day t4   --付费
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
             and t4.dt = '%(statdate)s'
            left join work.bud_promoter_detail_tmp_2_%(statdatenum)s t5   --昨日新增活跃
              on t1.fbpid = t5.fbpid
             and t1.fuid = t5.all_fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt <= '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总三推广员
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fsource_path        --来源路径
                 ,fpromoter           --推广员id
                 ,0 fnew_partner_unum   --新增代理商
                 ,0 fact_partner_unum   --活跃代理商
                 ,count(distinct reg_fuid) freg_unum           --新增用户数
                 ,count(distinct act_fuid) fact_unum           --活跃用户数
                 ,count(distinct pay_fuid) fpay_unum           --付费用户数
                 ,count(distinct case when fparty_num > 0 then reg_fuid end) freg_play_unum      --新增玩牌用户数
                 ,count(distinct case when fparty_num > 0 then act_fuid end) fact_play_unum      --活跃玩牌用户数
                 ,sum(ftotal_usd_amt) fincome                  --付费额度
                 ,count(distinct case when act_fuid is not null then y_reg_fuid end) freg_back_unum      --昨注回头
                 ,count(distinct case when act_fuid is not null then y_act_fuid end) fact_back_unum      --昨日活跃回头
            from work.bud_promoter_detail_tmp_3_%(statdatenum)s t
           where t.fpartner <> '0'
             and t.fgame_id <> -13658
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fsource_path, fpromoter
         """

        # 组合
        hql = (
            """insert into table work.bud_promoter_detail_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总三推广员
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fsource_path        --来源路径
                 ,fpromoter           --推广员id
                 ,0 fnew_partner_unum   --新增代理商
                 ,0 fact_partner_unum   --活跃代理商
                 ,count(distinct reg_fuid) freg_unum           --新增用户数
                 ,count(distinct act_fuid) fact_unum           --活跃用户数
                 ,count(distinct pay_fuid) fpay_unum           --付费用户数
                 ,count(distinct case when fparty_num > 0 then reg_fuid end) freg_play_unum      --新增玩牌用户数
                 ,count(distinct case when fparty_num > 0 then act_fuid end) fact_play_unum      --活跃玩牌用户数
                 ,sum(ftotal_usd_amt) fincome                  --付费额度
                 ,count(distinct case when act_fuid is not null then y_reg_fuid end) freg_back_unum      --昨注回头
                 ,count(distinct case when act_fuid is not null then y_act_fuid end) fact_back_unum      --昨日活跃回头
            from work.bud_promoter_detail_tmp_3_%(statdatenum)s t
           where t.fpartner <> '0'
             and t.fgame_id = -13658
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fsource_path, fpromoter
         """

        # 组合
        hql = (
            """insert into table work.bud_promoter_detail_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table bud_dm.bud_promoter_detail  partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fgame_id
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,fsource_path        --来源路径
                          ,fpromoter           --推广员id
                          ,max(coalesce(fnew_partner_unum, 0)) fnew_partner_unum   --新增代理商
                          ,max(coalesce(fact_partner_unum, 0)) fact_partner_unum   --活跃代理商
                          ,max(coalesce(freg_unum        , 0)) freg_unum           --新增用户数
                          ,max(coalesce(fact_unum        , 0)) fact_unum           --活跃用户数
                          ,max(coalesce(fpay_unum        , 0)) fpay_unum           --付费用户数
                          ,max(coalesce(freg_play_unum   , 0)) freg_play_unum      --新增玩牌用户数
                          ,max(coalesce(fact_play_unum   , 0)) fact_play_unum      --活跃玩牌用户数
                          ,max(coalesce(fincome          , 0)) fincome             --付费额度
                          ,max(coalesce(freg_back_unum   , 0)) freg_back_unum      --昨注回头
                          ,max(coalesce(fact_back_unum   , 0)) fact_back_unum      --昨日活跃回头
                     from work.bud_promoter_detail_tmp_%(statdatenum)s
                    where fnew_partner_unum + fact_partner_unum + freg_unum + fact_unum + fpay_unum > 0
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                             ,fsource_path,fpromoter;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_promoter_detail_tmp_%(statdatenum)s;
                 drop table if exists work.bud_promoter_detail_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_promoter_detail_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_promoter_detail_tmp_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_promoter_detail(sys.argv[1:])
a()
