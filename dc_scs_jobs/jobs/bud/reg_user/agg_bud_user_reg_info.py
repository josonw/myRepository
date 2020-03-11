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


class agg_bud_user_reg_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_reg_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               freg_unum           bigint         comment '新增用户人数',
               freg_play_unum      bigint         comment '新增用户玩牌人数',
               freg_play_cnt       bigint         comment '新增用户玩牌人次',
               freg_pay_unum       bigint         comment '新增用户付费人数',
               freg_pay_cnt        bigint         comment '新增用户付费人次',
               freg_log_unum       bigint         comment '新增用户登录人数',
               freg_log_cnt        bigint         comment '新增用户登录人次',
               freg_rupt_unum      bigint         comment '新增用户破产人数',
               freg_rupt_cnt       bigint         comment '新增用户破产人次',
               freg_rlv_unum       bigint         comment '新增用户救济人数',
               freg_rlv_cnt        bigint         comment '新增用户救济人次',
               freg_terminal_unum  bigint         comment '新增用户终端数', --已废弃
               freg_ip_cnt         bigint         comment '新增用户ip数',   --已废弃
               freg_pay_income     decimal(20,2)  comment '新增用户付费额度',
               fm_reg_unum         bigint         comment '主新增',
               fs_reg_unum         bigint         comment '次新增'
               )comment '新增用户信息表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_reg_info';
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
                               (fgamefsk, fgame_id) )"""

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
        hql = """--取注册相关指标子游戏新增
            drop table if exists work.bud_user_reg_info_tmp_%(statdatenum)s;
          create table work.bud_user_reg_info_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t2.flogin_cnt             --登陆次数
                 ,t2.fparty_num             --玩牌局数
                 ,t3.fpay_cnt               --付费次数
                 ,t3.ftotal_usd_amt         --付费额度
                 ,t4.frupt_cnt              --破产次数
                 ,t4.frlv_cnt               --救济次数
                 ,case when t5.fuid is not null then 1 else 0 end is_main  --主新增
                 ,case when t6.fuid is not null then 1 else 0 end is_sub   --次新增
            from dim.reg_user_sub t1
            left join dim.user_act t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t1.fgame_id = t2.fgame_id
             and t2.dt = '%(statdate)s'
            left join dim.user_pay_day t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t1.fgame_id = t3.fgame_id
             and t3.dt = '%(statdate)s'
            left join dim.user_bankrupt_relieve t4
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
             and t1.fgame_id = t4.fgame_id
             and t4.dt = '%(statdate)s'
            left join dim.city_change_first t5
              on t1.fbpid = t5.fbpid
             and t1.fuid = t5.fuid
             and t5.dt <= '%(statdate)s'
            left join dim.city_change t6
              on t1.fbpid = t6.fbpid
             and t1.fuid = t6.fuid
             and t6.dt <= '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fis_first = 1  --首次进入子游戏;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) freg_unum                                            --新增用户人数
                 ,count(distinct case when fparty_num >0 then fuid end) freg_play_unum      --新增用户玩牌人数
                 ,sum(fparty_num) freg_play_cnt                                             --新增用户玩牌人次
                 ,count(distinct case when fpay_cnt >0 then fuid end) freg_pay_unum         --新增用户付费人数
                 ,sum(fpay_cnt) freg_pay_cnt                                                --新增用户付费人次
                 ,count(distinct case when flogin_cnt >0 then fuid end) freg_log_unum       --新增用户登录人数
                 ,sum(flogin_cnt) freg_log_cnt                                              --新增用户登录人次
                 ,count(distinct case when frupt_cnt >0 then fuid end) freg_rupt_unum       --新增用户破产人数
                 ,sum(frupt_cnt) freg_rupt_cnt                                              --新增用户破产人次
                 ,count(distinct case when frlv_cnt >0 then fuid end) freg_rlv_unum         --新增用户救济人数
                 ,sum(frlv_cnt) freg_rlv_cnt                                                --新增用户救济人次
                 ,0 freg_terminal_unum                                    --新增用户终端数  （废弃）
                 ,0 freg_ip_cnt                                           --新增用户ip数    （废弃）
                 ,sum(ftotal_usd_amt) freg_pay_income                                       --新增用户付费额度
                 ,count(distinct case when is_main = 1 then fuid end) fm_reg_unum           --主新增
                 ,count(distinct case when is_sub = 1 and is_main = 0 then fuid end) fs_reg_unum           --次新增
            from work.bud_user_reg_info_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_reg_info
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据  大厅数据
        hql = """--取注册相关指标大厅新增
            drop table if exists work.bud_user_reg_info_1_tmp_%(statdatenum)s;
          create table work.bud_user_reg_info_1_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t2.flogin_cnt             --登陆次数
                 ,t2.fparty_num             --玩牌局数
                 ,t3.fpay_cnt               --付费次数
                 ,t3.ftotal_usd_amt         --付费额度
                 ,t4.frupt_cnt              --破产次数
                 ,t4.frlv_cnt               --救济次数
                 ,case when t5.fuid is not null then 1 else 0 end is_main  --主新增
                 ,case when t6.fuid is not null then 1 else 0 end is_sub   --次新增
            from dim.reg_user_main_additional t1
            left join (select fbpid,fuid,sum(flogin_cnt) flogin_cnt,sum(fparty_num) fparty_num
                         from dim.user_act_main
                        where dt = '%(statdate)s'
                        group by fbpid,fuid) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join (select fbpid,fuid,sum(fpay_cnt) fpay_cnt,sum(ftotal_usd_amt) ftotal_usd_amt
                         from dim.user_pay_day
                        where dt = '%(statdate)s'
                        group by fbpid,fuid) t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            left join (select fbpid,fuid,sum(frupt_cnt) frupt_cnt,sum(frlv_cnt) frlv_cnt
                         from dim.user_bankrupt_relieve
                        where dt = '%(statdate)s'
                        group by fbpid,fuid) t4
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
            left join dim.city_change_first t5
              on t1.fbpid = t5.fbpid
             and t1.fuid = t5.fuid
             and t5.dt <= '%(statdate)s'
            left join dim.city_change t6
              on t1.fbpid = t6.fbpid
             and t1.fuid = t6.fuid
             and t6.dt <= '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s' ;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) freg_unum                                            --新增用户人数
                 ,count(distinct case when fparty_num >0 then fuid end) freg_play_unum      --新增用户玩牌人数
                 ,sum(fparty_num) freg_play_cnt                                             --新增用户玩牌人次
                 ,count(distinct case when fpay_cnt >0 then fuid end) freg_pay_unum         --新增用户付费人数
                 ,sum(fpay_cnt) freg_pay_cnt                                                --新增用户付费人次
                 ,count(distinct case when flogin_cnt >0 then fuid end) freg_log_unum       --新增用户登录人数
                 ,sum(flogin_cnt) freg_log_cnt                                              --新增用户登录人次
                 ,count(distinct case when frupt_cnt >0 then fuid end) freg_rupt_unum       --新增用户破产人数
                 ,sum(frupt_cnt) freg_rupt_cnt                                              --新增用户破产人次
                 ,count(distinct case when frlv_cnt >0 then fuid end) freg_rlv_unum         --新增用户救济人数
                 ,sum(frlv_cnt) freg_rlv_cnt                                                --新增用户救济人次
                 ,0 freg_terminal_unum                                    --新增用户终端数  （废弃）
                 ,0 freg_ip_cnt                                           --新增用户ip数    （废弃）
                 ,sum(ftotal_usd_amt) freg_pay_income                                       --新增用户付费额度
                 ,count(distinct case when is_main = 1 then fuid end) fm_reg_unum           --主新增
                 ,count(distinct case when is_sub = 1 and is_main = 0 then fuid end) fs_reg_unum           --次新增
            from work.bud_user_reg_info_1_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_user_reg_info
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_reg_info_tmp_%(statdatenum)s;
                 drop table if exists work.bud_user_reg_info_1_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_reg_info(sys.argv[1:])
a()
