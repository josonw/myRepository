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


class agg_bud_user_quit_match_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_quit_match_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fquit_unum            bigint          comment '取消报名人数',
               fquit_cnt             bigint          comment '取消报名人次',
               fz_quit_unum          bigint          comment '主动取消报名人数',
               fz_quit_cnt           bigint          comment '主动取消报名人次',
               fb_quit_unum          bigint          comment '被动取消报名人数',
               fb_quit_cnt           bigint          comment '被动取消报名人次',
               fout_unum             bigint          comment '退赛人数',
               fout_cnt              bigint          comment '退赛人次',
               fz_out_unum           bigint          comment '主动退赛人数',
               fz_out_cnt            bigint          comment '主动退赛人次',
               fb_out_unum           bigint          comment '被动退赛人数',
               fb_out_cnt            bigint          comment '被动退赛人次',
               fdieout_unum          bigint          comment '淘汰人数',
               fdieout_cnt           bigint          comment '淘汰人次'
               )comment '用户退赛信息'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_quit_match_info';
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

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_quit_match_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_quit_match_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fmatch_id
                 ,t1.fuid
                 ,t1.fparty_type
                 ,t1.fcause            --退赛原因
                 ,t1.fjoin_flag        --报名标识
                 ,t1.fparty_flag       --参赛标识
                 ,t1.fquit_flag        --退赛标识
            from dim.match_user t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s' and fquit_flag = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct case when fcause in (1,2) then fuid end) quit_unum                                              --取消报名人数
                 ,count(distinct case when fcause in (1,2) then concat_ws('0', fmatch_id, cast (fuid as string)) end) fquit_cnt  --取消报名人次
                 ,count(distinct case when fcause = 1 then fuid end) fz_quit_unum                                                --主动取消报名人数
                 ,count(distinct case when fcause = 1 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fz_quit_cnt     --主动取消报名人次
                 ,count(distinct case when fcause = 2 then fuid end) fb_quit_unum                                                --被动取消报名人数
                 ,count(distinct case when fcause = 2 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fb_quit_cnt     --被动取消报名人次
                 ,count(distinct case when fcause in (3,4) then fuid end) fout_unum                                              --退赛人数
                 ,count(distinct case when fcause in (3,4) then concat_ws('0', fmatch_id, cast (fuid as string)) end) fout_cnt   --退赛人次
                 ,count(distinct case when fcause = 3 then fuid end) fz_out_unum                                                 --主动退赛人数
                 ,count(distinct case when fcause = 3 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fz_out_cnt      --主动退赛人次
                 ,count(distinct case when fcause = 4 then fuid end) fb_out_unum                                                 --被动退赛人数
                 ,count(distinct case when fcause = 4 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fb_out_cnt      --被动退赛人次
                 ,count(distinct case when fcause = 5 then fuid end) fdieout_unum                                                --淘汰人数
                 ,count(distinct case when fcause = 5 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fdieout_cnt     --淘汰人次
            from work.bud_user_quit_match_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_quit_match_info
                      partition(dt='%(statdate)s')
                      %(sql_template)s; """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_quit_match_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_quit_match_info(sys.argv[1:])
a()
