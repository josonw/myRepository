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


class agg_bud_user_match_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fparty_type           varchar(10)     comment '牌局类型',
               fapply_unum           bigint          comment '报名人数',
               fapply_cnt            bigint          comment '报名人次',
               ffirst_apply_unum     bigint          comment '首次报名人数',
               fnew_apply_unum       bigint          comment '新增用户报名人数',
               fapply_fr_unum        bigint          comment '无偿报名人数',
               fapply_co_unum        bigint          comment '有偿报名人数',
               ffirst_apply_fr_unum  bigint          comment '首次无偿报名人数',
               ffirst_apply_co_unum  bigint          comment '首次有偿报名人数',
               fmatch_unum           bigint          comment '参赛人数',
               fmatch_cnt            bigint          comment '参赛人次',
               ffirst_match_unum     bigint          comment '首次参赛人数',
               fnew_match_unum       bigint          comment '新增用户参赛人数',
               fmatch_fr_unum        bigint          comment '无偿参赛人数',
               fmatch_co_unum        bigint          comment '有偿参赛人数',
               fwin_unum             bigint          comment '获奖人数',
               fwin_num              bigint          comment '获奖人次',
               fbegin_cnt            bigint          comment '开赛次数',
               fbegin_suss_cnt       bigint          comment '开赛成功次数',
               fbegin_fail_cnt       bigint          comment '开赛失败次数',
               fjoin_suss_cnt        bigint          comment '报名开赛成功次数'
               )comment '用户赛事信息'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_info';
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
            drop table if exists work.bud_user_match_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_info_tmp_1_%(statdatenum)s as
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
                 ,t1.fentry_fee        --报名费
                 ,t1.fcause            --退赛原因
                 ,t1.fio_type          --发放消耗
                 ,t1.ffirst_apply      --是否首次报名
                 ,t1.fnew_apply        --是否注册用户报名
                 ,t1.ffirst_party      --是否首次参赛
                 ,t1.fnew_party        --是否注册用户参赛
                 ,t1.fitem_num         --物品数量
                 ,t1.fcost             --物品成本RMB
                 ,t1.fjoin_flag        --报名标识
                 ,t1.fparty_flag       --参赛标识
                 ,t1.fquit_flag        --退赛标识
                 ,max(fjoin_flag) over(partition by fmatch_id) fjoin
                 ,max(fparty_flag) over(partition by fmatch_id) fparty
            from dim.match_user t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
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
                 ,fparty_type                                --牌局类型(汇总)
                 ,count(distinct case when fjoin_flag = 1 then fuid end) fapply_unum           --报名人数
                 ,count(case when fjoin_flag = 1 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fapply_cnt            --报名人次
                 ,count(distinct case when fjoin_flag = 1 and ffirst_apply = 1 then fuid end) ffirst_apply_unum     --首次报名人数
                 ,count(distinct case when fjoin_flag = 1 and fnew_apply = 1 then fuid end) fnew_apply_unum       --新增用户报名人数
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee = 0 then fuid end) fapply_fr_unum        --无偿报名人数
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee > 0 then fuid end) fapply_co_unum        --有偿报名人数
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee = 0 and ffirst_apply = 1 then fuid end) ffirst_apply_fr_unum  --首次无偿报名人数
                 ,count(distinct case when fjoin_flag = 1 and fentry_fee > 0 and ffirst_apply = 1 then fuid end) ffirst_apply_co_unum  --首次有偿报名人数
                 ,count(distinct case when fparty_flag = 1 then fuid end) fmatch_unum           --参赛人数
                 ,count(distinct case when fparty_flag = 1 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fmatch_cnt            --参赛人次
                 ,count(distinct case when fparty_flag = 1 and ffirst_party = 1 then fuid end) ffirst_match_unum     --首次参赛人数
                 ,count(distinct case when fparty_flag = 1 and fnew_party = 1 then fuid end) fnew_match_unum       --新增用户参赛人数
                 ,count(distinct case when fparty_flag = 1 and fentry_fee = 0 then fuid end) fmatch_fr_unum        --无偿参赛人数
                 ,count(distinct case when fparty_flag = 1 and fentry_fee > 0 then fuid end) fmatch_co_unum        --有偿参赛人数
                 ,count(distinct case when fio_type = 1 then fuid end) fwin_unum             --获奖人数
                 ,count(distinct case when fio_type = 1 then concat_ws('0', fmatch_id, cast (fuid as string)) end) fwin_num          --获奖人次
                 ,count(distinct case when fjoin_flag = 1 then fmatch_id end) fbegin_cnt                                             --开赛次数
                 ,count(distinct case when fparty_flag = 1 then fmatch_id end) fbegin_suss_cnt                                       --开赛成功次数
                 ,count(distinct case when fjoin_flag = 1 then fmatch_id end)-count(distinct case when fparty_flag = 1 then fmatch_id end) fbegin_fail_cnt             --开赛失败次数
                 ,count(distinct case when fjoin_flag = 1 and fparty = 1 then fmatch_id end) fjoin_suss_cnt        --报名开赛成功次数
            from work.bud_user_match_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type
         """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_match_info
                      partition(dt='%(statdate)s')
                      %(sql_template)s """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_info(sys.argv[1:])
a()
