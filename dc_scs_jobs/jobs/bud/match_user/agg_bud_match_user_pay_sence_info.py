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


class agg_bud_match_user_pay_sence_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_pay_sence_stg (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               hallmode            smallint,
               forder_id           varchar(255)     comment '订单id',
               fplatform_uid       varchar(50)      comment '平台id',
               fuid                bigint           comment '用户id',
               fbankrupt           tinyint          comment '是否破产',
               fpay_scene          string           comment '付费场景',
               fsubname            string           comment '二级场次',
               fgsubname           string           comment '三级场次',
               fpay_scene_type     string           comment '场景类型',
               faward_type         string           comment '奖励类型',
               fparty_type         string           comment '牌局类型',
               fpay_scene_extra    string           comment '场景补充字段',
               fmatch_rule_type    string           comment '赛制类型',
               fitem_category      string           comment '物品类型',
               fincome             decimal(38,9)    comment '订单金额',
               is_sus              int              comment '是否成功付费',
               is_first            int              comment '是否首次付费'
               )comment '地方棋牌订单明细'
        partitioned by(dt date)
        location '/dw/bud_dm/bud_user_pay_sence_stg';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        create table if not exists bud_dm.bud_match_pay_user_stg (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               hallmode            smallint,
               fuid                bigint            comment '用户id',
               fgsubname           varchar(100)      comment '三级场次',
               fparty_type         varchar(100)      comment '牌局类型',
               fitem_id            varchar(100)      comment '报名物品id',
               fjoin_flag          int               comment '是否报名',
               fparty_flag         int               comment '是否参赛',
               faward_type         varchar(100)      comment '奖励类型',
               fmatch_rule_id      varchar(100)      comment '赛制id'
               )comment '地方棋牌比赛付费用户明细'
        partitioned by(dt date)
        location '/dw/bud_dm/bud_match_pay_user_stg';
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
        hql = """--地方棋牌订单明细
          insert overwrite table bud_dm.bud_user_pay_sence_stg partition(dt = '%(statdate)s')
          select /*+ MAPJOIN(tt) */
                 '%(statdate)s' fdate
                 ,tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.forder_id
                 ,t1.fplatform_uid
                 ,t1.fuid
                 ,t2.fbankrupt
                 ,coalesce(t2.fpay_scene,'%(null_str_report)s') fpay_scene
                 ,coalesce(t2.fgameparty_subname,'%(null_str_report)s')  fsubname
                 ,coalesce(t2.fgameparty_gsubname,'%(null_str_report)s')  fgsubname
                 ,case when coalesce(t2.fpay_scene_type, '0') = '0' then '%(null_str_report)s' else t2.fpay_scene_type end fpay_scene_type
                 ,coalesce(t2.faward_type,'%(null_str_report)s')  faward_type
                 ,coalesce(t2.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t2.fpay_scene_extra,'%(null_str_report)s')  fpay_scene_extra
                 ,coalesce(t2.fmatch_rule_type,'%(null_str_report)s')  fmatch_rule_type
                 ,case when t1.fproduct_name like '%%%%金条%%%%' then '金条'
                    when t1.fproduct_name like '%%%%银币%%%%' or t1.fproduct_name like '%%%%金币%%%%' or t1.fproduct_name like '%%%%游戏币%%%%' then '游戏币'
                    when t1.fproduct_name like '%%%%VIP%%%%' or t1.fproduct_name like '%%%%vip%%%%' then 'VIP'
                    else '其他' end fitem_category
                 ,coalesce(t3.fcoins_num * t3.frate,0) fincome
                 ,case when t3.forder_id is not null then 1 else 0 end is_sus
                 ,case when t4.fplatform_uid is not null then 1 else 0 end is_first
            from stage.payment_stream_all_stg t1  --所有订单
            left join stage.user_generate_order_stg t2
              on t1.forder_id = t2.forder_id
             and t2.dt = '%(statdate)s'
            left join stage.payment_stream_stg t3  --成功订单
              on t1.forder_id = t3.forder_id
             and t3.dt = '%(statdate)s'
            left join dim.user_pay t4  --首付
              on t3.fbpid = t4.fbpid
             and t3.fplatform_uid = t4.fplatform_uid
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
             and fgamefsk = 4132314431
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--地方棋牌订单明细
            insert overwrite table bud_dm.bud_match_pay_user_stg partition (dt = '%(statdate)s' )
            select /*+ MAPJOIN(tt) */
                   '%(statdate)s' fdate,
                   tt.fgamefsk,
                   tt.fplatformfsk,
                   tt.fhallfsk,
                   coalesce(a.fgame_id,%(null_int_report)d) fsubgamefsk,
                   tt.fterminaltypefsk,
                   tt.fversionfsk,
                   tt.hallmode,
                   a.fuid,
                   coalesce(b.fsubname,'%(null_str_report)s') fgsubname,
                   coalesce(b.fparty_type,'%(null_str_report)s') fparty_type,
                   fitem_id,
                   max(fjoin_flag) fjoin_flag,
                   max(fparty_flag) fparty_flag,
                   coalesce(b.faward_type,'%(null_str_report)s') faward_type,
                   coalesce(b.fmatch_rule_id,'%(null_str_report)s') match_rule_id
              from (
                  --报名参加牌局比赛
                    select jg.fbpid,
                           coalesce(jg.fgame_id,cast (0 as bigint)) fgame_id,
                           jg.fuid,
                           concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id,
                           jg.fitem_id,
                           1 fjoin_flag,
                           0 fparty_flag,
                           dt
                      from stage.join_gameparty_stg jg
                     where jg.dt = '%(statdate)s'
                       and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                     union all
                  --参赛
                    select ug.fbpid,
                           coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                           ug.fuid,
                           concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id,
                           null fitem_id,
                           0 fjoin_flag,
                           1 fparty_flag,
                           dt
                      from stage.user_gameparty_stg ug
                     where ug.dt = '%(statdate)s'
                       and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                   ) a
              left join (select distinct concat_ws('_', cast (coalesce(fmatch_cfg_id,0) as string), cast (coalesce(fmatch_log_id,0) as string)) fmatch_id, fpname, fname, fsubname, fparty_type, faward_type,fmatch_rule_id
                           from stage.join_gameparty_stg jg
                          where jg.dt = '%(statdate)s'
                            and (coalesce(fmatch_cfg_id,0)<>0 or coalesce(fmatch_log_id,0)<>0)
                        ) b
                on a.fmatch_id = b.fmatch_id
              join (select distinct dt,fuid
                      from stage.payment_stream_stg ttt
                      join dim.bpid_map_bud tt
                        on ttt.fbpid = tt.fbpid
                       and fgamefsk = 4132314431
                     where dt = '%(statdate)s') ttt
                on ttt.fuid = a.fuid
              join dim.bpid_map_bud tt
                on a.fbpid = tt.fbpid
               and fgamefsk = 4132314431
             group by tt.fgamefsk,
                      tt.fplatformfsk,
                      tt.fhallfsk,
                      a.fgame_id,
                      tt.fterminaltypefsk,
                      tt.fversionfsk,
                      tt.hallmode,
                      a.fuid,
                      coalesce(b.fsubname,'%(null_str_report)s'),
                      coalesce(b.fparty_type,'%(null_str_report)s'),
                      coalesce(b.faward_type,'%(null_str_report)s'),
                      coalesce(b.fmatch_rule_id,'%(null_str_report)s'),
                      fitem_id;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = agg_bud_match_user_pay_sence_info(sys.argv[1:])
a()
