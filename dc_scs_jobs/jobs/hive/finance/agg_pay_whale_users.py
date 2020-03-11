#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_pay_whale_users(BaseStatModel):
    """BUD后台菜单路径：游戏运营分析\付费分析\鲸鱼用户\Top100玩家"""
    def create_tab(self):
        hql = """
            create table if not exists dcnew.pay_whale_users
            (
              fdate string comment '日期（同步PG用）',
              fgamefsk bigint comment '游戏ID',
              fgamename string comment '游戏名称',
              fplatformfsk bigint comment '平台ID',
              fplatformname string comment '平台名称',
              fhallfsk bigint comment '大厅ID',
              fsubgamefsk bigint comment '子游戏ID（此字段无内容，预留为了兼容BUD版本选择器）',
              fterminaltypefsk bigint comment '终端ID',
              fversionfsk bigint comment '应用包ID',
              fchannel_code bigint comment '渠道ID（此字段无内容，预留为了兼容BUD版本选择器）',
              fuid bigint comment '用户ID',
              fplatform_uid string comment '游戏平台UID/用户站点ID',
              pay_count int comment '付费次数',
              pay_sum_usd decimal(12,2) comment '付费额度（单位：美元）',
              login_count int comment '登录次数',
              play_innings int comment '玩牌局数',
              gamecoin_obtain bigint comment '发放游戏币',
              gamecoin_expend bigint comment '消耗游戏币',
              latest_login_time string comment '最后登录时间',
              timestamp_and_gamecoin string comment '时间戳 | 游戏币结余'
            )
            comment '鲸鱼用户信息表'
            partitioned by (dt string)
            stored as textfile
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        hql = """
            --付费信息
            with pay_info as
             (select fbpid,
                     fuid,
                     fplatform_uid,
                     count(distinct forder_id) as pay_count,
                     sum(case
                           when coalesce(fpamount_usd,0) = 0 then
                            coalesce(fcoins_num,0) * coalesce(frate,0)
                           else
                            fpamount_usd
                         end) as pay_sum_usd
                from stage.payment_stream_stg
               where dt = '%(statdate)s'
               group by fbpid, fuid, fplatform_uid),
            
            --登录信息
            login_info as
             (select fbpid,
                     fuid,
                     count(distinct flogin_at) as login_count,
                     max(flogin_at) as latest_login_time
                from stage.user_login_stg
               where dt = '%(statdate)s'
               group by fbpid, fuid),
            
            --牌局信息
            gameparty_info as
             (select fbpid, fuid, count(distinct finning_id) as play_innings
                from stage.user_gameparty_stg
               where dt = '%(statdate)s'
               group by fbpid, fuid),
            
            --游戏币信息
            gamecoin_info as
             (select fbpid,
                     fuid,
                     sum(case
                           when act_type = 1 then
                            abs(act_num)
                           else
                            0
                         end) as gamecoin_obtain,
                     sum(case
                           when act_type = 2 then
                            abs(act_num)
                           else
                            0
                         end) as gamecoin_expend,
                     max(concat(cast(unix_timestamp(lts_at) as string),
                                '|',
                                cast(user_gamecoins_num as string))) as timestamp_and_gamecoin
                from stage.pb_gamecoins_stream_stg
               where dt = '%(statdate)s'
               group by fbpid, fuid),
            
            --目标用户汇总
            target_users as
             (select fbpid,fuid from pay_info
              union distinct
              select fbpid,fuid from login_info
              union distinct
              select fbpid,fuid from gameparty_info
              union distinct
              select fbpid,fuid from gamecoin_info)
            
            --数据写入到表
            insert overwrite table dcnew.pay_whale_users partition(dt='%(statdate)s')
            
            --非大厅模式游戏用户
            select '%(statdate)s' as fdate,
                   t5.fgamefsk,
                   t5.fgamename,
                   t5.fplatformfsk,
                   t5.fplatformname,
                   %(null_int_group_rule)d as fhallfsk,
                   %(null_int_group_rule)d as fsubgamefsk,
                   coalesce(t5.fterminaltypefsk,%(null_int_group_rule)d) as fterminaltypefsk,
                   coalesce(t5.fversionfsk,%(null_int_group_rule)d) as fversionfsk,
                   %(null_int_group_rule)d as fchannel_code,
                   coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid) as fuid,
                   t1.fplatform_uid,
                   sum(t1.pay_count) as pay_count,
                   sum(t1.pay_sum_usd) as pay_sum_usd,
                   sum(t2.login_count) as login_count,
                   sum(t3.play_innings) as play_innings,
                   sum(t4.gamecoin_obtain) as gamecoin_obtain,
                   sum(t4.gamecoin_expend) as gamecoin_expend,
                   max(t2.latest_login_time) as latest_login_time,
                   max(t4.timestamp_and_gamecoin) as timestamp_and_gamecoin
              from target_users t0
              left join pay_info t1
                on t0.fbpid = t1.fbpid and t0.fuid = t1.fuid
              left join login_info t2
                on t0.fbpid = t2.fbpid and t0.fuid = t2.fuid
              left join gameparty_info t3
                on t0.fbpid = t3.fbpid and t0.fuid = t3.fuid
              left join gamecoin_info t4
                on t0.fbpid = t4.fbpid and t0.fuid = t4.fuid
              left join dim.bpid_map t5
                on t0.fbpid = t5.fbpid
             where t5.hallmode = 0    --hallmode等于0
             group by t5.fgamefsk,
                      t5.fgamename,
                      t5.fplatformfsk,
                      t5.fplatformname,
                      t5.fterminaltypefsk,
                      t5.fversionfsk,
                      coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid),
                      t1.fplatform_uid
            grouping sets((t5.fgamefsk, t5.fgamename, t5.fplatformfsk, t5.fplatformname, coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid), t1.fplatform_uid),
                          (t5.fgamefsk, t5.fgamename, t5.fplatformfsk, t5.fplatformname, t5.fterminaltypefsk, t5.fversionfsk, coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid), t1.fplatform_uid))
            
            --大厅模式游戏用户
            union
            select '%(statdate)s' as fdate,
                   t5.fgamefsk,
                   t5.fgamename,
                   t5.fplatformfsk,
                   t5.fplatformname,
                   coalesce(t5.fhallfsk,%(null_int_group_rule)d) as fhallfsk,
                   %(null_int_group_rule)d as fsubgamefsk,
                   coalesce(t5.fterminaltypefsk,%(null_int_group_rule)d) as fterminaltypefsk,
                   coalesce(t5.fversionfsk,%(null_int_group_rule)d) as fversionfsk,
                   %(null_int_group_rule)d as fchannel_code,
                   coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid) as fuid,
                   t1.fplatform_uid,
                   sum(t1.pay_count) as pay_count,
                   sum(t1.pay_sum_usd) as pay_sum_usd,
                   sum(t2.login_count) as login_count,
                   sum(t3.play_innings) as play_innings,
                   sum(t4.gamecoin_obtain) as gamecoin_obtain,
                   sum(t4.gamecoin_expend) as gamecoin_expend,
                   max(t2.latest_login_time) as latest_login_time,
                   max(t4.timestamp_and_gamecoin) as timestamp_and_gamecoin
              from target_users t0
              left join pay_info t1
                on t0.fbpid = t1.fbpid and t0.fuid = t1.fuid
              left join login_info t2
                on t0.fbpid = t2.fbpid and t0.fuid = t2.fuid
              left join gameparty_info t3
                on t0.fbpid = t3.fbpid and t0.fuid = t3.fuid
              left join gamecoin_info t4
                on t0.fbpid = t4.fbpid and t0.fuid = t4.fuid
              left join dim.bpid_map t5
                on t0.fbpid = t5.fbpid
             where t5.hallmode = 1    --hallmode等于1
             group by t5.fgamefsk,
                      t5.fgamename,
                      t5.fplatformfsk,
                      t5.fplatformname,
                      t5.fhallfsk,
                      t5.fterminaltypefsk,
                      t5.fversionfsk,
                      coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid),
                      t1.fplatform_uid
            grouping sets((t5.fgamefsk, t5.fgamename, t5.fplatformfsk, t5.fplatformname, coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid), t1.fplatform_uid),
                          (t5.fgamefsk, t5.fgamename, t5.fplatformfsk, t5.fplatformname, t5.fhallfsk, coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid), t1.fplatform_uid),
                          (t5.fgamefsk, t5.fgamename, t5.fplatformfsk, t5.fplatformname, t5.fhallfsk, t5.fterminaltypefsk, t5.fversionfsk, coalesce(t1.fuid,t2.fuid,t3.fuid,t4.fuid), t1.fplatform_uid))
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = agg_pay_whale_users(sys.argv[1:])
a()