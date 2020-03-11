#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_by_deferred_goods_balance(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 上个月1号
        create table if not exists analysis.by_deferred_goods_balance_fct
        (
            fdate                           date,
            fgamefsk                        bigint,
            fplatformfsk                    bigint,
            fpurple_unused_nums             bigint,
            fpurple_num_coin                bigint,
            fblue_unused_nums               bigint,
            fblue_num_coin                  bigint,
            fred_unused_nums                bigint,
            fred_num_coin                   bigint,
            fyellow_unused_nums             bigint,
            fyellow_num_coin                bigint,
            fpurple_living_times            bigint,
            fpurple_time_coin               bigint,
            fblue_living_times              bigint,
            fblue_time_coin                 bigint,
            fred_living_times               bigint,
            fred_time_coin                  bigint,
            fyellow_living_times            bigint,
            fyellow_time_coin               bigint,
            fpurple_using_nums              bigint,
            fblue_using_nums                bigint,
            fred_using_nums                 bigint,
            fyellow_using_nums              bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """

        hql_list = []

        # sum(case when ftpl_id=10 then funused_cnt end) "紫钻未使用数",
        # sum(case when ftpl_id=12 then funused_cnt end) "蓝钻未使用数",
        # sum(case when ftpl_id=13 then funused_cnt end) "红钻未使用数",
        # sum(case when ftpl_id=15 then funused_cnt end) "黄钻未使用数",
        # sum(case when ftpl_id=10 then fused_living_cnt end) "紫钻剩余次数",
        # sum(case when ftpl_id=12 then fused_living_cnt end) "蓝钻剩余次数",
        # sum(case when ftpl_id=13 then fused_living_cnt end) "红钻剩余次数",
        # sum(case when ftpl_id=15 then fused_living_cnt end) "黄钻剩余次数"
        hql = """
        insert overwrite table analysis.by_deferred_goods_balance_fct partition
        (dt = "%(ld_1month_ago_begin)s")
        -- 递延收入，道具
        select '%(ld_1month_ago_begin)s' fdate, fgamefsk, fplatformfsk,
               max(case when ftpl_id=10 then funused_cnt end) fpurple_unused_nums,
               0 fpurple_num_coin,
               max(case when ftpl_id=12 then funused_cnt end) fblue_unused_nums,
               0 fblue_num_coin,
               max(case when ftpl_id=13 then funused_cnt end) fred_unused_nums,
               0 fred_num_coin,
               max(case when ftpl_id=15 then funused_cnt end) fyellow_unused_nums,
               0 fyellow_num_coin,
               max(case when ftpl_id=10 then fused_living_cnt end) fpurple_living_times,
               0 fpurple_time_coin,
               max(case when ftpl_id=12 then fused_living_cnt end) fblue_living_times,
               0 fblue_time_coin,
               max(case when ftpl_id=13 then fused_living_cnt end) fred_living_times,
               0 fred_time_coin,
               max(case when ftpl_id=15 then fused_living_cnt end) fyellow_living_times,
               0 fyellow_time_coin,
               max(case when ftpl_id=10 then fused_cnt end) fpurple_using_nums,
               max(case when ftpl_id=12 then fused_cnt end) fblue_using_nums,
               max(case when ftpl_id=13 then fused_cnt end) fred_using_nums,
               max(case when ftpl_id=15 then fused_cnt end) fyellow_using_nums
        from stage.goods_balance_stg a
        join dim.bpid_map b
        on a.fbpid=b.fbpid
        where dt >='%(ld_month_begin)s' and dt < date_add('%(ld_month_begin)s', 1)
        group by fgamefsk,fplatformfsk
        order by fgamefsk,fplatformfsk

        """ % self.hql_dict
        hql_list.append( hql )

        # 只有月初日期才会执行
        if self.hql_dict.get('stat_date', '').endswith('-01'):
            result = self.exe_hql_list(hql_list)
        else:
            result = ''
        return result



if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = agg_by_deferred_goods_balance(stat_date)
    a()
