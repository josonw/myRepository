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


class agg_dfqp_fzb_match_suspect_info(BaseStatModel):
    def create_tab(self):

        hql = """--防作弊比赛嫌疑用户
        create table if not exists veda.dfqp_fzb_match_suspect_info (
         fdate               date,
         fgamefsk            bigint,
         fplatformfsk        bigint,
         fhallfsk            bigint,
         fuid                bigint          comment '用户ID',
         fmatch_num          bigint          comment '参加比赛次数',
         fmatch_num_rank     bigint          comment '参加比赛次数排名',
         fmatch_num_flag     bigint          comment '参加比赛次数标志位',
         faward_rate         decimal(6,2)    comment '得奖率',
         faward_rate_rank    bigint          comment '得奖率排名',
         faward_rate_flag    bigint          comment '得奖率标志位',
         faward_num          decimal(10,2)   comment '得奖金额',
         faward_num_rank     bigint          comment '得奖金额排名',
         faward_num_flag     bigint          comment '得奖金额标志位',
         fm_pay_income       decimal(20,2)   comment '近一个月付费',
         fm_pay_income_rank  bigint          comment '近一个月付费排名',
         fm_pay_income_flag  bigint          comment '近一个月付费标志位',
         faward_avg          decimal(10,2)   comment '场均获利',
         faward_avg_rank     bigint          comment '场均获利排名',
         faward_avg_flag     bigint          comment '场均获利标志位',
         fwin_rate           decimal(6,2)    comment '比赛牌局胜率',
         fwin_rate_rank      bigint          comment '比赛牌局胜率排名',
         fwin_rate_flag      bigint          comment '比赛牌局胜率标志位',
         fsuspect_mark       decimal(6,2)    comment '嫌疑用户六项平均排名数',
         fsuspect_rank       bigint          comment '嫌疑用户六项排名',
         fsuspect_flag       bigint          comment '嫌疑用户六项出现项数'
        )comment '防作弊比赛嫌疑用户'
        partitioned by(dt date)
        location '/dw/veda/dfqp_fzb_match_suspect_info';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--取基础指标
        drop table if exists work.dfqp_fzb_match_suspect_info_tmp_1_%(statdatenum)s;
        create table work.dfqp_fzb_match_suspect_info_tmp_1_%(statdatenum)s as
          select  fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fuid
                 ,sum(fmatch_num) fmatch_num
                 ,case when sum(fmatch_num) = 0 then 0 else sum(faward_cnt)/sum(fmatch_num) end faward_rate
                 ,sum(fcost) faward_num
                 ,sum(ftotal_usd_amt) fm_pay_income
                 ,case when sum(fmatch_num) = 0 then 0 else sum(fcost)/sum(fmatch_num) end faward_avg
                 ,case when sum(fparty_num) = 0 then 0 else sum(fwin_party_num)/sum(fparty_num) end fwin_rate
            from (
                 select  tt.fgamefsk
                        ,tt.fplatformfsk
                        ,tt.fhallfsk
                        ,t1.fuid
                        ,0 fmatch_num
                        ,0 fparty_num
                        ,0 fwin_party_num
                        ,sum(ftotal_usd_amt) ftotal_usd_amt
                        ,0 fcost
                        ,0 faward_cnt
                   from dim.user_pay_day t1
                   join dim.bpid_map_bud tt
                     on t1.fbpid = tt.fbpid
                    and tt.fgamefsk = 4132314431
                  where t1.dt between '%(ld_30day_ago)s' and '%(statdate)s'
                  group by  tt.fgamefsk, tt.fplatformfsk, tt.fhallfsk, t1.fuid
                  union all
                 select  tt.fgamefsk
                        ,tt.fplatformfsk
                        ,tt.fhallfsk
                        ,t1.fuid
                        ,count(distinct fmatch_id) fmatch_num
                        ,count(1) fparty_num
                        ,count(case when fgamecoins > 0 then 1 end) fwin_party_num
                        ,0 ftotal_usd_amt
                        ,0 fcost
                        ,0 faward_cnt
                   from dim.match_gameparty t1
                   join dim.bpid_map_bud tt
                     on t1.fbpid = tt.fbpid
                    and tt.fgamefsk = 4132314431
                    and t1.fgame_id = 203
                  where t1.dt = '%(statdate)s'
                  group by  tt.fgamefsk, tt.fplatformfsk, tt.fhallfsk, t1.fuid
                  union all
                 select  tt.fgamefsk
                        ,tt.fplatformfsk
                        ,tt.fhallfsk
                        ,t1.fuid
                        ,0 fmatch_num
                        ,0 fparty_num
                        ,0 fwin_party_num
                        ,0 ftotal_usd_amt
                        ,sum(case when fio_type = 1 then fcost end) fcost
                        ,count(distinct case when frank > 0 and frank <= fwin_num then t2.fmatch_id end) faward_cnt
                   from stage.match_economy_stg t1
                   left join dim.match_config t2
                     on concat_ws('_', cast (t1.fmatch_cfg_id as string), cast (t1.fmatch_log_id as string)) = t2.fmatch_id
                    and t2.dt = '%(statdate)s'
                   join dim.bpid_map_bud tt
                     on t1.fbpid = tt.fbpid
                    and tt.fgamefsk = 4132314431
                  where t1.dt = '%(statdate)s'
                    and t1.fio_type = 1
                    and t1.fgame_id = 203
                  group by  tt.fgamefsk, tt.fplatformfsk, tt.fhallfsk, t1.fuid) t
           group by fgamefsk, fplatformfsk, fhallfsk, fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--排名
        insert overwrite table veda.dfqp_fzb_match_suspect_info partition(dt='%(statdate)s')
select '%(statdate)s' fdate
       ,fgamefsk
       ,fplatformfsk
       ,fhallfsk
       ,fuid
       ,fmatch_num
       ,fmatch_num_rank
       ,fmatch_num_flag
       ,faward_rate
       ,faward_rate_rank
       ,faward_rate_flag
       ,faward_num
       ,faward_num_rank
       ,faward_num_flag
       ,fm_pay_income
       ,fm_pay_income_rank
       ,fm_pay_income_flag
       ,faward_avg
       ,faward_avg_rank
       ,faward_avg_flag
       ,fwin_rate
       ,fwin_rate_rank
       ,fwin_rate_flag
       ,fsuspect_mark
       ,row_number() over(partition by fgamefsk order by fsuspect_flag desc, fsuspect_mark, fuid) fsuspect_rank
       ,fsuspect_flag
  from (select fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fuid
               ,fmatch_num
               ,fmatch_num_rank
               ,fmatch_num_flag
               ,faward_rate
               ,faward_rate_rank
               ,faward_rate_flag
               ,faward_num
               ,faward_num_rank
               ,faward_num_flag
               ,fm_pay_income
               ,fm_pay_income_rank
               ,fm_pay_income_flag
               ,faward_avg
               ,faward_avg_rank
               ,faward_avg_flag
               ,fwin_rate
               ,fwin_rate_rank
               ,fwin_rate_flag
               ,(fmatch_num_rank + faward_rate_rank + faward_num_rank + fm_pay_income_rank + faward_avg_rank + fwin_rate_rank)
                 /(fmatch_num_flag + faward_rate_flag + faward_num_flag + fm_pay_income_flag + faward_avg_flag + fwin_rate_flag) fsuspect_mark
               ,(fmatch_num_flag + faward_rate_flag + faward_num_flag + fm_pay_income_flag + faward_avg_flag + fwin_rate_flag) fsuspect_flag
          from (select fgamefsk
                       ,fplatformfsk
                       ,fhallfsk
                       ,fuid
                       ,fmatch_num
                       ,case when fmatch_num_rank > 500 then 0 else fmatch_num_rank end fmatch_num_rank
                       ,case when fmatch_num_rank > 500 then 0 else 1 end fmatch_num_flag
                       ,faward_rate
                       ,case when faward_rate_rank > 500 then 0 else faward_rate_rank end faward_rate_rank
                       ,case when faward_rate_rank > 500 then 0 else 1 end faward_rate_flag
                       ,faward_num
                       ,case when faward_num_rank > 500 then 0 else faward_num_rank end faward_num_rank
                       ,case when faward_num_rank > 500 then 0 else 1 end faward_num_flag
                       ,fm_pay_income
                       ,case when fm_pay_income_rank > 500 then 0 else fm_pay_income_rank end fm_pay_income_rank
                       ,case when fm_pay_income_rank > 500 then 0 else 1 end fm_pay_income_flag
                       ,faward_avg
                       ,case when faward_avg_rank > 500 then 0 else faward_avg_rank end faward_avg_rank
                       ,case when faward_avg_rank > 500 then 0 else 1 end faward_avg_flag
                       ,fwin_rate
                       ,case when fwin_rate_rank > 500 then 0 else fwin_rate_rank end fwin_rate_rank
                       ,case when fwin_rate_rank > 500 then 0 else 1 end fwin_rate_flag
                  from (select  fgamefsk
                               ,fplatformfsk
                               ,fhallfsk
                               ,fuid
                               ,fmatch_num
                               ,row_number() over(partition by fgamefsk order by fmatch_num desc, fuid) fmatch_num_rank
                               ,faward_rate
                               ,row_number() over(partition by fgamefsk order by faward_rate desc, fuid) faward_rate_rank
                               ,faward_num
                               ,row_number() over(partition by fgamefsk order by faward_num desc, fuid) faward_num_rank
                               ,fm_pay_income
                               ,row_number() over(partition by fgamefsk order by fm_pay_income desc, fuid) fm_pay_income_rank
                               ,faward_avg
                               ,row_number() over(partition by fgamefsk order by faward_avg desc, fuid) faward_avg_rank
                               ,fwin_rate
                               ,row_number() over(partition by fgamefsk order by fwin_rate desc, fuid) fwin_rate_rank
                          from work.dfqp_fzb_match_suspect_info_tmp_1_%(statdatenum)s
                       ) t
                 where fmatch_num_rank <= 500
                    or faward_rate_rank <= 500
                    or faward_num_rank <= 500
                    or fm_pay_income_rank <= 500
                    or faward_avg_rank <= 500
                    or fwin_rate_rank <= 500 ) t ) t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.dfqp_fzb_match_suspect_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = agg_dfqp_fzb_match_suspect_info(sys.argv[1:])
a()
