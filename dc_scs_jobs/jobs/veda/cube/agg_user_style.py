#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class UserCoinSource(BaseStatModel):
    def create_tab(self):
        hql = """
        	create table if not exists veda.user_style_daily_sirendoudizhu
        	(
        		fbpid string ,
        		fuid bigint ,
        		sys_sum bigint comment '系统发放总量',
        		sys_percent double comment '系统发放占比（百分比）',
        		win_sum bigint comment '赢牌总量',
        		win_percent double comment '赢牌占比',
        		pay_sum bigint comment '付费总量',
        		pay_percent bigint comment '付费占比'
        		change_sum bigint comment '兑换总量',
        		change_percent double comment '兑换占比',
        		coin_total bigint comment '当日总共的游戏币获取数量',
        		primary_total_time bigint comment '初级场玩牌时长',
        		primary_total_count bigint comment '初级场玩牌局数',
        		primary_percent double comment '初级场玩牌时长占比',
        		practice_total_time bigint comment '练习场玩牌时长',
        		practice_total_count bigint comment '练习场玩牌局数',
        		practice_percent double comment '练习场玩牌时长占比',
        		native_total_time bigint comment '新手场玩牌时长',
        		native_total_count bigint comment '新手场玩牌局数',
        		native_percent double comment '新手场玩牌时长占比',
        		middle_total_time bigint comment '中级场玩牌时长',
        		middle_total_count bigint comment '中级场玩牌局数',
        		middle_percent double comment '中级场玩牌时长占比',
        		high_total_time bigint comment '高级场玩牌时长',
        		high_total_count bigint comment '高级场玩牌局数',
        		high_percent double comment '高级场玩牌时长占比',
        		master_total_time bigint comment '大师场玩牌时长',
        		master_total_count bigint comment '大师场玩牌局数',
        		master_percent double comment '大师场玩牌时长占比'
        	)
        	comment '四人斗地主用户风格表'
        	partitioned by (dt string comment '日期')
        	stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

     def stat(self):
     	HQL_coin_source = """
			WITH b AS
			  (SELECT fbpid
			   FROM dim.bpid_map
			   WHERE fgamename = '四人斗地主'),
			     dim_coin AS
			  (SELECT ftype,
			          ftypename
			   FROM analysis.game_coin_type_dim
			   WHERE fdirection = 'IN'
			     AND fgamefsk = 1396896 ),
			     a1 AS
			  (SELECT DISTINCT a.fbpid,
			                   a.fuid,
			                   dim_coin.ftypename AS fbigtype,
			                   sum(if(a.act_num > 0, act_num, 0)) over (partition BY a.fbpid,a.fuid,dim_coin.ftypename,a.dt) AS coin_sum,
			                   sum(if(a.act_num > 0, act_num, 0)) over (partition BY a.fbpid,a.fuid,a.dt) AS coin_total_sum,
			                   a.dt
			   FROM stage.pb_gamecoins_stream a
			   LEFT JOIN b ON a.fbpid = b.fbpid
			   LEFT JOIN dim_coin ON a.act_id = dim_coin.ftype
			   WHERE a.act_type = 1 ),
			     b1 AS
			  (SELECT fgameparty_name,
			          fdis_name
			   FROM dcnew.gameparty_name_dim
			   WHERE fgamefsk = 1396896 ),
			     b2 AS
			  (SELECT DISTINCT c.fbpid AS fbpid,
			                   c.fuid AS fuid,
			                   b1.fdis_name AS pname,
			                   sum(CASE
			                           WHEN unix_timestamp(c.fe_timer) - unix_timestamp(c.fs_timer) BETWEEN 1 AND 10800 THEN unix_timestamp(c.fe_timer) - unix_timestamp(c.fs_timer)
			                           ELSE 0
			                       END) over(partition BY c.fbpid,c.fuid) AS play_duration,
			                   sum(CASE
			                           WHEN unix_timestamp(c.fe_timer) - unix_timestamp(c.fs_timer) BETWEEN 1 AND 10800 THEN unix_timestamp(c.fe_timer) - unix_timestamp(c.fs_timer)
			                           ELSE 0
			                       END) over(partition BY c.fbpid,c.fuid,c.fsubname,c.dt) AS play_duration_subname,
			                   sum(c.fgamecoins) over(partition BY c.fbpid,c.fuid,c.dt) AS pname_coins_sum,
			                   sum(c.fgamecoins) over(partition BY c.fbpid,c.fuid,c.fsubname,c.dt) AS pname_coins,
			                   count(1) over(partition BY c.fbpid,c.fuid) AS pname_count_sum,
			                   count(1) over(partition BY c.fbpid,c.fuid,c.fsubname) AS pname_count,
			                   c.dt
			   FROM stage.user_gameparty c
			   LEFT JOIN b ON c.fbpid = b.fbpid
			   LEFT JOIN b1 ON c.fsubname = b1.fgameparty_name
			   WHERE c.fgamecoins >= 0 ),
			     b3 AS
			  (SELECT fbpid,
			          fuid,
			          pname,
			          play_duration_subname,
			          pname_count,
			          cast(play_duration_subname/play_duration AS decimal(20,4)) AS pname_percent,
			          dt
			   FROM b2),
			     b4 AS
			  (SELECT b3.fbpid,
			          b3.fuid,
			          max(CASE
			                  WHEN b3.pname = '初级场' THEN b3.play_duration_subname
			                  ELSE 0
			              END) AS primary_total_time,
			          max(CASE
			                  WHEN b3.pname = '初级场' THEN b3.pname_count
			                  ELSE 0
			              END) AS primary_total_count,
			          max(CASE
			                  WHEN b3.pname = '初级场' THEN b3.pname_percent
			                  ELSE 0
			              END) AS primary_percent,
			          max(CASE
			                  WHEN b3.pname = '练习场' THEN b3.play_duration_subname
			                  ELSE 0
			              END) AS practice_total_time,
			          max(CASE
			                  WHEN b3.pname = '练习场' THEN b3.pname_count
			                  ELSE 0
			              END) AS practice_total_count,
			          max(CASE
			                  WHEN b3.pname = '练习场' THEN b3.pname_percent
			                  ELSE 0
			              END) AS practice_percent,
			          max(CASE
			                  WHEN b3.pname = '新手场' THEN b3.play_duration_subname
			                  ELSE 0
			              END) AS native_total_time,
			          max(CASE
			                  WHEN b3.pname = '新手场' THEN b3.pname_count
			                  ELSE 0
			              END) AS native_total_count,
			          max(CASE
			                  WHEN b3.pname = '新手场' THEN b3.pname_percent
			                  ELSE 0
			              END) AS native_percent,
			          max(CASE
			                  WHEN b3.pname = '中级场' THEN b3.play_duration_subname
			                  ELSE 0
			              END) AS middle_total_time,
			          max(CASE
			                  WHEN b3.pname = '中级场' THEN b3.pname_count
			                  ELSE 0
			              END) AS middle_total_count,
			          max(CASE
			                  WHEN b3.pname = '中级场' THEN b3.pname_percent
			                  ELSE 0
			              END) AS middle_percent,
			          max(CASE
			                  WHEN b3.pname = '高级场' THEN b3.play_duration_subname
			                  ELSE 0
			              END) AS high_total_time,
			          max(CASE
			                  WHEN b3.pname = '高级场' THEN b3.pname_count
			                  ELSE 0
			              END) AS high_total_count,
			          max(CASE
			                  WHEN b3.pname = '高级场' THEN b3.pname_percent
			                  ELSE 0
			              END) AS high_percent,
			          max(CASE
			                  WHEN b3.pname = '大师场' THEN b3.play_duration_subname
			                  ELSE 0
			              END) AS master_total_time,
			          max(CASE
			                  WHEN b3.pname = '大师场' THEN b3.pname_count
			                  ELSE 0
			              END) AS master_total_count,
			          max(CASE
			                  WHEN b3.pname = '大师场' THEN b3.pname_percent
			                  ELSE 0
			              END) AS master_percent,
			          dt
			   FROM b3
			   GROUP BY fbpid,
			            fuid,
			            dt),
			     c1 AS
			  (SELECT a1.fbpid,
			          a1.fuid,
			          a1.fbigtype,
			          a1.coin_sum,
			          b2.pname_coins_sum,
			          a1.coin_total_sum+b2.pname_coins_sum AS coin_sum_total,
			          a1.dt
			   FROM a1
			   JOIN b2 ON a1.fbpid = b2.fbpid
			   AND a1.fuid = b2.fuid
			   AND a1.dt = b2.dt),
			     c2 AS
			  (SELECT fbpid,
			          fuid,
			          fbigtype,
			          coin_sum,
			          pname_coins_sum,
			          cast(coin_sum*100/coin_sum_total AS decimal(20,4)) AS coin_percent_1,
			          cast(pname_coins_sum*100/coin_sum_total AS decimal(20,4)) AS coin_percent_2,
			          coin_sum_total,
			          dt
			   FROM c1),
			     c3 AS
			  (SELECT fbpid,
			          fuid,
			          max(CASE
			                  WHEN fbigtype ='系统' THEN coin_sum
			                  ELSE 0
			              END) AS sys_sum,
			          max(CASE
			                  WHEN fbigtype ='系统' THEN coin_percent_1
			                  ELSE 0
			              END) AS sys_percent,
			          max(CASE
			                  WHEN fbigtype ='支付' THEN coin_sum
			                  ELSE 0
			              END) AS pay_sum,
			          max(CASE
			                  WHEN fbigtype ='支付' THEN coin_percent_1
			                  ELSE 0
			              END) AS pay_percent,
			          max(CASE
			                  WHEN fbigtype ='兑换' THEN coin_sum
			                  ELSE 0
			              END) AS change_sum,
			          max(CASE
			                  WHEN fbigtype ='兑换' THEN coin_percent_1
			                  ELSE 0
			              END) AS change_percent,
			          pname_coins_sum AS win_sum,
			          coin_percent_2 AS win_percent,
			          coin_sum_total AS coin_total,
			          dt
			   FROM c2
			   GROUP BY fbpid,
			            fuid,
			            pname_coins_sum,
			            coin_percent_2,
			            coin_sum_total,
			            dt)
			insert overwrite table veda.user_style_daily_sirendoudizhu partition (dt = '%(statdate)s')
			SELECT c3.*,
			       b4.primary_total_time,
			       b4.primary_total_count,
			       b4.primary_percent,
			       b4.practice_total_time,
			       b4.practice_total_count,
			       b4.practice_percent,
			       b4.native_total_time,
			       b4.native_total_count,
			       b4.native_percent,
			       b4.middle_total_time,
			       b4.middle_total_count,
			       b4.middle_percent,
			       b4.high_total_time,
			       b4.high_total_count,
			       b4.high_percent,
			       b4.master_total_time,
			       b4.master_total_count,
			       b4.master_percent
			FROM c3
			LEFT JOIN b4 ON c3.fbpid = b4.fbpid
			AND c3.fuid = b4.fuid
			AND c3.dt = b4.dt
			
     	"""
     	res = self.sql_exe(HQL_coin_source)
        if res != 0:
            return res

        
        


# 实例化执行
a = UserCoinSource(sys.argv[1:])
a()