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


class agg_fzb_relieve_user_tag(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
        	insert overwrite table veda.dfqp_fzb_relieve_user_info_day 
        	partition(dt='%(statdate)s')
			select 
				mid, 
				sum(coalesce(gold_play_time, 0)) gold_play_time, 
				sum(coalesce(total_play_time, 0)) total_play_time, 
				sum(coalesce(gold_play_time, 0))/sum(coalesce(total_play_time, 0)) gold_rate, 
				sum(coalesce(100_play_amt, 0)) 100_play_amt, 
				sum(coalesce(is_100_lose, 0)) is_100_lose, 
				sum(coalesce(pay_sum, 0.00)) pay_sum, 
				sum(coalesce(relieve_count, 0)) relieve_count 
			from 
			(select mid, gold_play_time, total_play_time, 100_play_amt, is_100_lose, pay_sum, relieve_count, rank() over(partition by mid order by dt desc) row_num 
			from veda.dfqp_fzb_relieve_user_tag_day where dt <= '%(statdate)s') m where row_num <= 7 group by mid having count(1) = 7;

			insert overwrite table veda.dfqp_fzb_relieve_user_tag 
			partition(dt='%(statdate)s')
			select mid, 1 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 14 and relieve_count <= 21 and (100_play_amt < -1000000 or is_100_lose >= 2)
			union all 
			select mid, 2 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 19 and relieve_count <= 21 and (100_play_amt >= -1000000 and 100_play_amt < 0 and is_100_lose < 2) and total_play_time <= 50000 and gold_rate >= 0.9
			union all 
			select mid, 3 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 19 and relieve_count <= 21 and (100_play_amt >= -1000000 and 100_play_amt < 0 and is_100_lose < 2) and total_play_time > 50000 and gold_rate >= 0.9
			union all
			select mid, 4 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 14 and relieve_count <= 18 and (100_play_amt >= -1000000 and 100_play_amt < 0 and is_100_lose < 2) and pay_sum = 0.00 and gold_rate >= 0.9
			union all
			select mid, 5 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 10 and relieve_count <= 13 and pay_sum = 0.00 and gold_rate >= 0.9
			union all
			select mid, 6 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 14 and relieve_count <= 21 and (100_play_amt >= -1000000 and 100_play_amt < 0 and is_100_lose < 2) and gold_rate < 0.9
			union all 
			select mid, 7 tag from veda.dfqp_fzb_relieve_user_info_day where dt = '%(statdate)s' and relieve_count >= 10 and relieve_count <= 13 and gold_rate < 0.9;
			
			insert overwrite table veda.dfqp_fzb_relieve_user_tag_stat 
			select mid, tag, count(1) from veda.dfqp_fzb_relieve_user_tag group by mid, tag; 
			
			insert overwrite table veda.dfqp_fzb_relieve_user_tag_final 
			partition(dt='%(statdate)s')
			select 
				z.mid, 
				case when coalesce(x.tag, 0) <> coalesce(y.tag, 0) then coalesce(y.tag, 0) else coalesce(x.tag, 0) end tag, 
				case when coalesce(x.tag, 0) <> coalesce(y.tag, 0) then 1 else 0 end is_change 
			from 
		 	(select mid from veda.dfqp_user_portrait where fhallname not in ('香港大厅', '台湾大厅')) z
		 	left join 
			(select mid, tag from veda.dfqp_fzb_relieve_user_tag_final where dt = date_add('%(statdate)s', -1)) x on z.mid = x.mid
			left join 
			(select mid, tag from veda.dfqp_fzb_relieve_user_tag where dt = '%(statdate)s') y on z.mid = y.mid;
			
			insert overwrite table veda.dfqp_fzb_relieve_user_tag_changed 
			partition(dt='%(statdate)s')
			select mid, tag, dt from veda.dfqp_fzb_relieve_user_tag_final where is_change = 1 and dt = '%(statdate)s';
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_relieve_user_tag(sys.argv[1:])
a()
