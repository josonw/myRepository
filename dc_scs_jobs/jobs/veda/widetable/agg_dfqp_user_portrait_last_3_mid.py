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


class agg_dfqp_user_portrait_last_3_mid(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
        
insert overwrite table veda.dfqp_user_portrait_last_3_mid
partition(dt = '%(statdate)s')
select 
	x.fuid, 
	case when nvl(t.pay_total, 0) > 0 then 1 else 0 end last_3_is_pay, 
	case when nvl(w.match_cnt, 0) > 0 then 1 else 0 end last_3_is_match, 
	t.fpay_cnt last_3_pay_cnt,  
	t.pay_total last_3_pay_total, 
	t.pay_max last_3_pay_max, 
	v.last_3_play_num, 
	v.last_3_play_time, 
	v.last_3_play_win_num, 
	v.last_3_play_lose_num, 
	v.last_3_play_num_gamecoins, 
	v.last_3_play_time_gamecoins, 
	v.last_3_play_win_num_gamecoins, 
	v.last_3_play_lose_num_gamecoins, 
	v.last_3_play_charge_gamecoins, 
	v.last_3_play_win_gamecoins + v.last_3_play_lose_gamecoins last_3_play_gamecoins, 
	v.last_3_play_win_gamecoins, 
	v.last_3_play_lose_gamecoins, 
	v.last_3_play_win_gamecoins_max, 
	v.last_3_play_lose_gamecoins_max, 
	v.last_3_play_num_gold, 
	v.last_3_play_time_gold, 
	v.last_3_play_win_num_gold, 
	v.last_3_play_lose_num_gold, 
	v.last_3_play_charge_gold, 
	v.last_3_play_win_gold + v.last_3_play_lose_gold last_3_play_gold, 
	v.last_3_play_win_gold, 
	v.last_3_play_lose_gold, 
	v.last_3_play_win_gold_max, 
	v.last_3_play_lose_gold_max, 
	v.last_3_play_num_integral, 
	v.last_3_play_time_integral, 
	v.last_3_play_win_num_integral, 
	v.last_3_play_lose_num_integral, 
	v.last_3_play_charge_integral, 
	v.last_3_play_win_integral + v.last_3_play_lose_integral last_3_play_integral, 
	v.last_3_play_win_integral, 
	v.last_3_play_lose_integral, 
	v.last_3_play_win_integral_max, 
	v.last_3_play_lose_integral_max, 
	w.join_cnt last_3_match_join_cnt, 
	w.join_amt last_3_match_join_fee, 
	w.max_join_amt last_3_match_join_fee_max, 
	w.match_cnt last_3_match_cnt, 
	w.win_cnt last_3_match_win_cnt, 
	w.win_amt last_3_match_income_total, 
	w.max_win_amt last_3_match_income_max, 
	u.rupt_days, 
	u.frupt_cnt last_3_rupt_cnt, 
	b.rlv_days, 
	b.frlv_cnt last_3_rlv_cnt, 
	b.frlv_gamecoins, 
	r.login_days, 
	r.flogin_cnt last_3_login_cnt,
	v.play_days, 
	case when v.last_3_play_num > 0 then 1 else 0 end is_play 
from 
(select x1.fuid, max(x1.user_gamecoins_num) user_gamecoins_num from 
(select fuid, fdate, user_gamecoins_num from dim.user_gamecoin_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s') x1 
join 
(select fuid, max(fdate) fdate from dim.user_gamecoin_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' group by fuid) x2 
on x1.fuid = x2.fuid and x1.fdate = x2.fdate group by x1.fuid) x 
left join (select fuid, sum(flogin_cnt) flogin_cnt, count(distinct dt) login_days from dim.user_act where dt >= date_add('%(statdate)s', -2) and dt <= '%(statdate)s'
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and flogin_cnt > 0 group by fuid) r on x.fuid = r.fuid 
left join (select fuid, sum(fcoins_num) pay_total, count(1) fpay_cnt, max(fcoins_num) pay_max from stage.payment_stream_stg where dt >= date_add('%(statdate)s', -2) and dt <= '%(statdate)s'
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) t 
on x.fuid = t.fuid 
left join (select fuid, sum(frupt_cnt) frupt_cnt, count(distinct dt) rupt_days 
from dim.user_bankrupt_relieve where dt >= date_add('%(statdate)s', -2) and dt <= '%(statdate)s' and frupt_cnt > 0 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) u 
on x.fuid = u.fuid 

left join (select fuid, count(distinct dt) rlv_days, sum(frlv_cnt) frlv_cnt, sum(frlv_gamecoins) frlv_gamecoins 
from dim.user_bankrupt_relieve where dt >= date_add('%(statdate)s', -2) and dt <= '%(statdate)s' and frlv_cnt > 0 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) b 
on x.fuid = b.fuid 
left join 
(select fuid, 
	sum(fparty_num) last_3_play_num, 
	sum(fplay_time) last_3_play_time, 
	1 last_3_play_day, 
	sum(fwin_party_num) last_3_play_win_num, 
	sum(flose_party_num) last_3_play_lose_num, 
	sum(case when fcoins_type = 1 then fparty_num else 0 end) last_3_play_num_gamecoins, 
	sum(case when fcoins_type = 1 then fplay_time else 0 end) last_3_play_time_gamecoins, 
	sum(case when fcoins_type = 1 then fwin_party_num else 0 end) last_3_play_win_num_gamecoins, 
	sum(case when fcoins_type = 1 then flose_party_num else 0 end) last_3_play_lose_num_gamecoins, 
	sum(case when fcoins_type = 1 then fcharge else 0 end) last_3_play_charge_gamecoins, 
	sum(case when fcoins_type = 1 then fwin_amt else 0 end) last_3_play_win_gamecoins, 
	sum(case when fcoins_type = 1 then flose_amt else 0 end) last_3_play_lose_gamecoins, 
	max(case when fcoins_type = 1 then fmax_win_amt else 0 end) last_3_play_win_gamecoins_max, 
	min(case when fcoins_type = 1 then -fmax_lose_amt else 0 end) last_3_play_lose_gamecoins_max, 
	sum(case when fcoins_type = 11 then fparty_num else 0 end) last_3_play_num_gold, 
	sum(case when fcoins_type = 11 then fplay_time else 0 end) last_3_play_time_gold, 
	sum(case when fcoins_type = 11 then fwin_party_num else 0 end) last_3_play_win_num_gold, 
	sum(case when fcoins_type = 11 then flose_party_num else 0 end) last_3_play_lose_num_gold, 
	sum(case when fcoins_type = 11 then fcharge else 0 end) last_3_play_charge_gold, 
	sum(case when fcoins_type = 11 then fwin_amt else 0 end) last_3_play_win_gold, 
	sum(case when fcoins_type = 11 then flose_amt else 0 end) last_3_play_lose_gold, 
	max(case when fcoins_type = 11 then fmax_win_amt else 0 end) last_3_play_win_gold_max, 
	min(case when fcoins_type = 11 then -fmax_lose_amt else 0 end) last_3_play_lose_gold_max, 
	sum(case when fcoins_type = 3 then fparty_num else 0 end) last_3_play_num_integral, 
	sum(case when fcoins_type = 3 then fplay_time else 0 end) last_3_play_time_integral, 
	sum(case when fcoins_type = 3 then fwin_party_num else 0 end) last_3_play_win_num_integral, 
	sum(case when fcoins_type = 3 then flose_party_num else 0 end) last_3_play_lose_num_integral, 
	sum(case when fcoins_type = 3 then fcharge else 0 end) last_3_play_charge_integral, 
	sum(case when fcoins_type = 3 then fwin_amt else 0 end) last_3_play_win_integral, 
	sum(case when fcoins_type = 3 then flose_amt else 0 end) last_3_play_lose_integral, 
	max(case when fcoins_type = 3 then fmax_win_amt else 0 end) last_3_play_win_integral_max, 
	min(case when fcoins_type = 3 then -fmax_lose_amt else 0 end) last_3_play_lose_integral_max, 
	count(distinct dt) play_days  
from dim.user_gameparty_nomatch where dt >= date_add('%(statdate)s', -2) and dt <= '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) v on x.fuid = v.fuid 
left join (select fuid, sum(join_amt) join_amt, max(max_join_amt) max_join_amt, sum(join_cnt) join_cnt, sum(match_cnt) match_cnt, 
sum(win_amt) win_amt, sum(win_cnt) win_cnt, max(max_win_amt) max_win_amt from dim.match_user_wide_using where dt >= date_add('%(statdate)s', -2) and dt <= '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) w 
on x.fuid = w.fuid 
join 
(select fuid from dim.user_act where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) a on x.fuid = a.fuid ;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_user_portrait_last_3_mid(sys.argv[1:])
a()
