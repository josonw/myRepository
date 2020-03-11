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


class agg_dfqp_user_portrait_last_1_mid(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """

insert overwrite table veda.dfqp_user_portrait_last_1_mid
partition(dt = '%(statdate)s')
select 
	distinct 
	x.fuid mid, 
	null hallname, 
	reg_signup_at, 
	reg_channel_code, 
	reg_m_dtype, 
	reg_m_pixel, 
	reg_m_imei, 
	reg_ip, 
	reg_ip_country, 
	reg_ip_province, 
	reg_ip_city, 
	q.fvip_level, 
	q.fvip_type, 
	q.fdue_at, 
	a.user_gender, 
	null user_constellation, 
	null user_birthday, 
	a.user_phone, 
	a.user_imsi, 
	a.user_imei, 
	a.user_email, 
	a.login_first_at, 
	a.login_last_2nd_at, 
	a.login_last_at, 
	a.login_last_country, 
	a.login_last_province, 
	a.login_last_city, 
	r.flogin_cnt, 
	case when nvl(r.flogin_cnt, 0) > 0 then 1 else 0 end login_days, 
	a.act_first_at, 
	a.act_last_2nd_at, 
	a.act_last_at, 
	case when nvl(r.flogin_cnt, 0) > 0 then 1 else 0 end act_days,
	null act_days_max, 
	null act_days_recently, 
	a.pay_first_at, 
	a.pay_last_2nd_at, 
	a.pay_last_at, 
	a.pay_usd_last,
	t.pay_cnt, 
	t.pay_total, 
	t.pay_max,  
	a.play_first_at, 
	a.play_last_2nd_at, 
	a.play_last_at, 
	v.play_num, 
	v.play_time, 
	case when nvl(v.play_num, 0) > 0 then 1 else 0 end, 
	v.play_win_num, 
	v.play_lose_num, 
	v.play_num_gamecoins, 
	v.play_time_gamecoins, 
	v.play_win_num_gamecoins, 
	v.play_lose_num_gamecoins, 
	v.play_charge_gamecoins, 
	v.play_win_gamecoins + v.play_lose_gamecoins play_gamecoins, 
	v.play_win_gamecoins, 
	v.play_lose_gamecoins, 
	v.play_win_gamecoins_max, 
	v.play_lose_gamecoins_max, 
	v.play_num_gold, 
	v.play_time_gold, 
	v.play_win_num_gold, 
	v.play_lose_num_gold, 
	v.play_charge_gold, 
	v.play_win_gold + v.play_lose_gold play_gold, 
	v.play_win_gold, 
	v.play_lose_gold, 
	v.play_win_gold_max, 
	v.play_lose_gold_max, 
	v.play_num_integral, 
	v.play_time_integral, 
	v.play_win_num_integral, 
	v.play_lose_num_integral, 
	v.play_charge_integral, 
	v.play_win_integral + v.play_lose_integral play_integral, 
	v.play_win_integral, 
	v.play_lose_integral, 
	v.play_win_integral_max, 
	v.play_lose_integral_max, 
	a.play_last_pname, 
	a.play_last_subname, 
	a.play_last_gsubname, 
	a.play_last_gamecoins, 
	w.join_cnt, 
	w.join_amt, 
	w.max_join_amt, 
	w.match_cnt, 
	w.win_cnt, 
	w.win_amt, 
	w.max_win_amt, 
	a.match_first_at, 
	a.match_last_2nd_at, 
	a.match_last_at, 
	a.match_last_pname, 
	a.match_last_subname, 
	a.match_last_gsubname, 
	null bankruptcy_first_at, 
	a.bankruptcy_last_at, 
	u.frupt_cnt, 
	case when nvl(u.frupt_cnt, 0) > 0 then 1 else 0 end, 
	null relieve_first_at, 
	a.relieve_last_at, 
	u.frlv_cnt, 
	case when nvl(u.frlv_cnt, 0) > 0 then 1 else 0 end, 
	u.frlv_gamecoins, 
	x.user_gamecoins_num, 
	nvl(p.fcurrencies_num, 0) fcurrencies_num, 
	null item_num, 
	nvl(z.fbank_gamecoins_num, 0) fbank_gamecoins_num, 
	nvl(c.fbank_currencies_num, 0) fbank_currencies_num, 
	a.user_latitude, 
	a.user_longitude, 
	x.user_gamecoins_num + nvl(z.fbank_gamecoins_num, 0) total_gamecoins, 
	nvl(p.fcurrencies_num, 0) + nvl(c.fbank_currencies_num, 0) total_gold, 
	b.fbpid,
	b.fgamefsk,
	b.fgamename,
	b.fplatformfsk,
	b.fplatformname,
	b.fhallfsk,
	b.fhallname,
	b.fterminaltypefsk,
	b.fterminaltypename,
	b.fversionfsk,
	b.fversionname, 
	a.login_last_ip, 
	a.login_last_imei, 
	a.login_last_dtype, 
	a.login_last_pixel, 
	a.login_last_version, 
	a.login_last_os, 
	a.login_last_network, 
	a.login_last_operator, 
	a.user_signup_at, 
	a.user_display_name, 
	a.user_idcard, 
	a.user_version, 
	a.user_status, 
	a.user_simulator_flag, 
	a.user_sign_excption_flag,
	a.user_wave_flag, 
	a.user_active_excption_flag, 
	a.user_mutil_account_flag, 
	a.user_identity, 
	a.cid, 
	a.user_superior_agent, 
	a.user_superior_promoter 
from 
(select x1.fuid, max(x1.user_gamecoins_num) user_gamecoins_num from 
(select fuid, fdate, user_gamecoins_num from dim.user_gamecoin_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s') x1 
join 
(select fuid, max(fdate) fdate from dim.user_gamecoin_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' group by fuid) x2 
on x1.fuid = x2.fuid and x1.fdate = x2.fdate group by x1.fuid) x 
left join 
(select p1.fuid, max(p1.fcurrencies_num) fcurrencies_num from 
(select fuid, fdate, fcurrencies_num from dim.user_currencies_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' and fcurrencies_type = '11') p1 
join 
(select fuid, max(fdate) fdate from dim.user_currencies_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' and fcurrencies_type = '11' group by fuid) p2 
on p1.fuid = p2.fuid and p1.fdate = p2.fdate group by p1.fuid) p on x.fuid = p.fuid
left join 
(select z1.fuid, max(z1.fbank_gamecoins_num) fbank_gamecoins_num from 
(select fuid, fdate, fbank_gamecoins_num from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' and fcurrencies_type = '0') z1 
join 
(select fuid, max(fdate) fdate from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' and fcurrencies_type = '0' group by fuid, fcurrencies_type) z2 
on z1.fuid = z2.fuid and z1.fdate = z2.fdate group by z1.fuid) z on x.fuid = z.fuid
left join 
(select c1.fuid, max(c1.fbank_gamecoins_num) fbank_currencies_num from 
(select fuid, fdate, fbank_gamecoins_num from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' and fcurrencies_type = '11') c1 
join 
(select fuid, max(fdate) fdate from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s' and fcurrencies_type = '11' group by fuid, fcurrencies_type) c2 
on c1.fuid = c2.fuid and c1.fdate = c2.fdate group by c1.fuid) c on x.fuid = c.fuid
left join 
(select q2.fuid, q2.fdue_at, q2.fvip_type, q2.fvip_level from 
(select fuid, max(fvip_at) fvip_at from stage.user_vip_stg where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s'group by fuid) q1
join
(select fuid, fvip_at, fdue_at, fvip_type, fvip_level from stage.user_vip_stg 
where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) and dt = '%(statdate)s') q2 on q1.fuid = q2.fuid and q1.fvip_at = q2.fvip_at) q 
on x.fuid = q.fuid
left join (select fuid, sum(flogin_cnt) flogin_cnt from dim.user_act where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) r on x.fuid = r.fuid 
left join (select fuid, sum(fcoins_num) pay_total, count(1) pay_cnt, max(fcoins_num) pay_max from stage.payment_stream_stg where dt = '%(statdate)s'
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) t 
on x.fuid = t.fuid 
left join (select fuid, sum(frupt_cnt) frupt_cnt, sum(frlv_cnt) frlv_cnt, sum(frlv_gamecoins) frlv_gamecoins from dim.user_bankrupt_relieve where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) u 
on x.fuid = u.fuid
left join (select fuid, 
	sum(fparty_num) play_num, 
	sum(fplay_time) play_time, 
	sum(fwin_party_num) play_win_num, 
	sum(flose_party_num) play_lose_num, 
	sum(case when fcoins_type = 1 then fparty_num else 0 end) play_num_gamecoins, 
	sum(case when fcoins_type = 1 then fplay_time else 0 end) play_time_gamecoins, 
	sum(case when fcoins_type = 1 then fwin_party_num else 0 end) play_win_num_gamecoins, 
	sum(case when fcoins_type = 1 then flose_party_num else 0 end) play_lose_num_gamecoins, 
	sum(case when fcoins_type = 1 then fcharge else 0 end) play_charge_gamecoins, 
	sum(case when fcoins_type = 1 then fwin_amt else 0 end) play_win_gamecoins, 
	sum(case when fcoins_type = 1 then flose_amt else 0 end) play_lose_gamecoins, 
	max(case when fcoins_type = 1 then fmax_win_amt else 0 end) play_win_gamecoins_max, 
	min(case when fcoins_type = 1 then -fmax_lose_amt else 0 end) play_lose_gamecoins_max, 
	sum(case when fcoins_type = 11 then fparty_num else 0 end) play_num_gold, 
	sum(case when fcoins_type = 11 then fplay_time else 0 end) play_time_gold, 
	sum(case when fcoins_type = 11 then fwin_party_num else 0 end) play_win_num_gold, 
	sum(case when fcoins_type = 11 then flose_party_num else 0 end) play_lose_num_gold, 
	sum(case when fcoins_type = 11 then fcharge else 0 end) play_charge_gold, 
	sum(case when fcoins_type = 11 then fwin_amt else 0 end) play_win_gold, 
	sum(case when fcoins_type = 11 then flose_amt else 0 end) play_lose_gold, 
	max(case when fcoins_type = 11 then fmax_win_amt else 0 end) play_win_gold_max, 
	min(case when fcoins_type = 11 then -fmax_lose_amt else 0 end) play_lose_gold_max, 
	sum(case when fcoins_type = 3 then fparty_num else 0 end) play_num_integral, 
	sum(case when fcoins_type = 3 then fplay_time else 0 end) play_time_integral, 
	sum(case when fcoins_type = 3 then fwin_party_num else 0 end) play_win_num_integral, 
	sum(case when fcoins_type = 3 then flose_party_num else 0 end) play_lose_num_integral, 
	sum(case when fcoins_type = 3 then fcharge else 0 end) play_charge_integral, 
	sum(case when fcoins_type = 3 then fwin_amt else 0 end) play_win_integral, 
	sum(case when fcoins_type = 3 then flose_amt else 0 end) play_lose_integral, 
	max(case when fcoins_type = 3 then fmax_win_amt else 0 end) play_win_integral_max, 
	min(case when fcoins_type = 3 then -fmax_lose_amt else 0 end) play_lose_integral_max 
from dim.user_gameparty_nomatch where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) v on x.fuid = v.fuid 
left join (select fuid, sum(join_amt) join_amt, max(max_join_amt) max_join_amt, sum(join_cnt) join_cnt, sum(match_cnt) match_cnt, 
sum(win_amt) win_amt, sum(win_cnt) win_cnt, max(max_win_amt) max_win_amt from dim.match_user_wide_using where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431 and fhallfsk <> 8000657) group by fuid) w 
on x.fuid = w.fuid 
left join (select uid, reg_signup_at, reg_channel_code, reg_m_dtype, reg_m_pixel, reg_m_imei, reg_ip, reg_ip_country, reg_ip_province, reg_ip_city,user_gender, user_phone, 
user_imei, user_imsi, user_email, login_last_2nd_at, login_last_at, login_last_country, login_last_province, 
login_last_city, act_last_2nd_at, act_last_at, pay_last_2nd_at, pay_last_at, pay_usd_last, bankruptcy_last_at, relieve_last_at, play_last_2nd_at, play_last_at, 
play_pgame_last play_last_pname, play_subgame_last play_last_subname, play_gsubgame_last play_last_gsubname, play_gamecoin_last_num play_last_gamecoins, match_last_2nd_at, match_last_at, 
match_pgame_last match_last_pname, match_subgame_last match_last_subname, match_gsubgame_last match_last_gsubname, 
act_first_at, play_first_at, match_first_at, login_first_at, pay_first_at, user_latitude, user_longitude,
login_last_ip, login_last_imei, login_last_dtype, login_last_pixel, login_last_version, login_last_os, login_last_network, login_last_operator, 
user_signup_at, user_display_name, user_idcard, user_version, user_status, user_simulator_flag, 
user_sign_excption_flag,user_wave_flag, user_active_excption_flag, user_mutil_account_flag, user_identity, cid, user_superior_agent, user_superior_promoter	
from veda.tmp_veda_user_info_hbase where gamefsk = 4132314431) a on x.fuid = a.uid 
left join 
(select b1.fuid, b1.fbpid, fgamefsk, fgamename, fplatformfsk, fplatformname, fhallfsk, fhallname, fterminaltypefsk, fterminaltypename, fversionfsk, fversionname 
from stage.user_signup_stg b1 
join dim.bpid_map b2 on b1.fbpid = b2.fbpid 
where b2.fgamefsk = 4132314431 and fhallfsk <> 8000657 and b1.dt = '%(statdate)s') b on x.fuid = b.fuid 
where nvl(x.fuid, 0) > 0;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_user_portrait_last_1_mid(sys.argv[1:])
a()
