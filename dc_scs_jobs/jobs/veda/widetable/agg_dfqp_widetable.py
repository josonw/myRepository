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


class agg_dfqp_widetable(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """insert overwrite table veda.dfqp_user_wide_table_last select * from veda.dfqp_user_wide_table;
        
        
insert overwrite table veda.dfqp_user_wide_table_mid
select 
	x.fuid, 
	'%(statdate)s', 
	reg_signup_at, 
	reg_channel_code, 
	reg_m_dtype, 
	reg_m_pixel, 
	reg_m_imei, 
	reg_ip, 
	reg_ip_country, 
	reg_ip_province, 
	reg_ip_city, 
	a.user_gender, 
	'', 
	'', 
	q.fvip_level, 
	q.fvip_type, 
	q.fdue_at, 
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
	0, 
	0, 
	case when nvl(r.flogin_cnt, 0) > 0 then 1 else 0 end, 
	a.act_first_at, 
	a.act_last_2nd_at, 
	a.act_last_at, 
	case when nvl(r.flogin_cnt, 0) > 0 then 1 else 0 end, 
	0, 
	0, 
	a.pay_first_at, 
	a.pay_last_2nd_at, 
	a.pay_last_at, 
	t.fmax_usd_amt, 
	a.pay_usd_last, 
	t.ftotal_usd_amt, 
	t.fpay_cnt, 
	u.frupt_cnt, 
	a.bankruptcy_last_at, 
	case when nvl(u.frupt_cnt, 0) > 0 then 1 else 0 end, 
	u.frlv_cnt, 
	a.relieve_last_at, 
	case when nvl(u.frlv_cnt, 0) > 0 then 1 else 0 end, 
	u.frlv_gamecoins, 
	a.play_first_at, 
	a.play_last_2nd_at, 
	a.play_last_at, 
	v.fparty_num, 
	v.fplay_time, 
	case when nvl(v.fparty_num, 0) > 0 then 1 else 0 end, 
	v.fwin_party_num, 
	v.flose_party_num, 
	v.fcharge, 
	v.fwin_amt, 
	v.flose_amt, 
	a.play_pgame_last, 
	a.play_subgame_last, 
	a.play_gsubgame_last, 
	a.play_gamecoin_last_num, 
	v.fmax_win_amt, 
	v.fmax_lose_amt, 
	w.join_amt, 
	w.max_join_amt, 
	w.join_cnt, 
	w.match_cnt, 
	w.win_cnt, 
	w.win_amt, 
	w.max_win_amt, 
	a.match_first_at, 
	a.match_last_2nd_at, 
	a.match_last_at, 
	a.match_pgame_last, 
	a.match_subgame_last, 
	a.match_gsubgame_last, 
	0, 
	0, 
	x.user_gamecoins_num, 
	p.fcurrencies_num, 
	0, 
	z.fbank_gamecoins_num, 
	c.fbank_currencies_num 
from 
(select x1.fuid, max(x1.user_gamecoins_num) user_gamecoins_num from 
(select fuid, fdate, user_gamecoins_num from dim.user_gamecoin_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s') x1 
join 
(select fuid, max(fdate) fdate from dim.user_gamecoin_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' group by fuid) x2 
on x1.fuid = x2.fuid and x1.fdate = x2.fdate group by x1.fuid) x 
left join 
(select p1.fuid, max(p1.fcurrencies_num) fcurrencies_num from 
(select fuid, fdate, fcurrencies_num from dim.user_currencies_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' and fcurrencies_type = '11') p1 
join 
(select fuid, max(fdate) fdate from dim.user_currencies_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' and fcurrencies_type = '11' group by fuid) p2 
on p1.fuid = p2.fuid and p1.fdate = p2.fdate group by p1.fuid) p on x.fuid = p.fuid
left join 
(select z1.fuid, max(z1.fbank_gamecoins_num) fbank_gamecoins_num from 
(select fuid, fdate, fbank_gamecoins_num from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' and fcurrencies_type = '0') z1 
join 
(select fuid, max(fdate) fdate from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' and fcurrencies_type = '0' group by fuid, fcurrencies_type) z2 
on z1.fuid = z2.fuid and z1.fdate = z2.fdate group by z1.fuid) z on x.fuid = z.fuid
left join 
(select c1.fuid, max(c1.fbank_gamecoins_num) fbank_currencies_num from 
(select fuid, fdate, fbank_gamecoins_num from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' and fcurrencies_type = '11') c1 
join 
(select fuid, max(fdate) fdate from dim.user_bank_balance where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s' and fcurrencies_type = '11' group by fuid, fcurrencies_type) c2 
on c1.fuid = c2.fuid and c1.fdate = c2.fdate group by c1.fuid) c on x.fuid = c.fuid
left join 
(select q2.fuid, q2.fdue_at, q2.fvip_type, q2.fvip_level from 
(select fuid, max(fvip_at) fvip_at from stage.user_vip_stg where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s'group by fuid) q1
join
(select fuid, fvip_at, fdue_at, fvip_type, fvip_level from stage.user_vip_stg 
where fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) and dt = '%(statdate)s') q2 on q1.fuid = q2.fuid and q1.fvip_at = q2.fvip_at) q 
on x.fuid = q.fuid
left join (select fuid, sum(flogin_cnt) flogin_cnt from dim.user_act where dt = '%(statdate)s' group by fuid) r on x.fuid = r.fuid 
left join (select fuid, sum(ftotal_usd_amt) ftotal_usd_amt, sum(fpay_cnt) fpay_cnt, max(fmax_usd_amt) fmax_usd_amt from dim.user_pay_day where dt = '%(statdate)s'
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) group by fuid) t 
on x.fuid = t.fuid 
left join (select fuid, sum(frupt_cnt) frupt_cnt, sum(frlv_cnt) frlv_cnt, sum(frlv_gamecoins) frlv_gamecoins from dim.user_bankrupt_relieve where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) group by fuid) u 
on x.fuid = u.fuid
left join (select fuid, sum(fparty_num) fparty_num, sum(fplay_time) fplay_time, 1 play_day, sum(fwin_party_num) fwin_party_num, 
sum(flose_party_num) flose_party_num, sum(fcharge) fcharge, sum(fwin_amt) fwin_amt, sum(flose_amt) flose_amt, max(fmax_win_amt) fmax_win_amt, -max(fmax_lose_amt) fmax_lose_amt 
from dim.user_gameparty_nomatch where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) group by fuid) v on x.fuid = v.fuid 
left join (select fuid, sum(join_amt) join_amt, max(max_join_amt) max_join_amt, sum(join_cnt) join_cnt, sum(match_cnt) match_cnt, 
sum(win_amt) win_amt, sum(win_cnt) win_cnt, max(max_win_amt) max_win_amt from dim.match_user_wide_using where dt = '%(statdate)s' 
and fbpid in (select fbpid from dim.bpid_map where fgamefsk = 4132314431) group by fuid) w 
on x.fuid = w.fuid 
left join (select uid, reg_signup_at, reg_channel_code, reg_m_dtype, reg_m_pixel, reg_m_imei, reg_ip, reg_ip_country, reg_ip_province, reg_ip_city,user_gender, user_phone, 
user_imei, user_imsi, user_email, login_last_2nd_at, login_last_at, login_last_country, login_last_province, 
login_last_city, act_last_2nd_at, act_last_at, pay_last_2nd_at, pay_last_at, pay_usd_last, bankruptcy_last_at, relieve_last_at, play_last_2nd_at, play_last_at, 
play_pgame_last, play_subgame_last, play_gsubgame_last, play_gamecoin_last_num, match_last_2nd_at, match_last_at, match_pgame_last, match_subgame_last, match_gsubgame_last, 
act_first_at, play_first_at, match_first_at, login_first_at, pay_first_at 
from veda.veda_user_info_all where gamefsk = 4132314431) a on x.fuid = a.uid;


insert overwrite table veda.dfqp_user_wide_table
select 
	uid, dt, reg_signup_at, reg_channel_code, reg_m_dtype, reg_m_pixel, reg_m_imei, reg_ip, reg_ip_country, reg_ip_province, reg_ip_city, gender, constellation, birthday, 
	vip_level, vip_type, vip_due_at, m_phone, m_imsi, m_imei, e_mail, login_first_at, login_last_2nd_at, login_last_at, login_last_country, login_last_province, 
	login_last_city, login_cnt, login_device_num, login_ip_num, login_day_num, act_first_at, act_last_2nd_at, act_last_at, act_day_num, 
	case when act_day_num_recently > act_day_num_max then act_day_num_recently else act_day_num_max end act_day_num_max, 
	act_day_num_recently, pay_first_at, pay_last_2nd_at, pay_last_at, pay_usd_max, pay_usd_last, pay_usd_num, pay_cnt, bankruptcy_cnt, bankruptcy_last_at, bankruptcy_day_num, 
	relieve_cnt, relieve_last_at, relieve_day_num, relieve_gamecoin_num, play_first_at, play_last_2nd_at, play_last_at, play_inning_num, play_time, play_day_num, 
	play_win_num, play_lose_num, play_charge_total, play_gamecoin_win_num, play_gamecoin_lose_num, play_pgame_last, play_subgame_last, play_gsubgame_last, 
	play_gamecoin_last_num, play_gamecoin_win_max, play_gamecoin_lose_max, match_join_fee_total, match_join_fee_max, match_join_cnt, match_cnt, match_win_cnt, 
	match_income_total, match_income_max, match_first_at, match_last_2nd_at, match_last_at, match_pgame_last, match_subgame_last, match_gsubgame_last, friends_num, 
	subgame_num, gamecoins_num, currencies_num, item_num, bank_gamecoins_num, bank_currencies_num 
from 
(select 
	nvl(x.uid, y.uid) uid, 
	'%(statdate)s' dt, 
	nvl(x.reg_signup_at, y.reg_signup_at) reg_signup_at, 
	nvl(x.reg_channel_code, y.reg_channel_code) reg_channel_code, 
	nvl(x.reg_m_dtype, y.reg_m_dtype) reg_m_dtype, 
	nvl(x.reg_m_pixel, y.reg_m_pixel) reg_m_pixel, 
	nvl(x.reg_m_imei, y.reg_m_imei) reg_m_imei, 
	nvl(x.reg_ip, y.reg_ip) reg_ip, 
	nvl(x.reg_ip_country, y.reg_ip_country) reg_ip_country, 
	nvl(x.reg_ip_province, y.reg_ip_province) reg_ip_province, 
	nvl(x.reg_ip_city, y.reg_ip_city) reg_ip_city, 
	nvl(y.gender, x.gender) gender, 
	nvl(y.constellation, x.constellation) constellation, 
	nvl(y.birthday, x.birthday) birthday, 
	nvl(y.vip_level, x.vip_level) vip_level, 
	nvl(y.vip_type, x.vip_type) vip_type, 
	nvl(y.vip_due_at, x.vip_due_at) vip_due_at, 
	nvl(y.m_phone, x.m_phone) m_phone, 
	nvl(y.m_imsi, x.m_imsi) m_imsi, 
	nvl(y.m_imei, x.m_imei) m_imei, 
	nvl(y.e_mail, x.e_mail) e_mail, 
	nvl(x.login_first_at, y.login_first_at) login_first_at, 
	nvl(y.login_last_2nd_at, x.login_last_2nd_at) login_last_2nd_at, 
	nvl(y.login_last_at, x.login_last_at) login_last_at, 
	nvl(y.login_last_country, x.login_last_country) login_last_country, 
	nvl(y.login_last_province, x.login_last_province) login_last_province, 
	nvl(y.login_last_city, x.login_last_city) login_last_city, 
	nvl(x.login_cnt, 0) + nvl(y.login_cnt, 0) login_cnt, 
	nvl(x.login_device_num, 0) + nvl(y.login_device_num, 0) login_device_num, 
	nvl(x.login_ip_num, 0) + nvl(y.login_ip_num, 0) login_ip_num, 
	nvl(x.login_day_num, 0) + nvl(y.login_day_num, 0) login_day_num, 
	nvl(x.act_first_at, y.act_first_at) act_first_at, 
	nvl(y.act_last_2nd_at, x.act_last_2nd_at) act_last_2nd_at, 
	nvl(y.act_last_at, x.act_last_at) act_last_at, 
	nvl(x.act_day_num, 0) + nvl(y.act_day_num, 0) act_day_num,  
	x.act_day_num_max, 
	case when nvl(y.act_day_num_recently, 0) > 0 then nvl(x.act_day_num_recently, 0) + nvl(y.act_day_num_recently, 0) else 0 end act_day_num_recently, 
	nvl(x.pay_first_at, y.pay_first_at) pay_first_at, 
	nvl(y.pay_last_2nd_at, x.pay_last_2nd_at) pay_last_2nd_at, 
	nvl(y.pay_last_at, x.pay_last_at) pay_last_at, 
	case when y.pay_usd_max > x.pay_usd_max then y.pay_usd_max else x.pay_usd_max end pay_usd_max, 
	nvl(y.pay_usd_last, x.pay_usd_last) pay_usd_last, 
	nvl(x.pay_usd_num, 0) + nvl(y.pay_usd_num, 0) pay_usd_num, 
	nvl(x.pay_cnt, 0) + nvl(y.pay_cnt, 0) pay_cnt, 
	nvl(x.bankruptcy_cnt, 0) + nvl(y.bankruptcy_cnt, 0) bankruptcy_cnt, 
	nvl(y.bankruptcy_last_at, x.bankruptcy_last_at) bankruptcy_last_at, 
	nvl(x.bankruptcy_day_num, 0) + nvl(y.bankruptcy_day_num, 0) bankruptcy_day_num, 
	nvl(x.relieve_cnt, 0) + nvl(y.relieve_cnt, 0) relieve_cnt, 
	nvl(y.relieve_last_at, x.relieve_last_at) relieve_last_at, 
	nvl(x.relieve_day_num, 0) + nvl(y.relieve_day_num, 0) relieve_day_num, 
	nvl(x.relieve_gamecoin_num, 0) + nvl(y.relieve_gamecoin_num, 0) relieve_gamecoin_num, 
	nvl(x.play_first_at, y.play_first_at) play_first_at, 
	nvl(y.play_last_2nd_at, x.play_last_2nd_at) play_last_2nd_at, 
	nvl(y.play_last_at, x.play_last_at) play_last_at, 
	nvl(x.play_inning_num, 0) + nvl(y.play_inning_num, 0) play_inning_num, 
	nvl(x.play_time, 0) + nvl(y.play_time, 0) play_time, 
	nvl(x.play_day_num, 0) + nvl(y.play_day_num, 0) play_day_num, 
	nvl(x.play_win_num, 0) + nvl(y.play_win_num, 0) play_win_num, 
	nvl(x.play_lose_num, 0) + nvl(y.play_lose_num, 0) play_lose_num, 
	nvl(x.play_charge_total, 0) + nvl(y.play_charge_total, 0) play_charge_total, 
	nvl(x.play_gamecoin_win_num, 0) + nvl(y.play_gamecoin_win_num, 0) play_gamecoin_win_num, 
	nvl(x.play_gamecoin_lose_num, 0) + nvl(y.play_gamecoin_lose_num, 0) play_gamecoin_lose_num, 
	nvl(y.play_pgame_last, x.play_pgame_last) play_pgame_last, 
	nvl(y.play_subgame_last, x.play_subgame_last) play_subgame_last, 
	nvl(y.play_gsubgame_last, x.play_gsubgame_last) play_gsubgame_last, 
	nvl(y.play_gamecoin_last_num, x.play_gamecoin_last_num) play_gamecoin_last_num, 
	case when y.play_gamecoin_win_max > x.play_gamecoin_win_max then y.play_gamecoin_win_max else x.play_gamecoin_win_max end play_gamecoin_win_max, 
	case when y.play_gamecoin_lose_max < x.play_gamecoin_lose_max then y.play_gamecoin_lose_max else x.play_gamecoin_lose_max end play_gamecoin_lose_max, 
	nvl(x.match_join_fee_total, 0) + nvl(y.match_join_fee_total, 0) match_join_fee_total, 
	case when y.match_join_fee_max > x.match_join_fee_max then y.match_join_fee_max else x.match_join_fee_max end match_join_fee_max, 
	nvl(x.match_join_cnt, 0) + nvl(y.match_join_cnt, 0) match_join_cnt, 
	nvl(x.match_cnt, 0) + nvl(y.match_cnt, 0) match_cnt, 
	nvl(x.match_win_cnt, 0) + nvl(y.match_win_cnt, 0) match_win_cnt, 
	nvl(x.match_income_total, 0) + nvl(y.match_income_total, 0) match_income_total, 
	case when y.match_income_max > x.match_income_max then y.match_income_max else x.match_income_max end match_income_max, 
	nvl(x.match_first_at, y.match_first_at) match_first_at, 
	nvl(y.match_last_2nd_at, x.match_last_2nd_at) match_last_2nd_at, 
	nvl(y.match_last_at, x.match_last_at) match_last_at, 
	nvl(y.match_pgame_last, x.match_pgame_last) match_pgame_last, 
	nvl(y.match_subgame_last, x.match_subgame_last) match_subgame_last, 
	nvl(y.match_gsubgame_last, x.match_gsubgame_last) match_gsubgame_last, 
	nvl(y.friends_num, x.friends_num) friends_num, 
	nvl(y.subgame_num, x.subgame_num) subgame_num, 
	nvl(nvl(y.gamecoins_num, x.gamecoins_num), 0) gamecoins_num, 
	nvl(nvl(y.currencies_num, x.currencies_num), 0) currencies_num, 
	nvl(nvl(y.item_num, x.item_num), 0) item_num, 
	nvl(nvl(y.bank_gamecoins_num, x.bank_gamecoins_num), 0) bank_gamecoins_num, 
	nvl(nvl(y.bank_currencies_num, x.bank_currencies_num), 0) bank_currencies_num 
from veda.dfqp_user_wide_table_last x 
left join veda.dfqp_user_wide_table_mid y on x.uid = y.uid) m;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_widetable(sys.argv[1:])
a()
