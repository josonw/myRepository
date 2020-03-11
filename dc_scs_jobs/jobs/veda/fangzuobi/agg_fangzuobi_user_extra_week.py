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


class agg_fangzuobi_user_extra_week(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
insert overwrite table veda.tmp_user_play_100_day
partition(dt='%(statdate)s')
select x.fuid, x.first_login, x.bank_gamecoins, y.first_at, y.first_gamecoins, z.last_at, z.last_gamecoins from
(select x6.fuid, x6.first_login, x5.bank_gamecoins from
(select fuid, flogin_at, bank_gamecoins from stage.user_login_stg x3 left join dim.bpid_map x4 on x3.fbpid = x4.fbpid where x4.fgamename = '地方棋牌' and x3.dt = '%(statdate)s') x5
join
(select fuid, min(flogin_at) first_login from stage.user_login_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
group by fuid) x6
on x5.fuid = x6.fuid and x5.flogin_at = x6.first_login) x
left join
(select y6.fuid, y6.first_at, y5.first_gamecoins from
(select fuid, flts_at, (fuser_gcoins - fgamecoins) first_gamecoins from stage.user_gameparty y3 left join dim.bpid_map y4 on y3.fbpid = y4.fbpid where y4.fgamename = '地方棋牌' and y3.dt = '%(statdate)s' and split(fsubname, '_')[1] = '100') y5
join
(select fuid, min(flts_at) first_at from stage.user_gameparty y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s' and split(fsubname, '_')[1] = '100'
group by fuid) y6
on y5.fuid = y6.fuid and y5.flts_at = y6.first_at) y on x.fuid = y.fuid
left join
(select z6.fuid, z6.last_at, z5.last_gamecoins from
(select fuid, flts_at, fuser_gcoins last_gamecoins from stage.user_gameparty z3 left join dim.bpid_map z4 on z3.fbpid = z4.fbpid where z4.fgamename = '地方棋牌' and z3.dt = '%(statdate)s' and split(fsubname, '_')[1] = '100') z5
join
(select fuid, max(flts_at) last_at from stage.user_gameparty z1 left join dim.bpid_map z2 on z1.fbpid = z2.fbpid where z2.fgamename = '地方棋牌' and z1.dt = '%(statdate)s' and split(fsubname, '_')[1] = '100'
group by fuid) z6
on z5.fuid = z6.fuid and z5.flts_at = z6.last_at) z on x.fuid = z.fuid
where y.fuid is not null and z.fuid is not null and x.fuid is not null;


insert overwrite table veda.user_extra_day
partition(dt='%(statdate)s')
select m.fuid, m.fip, w.fip_num, w.ip_list, m.fm_imei, z.fm_imei_num, z.imei_list, nvl(m.fsimulator_flag, 0), 0, 0, 0,
case when u.save > 0 then 1 else 0 end is_save, case when u.get > 0 then 1 else 0 end is_get,
nvl(q.rupt_num, 0), p.first_gamecoins, p.first_bank_gamecoins, p.last_gamecoins, p.last_bank_gamecoins, nvl(v.fplay_n_gold_cnt, 0), nvl(v.fplay_n_gold_100_cnt, 0),
nvl(v.fplay_gold_cnt, 0), nvl(v.fplay_gold_100_cnt, 0) from
(select y.fuid, flogin_at, fm_imei, fip, fsimulator_flag from
(select fuid, flogin_at, fm_imei, fip, fsimulator_flag from stage.user_login_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s') y
join
(select fuid, max(flogin_at) login_last_at from stage.user_login_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
group by fuid) x
on x.fuid = y.fuid and y.flogin_at = x.login_last_at) m
left join
(select z3.fuid, count(*) fm_imei_num, concat_ws(',', collect_set(z3.fm_imei)) imei_list from
(select distinct fuid, fm_imei from stage.user_login_stg z1 left join dim.bpid_map z2 on z1.fbpid = z2.fbpid where z2.fgamename = '地方棋牌' and z1.dt = '%(statdate)s' order by fuid, fm_imei) z3
group by z3.fuid) z on m.fuid = z.fuid
left join
(select w3.fuid, count(*) fip_num, concat_ws(',', collect_set(w3.fip)) ip_list from
(select distinct fuid, fip from stage.user_login_stg w1 left join dim.bpid_map w2 on w1.fbpid = w2.fbpid where w2.fgamename = '地方棋牌' and w1.dt = '%(statdate)s' order by fuid, fip) w3
group by w3.fuid) w on m.fuid = w.fuid
left join
(select fuid, count(*) rupt_num from stage.user_bankrupt_relieve_stg q1 left join dim.bpid_map q2 on q1.fbpid = q2.fbpid where q2.fgamename = '地方棋牌' and q1.dt = '%(statdate)s'
group by fuid) q on m.fuid = q.fuid
left join
(select fuid,
sum(case when fact_type = 0 then 1 else 0 end) save,
sum(case when fact_type = 1 then 1 else 0 end) get
from stage.user_bank_stage u1 left join dim.bpid_map u2 on u1.fbpid = u2.fbpid where u2.fgamename = '地方棋牌' and u1.dt = '%(statdate)s'
group by fuid) u on m.fuid = u.fuid
left join
(select fuid,
sum(case when v1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) and split(fsubname, '_')[1] != '100'  then 1 else 0 end) fplay_gold_cnt,
sum(case when v1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) and split(fsubname, '_')[1] = '100'  then 1 else 0 end) fplay_gold_100_cnt,
sum(case when v1.fgame_id not in (3, 10, 23, 204, 20, 21, 27, 29, 903) and split(fsubname, '_')[1] != '100'  then 1 else 0 end) fplay_n_gold_cnt,
sum(case when v1.fgame_id not in (3, 10, 23, 204, 20, 21, 27, 29, 903) and split(fsubname, '_')[1] = '100'  then 1 else 0 end) fplay_n_gold_100_cnt
from stage.user_gameparty_stg v1 left join dim.bpid_map v2 on v1.fbpid = v2.fbpid where v2.fgamename = '地方棋牌' and v1.dt = '%(statdate)s'
group by fuid) v on m.fuid = v.fuid
left join
(select p2.fuid,
avg(p2.first_gamecoins) first_gamecoins,
case when first_login < first_at then avg(p2.first_bank_gamecoins) + nvl(sum(case when p1.flts_at > first_login and p1.flts_at < first_at then nvl(p1.fact_num, 0) else 0 end), 0)
     when first_login > first_at and first_login < last_at then avg(p2.first_bank_gamecoins) - nvl(sum(case when p1.flts_at > first_at and p1.flts_at < first_login then nvl(p1.fact_num, 0) else 0 end), 0)
     when first_login > last_at then avg(p2.first_bank_gamecoins) - nvl(sum(case when p1.flts_at < first_login and p1.flts_at > first_at then nvl(p1.fact_num, 0) else 0 end), 0)
end first_bank_gamecoins,
avg(p2.last_gamecoins) last_gamecoins,
case when first_login < first_at then avg(p2.first_bank_gamecoins) + nvl(sum(case when p1.flts_at > first_login and p1.flts_at < last_at then nvl(p1.fact_num, 0) else 0 end), 0)
     when first_login > first_at and first_login < last_at then avg(p2.first_bank_gamecoins) + nvl(sum(case when p1.flts_at < last_at and p1.flts_at > first_login then nvl(p1.fact_num, 0) else 0 end), 0)
     when first_login > last_at then avg(p2.first_bank_gamecoins) - nvl(sum(case when p1.flts_at < first_login and p1.flts_at > last_at then nvl(p1.fact_num, 0) else 0 end), 0)
end last_bank_gamecoins from
(select fuid, flts_at, fact_num from stage.user_bank_stage where dt = '%(statdate)s' and fact_type in (0, 1) and fcurrencies_type = 0) p1
right join
(select fuid, flogin_first_at first_login, first_bank_gamecoins, fplay_100_first_at first_at, fplay_100_first_gc first_gamecoins, fplay_100_last_at last_at, fplay_100_last_gc last_gamecoins
from veda.tmp_user_play_100_day where dt = '%(statdate)s') p2
on p1.fuid = p2.fuid
group by p2.fuid, first_at, first_login, last_at) p on m.fuid = p.fuid;



insert overwrite table veda.user_extra_week
partition(dt='%(statdate)s')
select
    x.fuid,
    o.fsignup_at,
    day1_extra.fip_last day1_ip_last,
    day2_extra.fip_last day2_ip_last,
    day3_extra.fip_last day3_ip_last,
    day4_extra.fip_last day4_ip_last,
    day5_extra.fip_last day5_ip_last,
    day6_extra.fip_last day6_ip_last,
    day7_extra.fip_last day7_ip_last,
    day1_extra.fip_cnt day1_ip_cnt,
    day2_extra.fip_cnt day2_ip_cnt,
    day3_extra.fip_cnt day3_ip_cnt,
    day4_extra.fip_cnt day4_ip_cnt,
    day5_extra.fip_cnt day5_ip_cnt,
    day6_extra.fip_cnt day6_ip_cnt,
    day7_extra.fip_cnt day7_ip_cnt,
    e.fip_num,
    day1_extra.fip_list day1_ip_list,
    day2_extra.fip_list day2_ip_list,
    day3_extra.fip_list day3_ip_list,
    day4_extra.fip_list day4_ip_list,
    day5_extra.fip_list day5_ip_list,
    day6_extra.fip_list day6_ip_list,
    day7_extra.fip_list day7_ip_list,
    e.fip_list,
    day1_extra.fimei_last day1_imei_last,
    day2_extra.fimei_last day2_imei_last,
    day3_extra.fimei_last day3_imei_last,
    day4_extra.fimei_last day4_imei_last,
    day5_extra.fimei_last day5_imei_last,
    day6_extra.fimei_last day6_imei_last,
    day7_extra.fimei_last day7_imei_last,
    day1_extra.fimei_cnt day1_imei_cnt,
    day2_extra.fimei_cnt day2_imei_cnt,
    day3_extra.fimei_cnt day3_imei_cnt,
    day4_extra.fimei_cnt day4_imei_cnt,
    day5_extra.fimei_cnt day5_imei_cnt,
    day6_extra.fimei_cnt day6_imei_cnt,
    day7_extra.fimei_cnt day7_imei_cnt,
    d.fm_imei_num,
    day1_extra.fimei_list day1_imei_list,
    day2_extra.fimei_list day2_imei_list,
    day3_extra.fimei_list day3_imei_list,
    day4_extra.fimei_list day4_imei_list,
    day5_extra.fimei_list day5_imei_list,
    day6_extra.fimei_list day6_imei_list,
    day7_extra.fimei_list day7_imei_list,
    d.fm_imei_list,
    day1_extra.fis_simu_last day1_is_simu_last,
    day2_extra.fis_simu_last day2_is_simu_last,
    day3_extra.fis_simu_last day3_is_simu_last,
    day4_extra.fis_simu_last day4_is_simu_last,
    day5_extra.fis_simu_last day5_is_simu_last,
    day6_extra.fis_simu_last day6_is_simu_last,
    day7_extra.fis_simu_last day7_is_simu_last,
    x.login_days,
    case when nvl(day1_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day1_login,
    case when nvl(day2_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day2_login,
    case when nvl(day3_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day3_login,
    case when nvl(day4_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day4_login,
    case when nvl(day5_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day5_login,
    case when nvl(day6_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day6_login,
    case when nvl(day7_basic.flogin_cnt, 0) > 0 then 1 else 0 end is_day7_login,
    x.pay_cnt,
    x.pay_total,
    case when y.fis_exchange_gold > 0 then 1 else 0 end is_exchange_gold,
    case when y.fis_exchange_fee > 0 then 1 else 0 end is_exchange_fee,
    case when y.fis_exchange_item > 0 then 1 else 0 end is_exchange_item,
    case when y.fis_save > 0 then 1 else 0 end is_save,
    case when y.fis_get > 0 then 1 else 0 end is_get,
    concat(nvl(day1_basic.frupt_cnt, 0), '/', nvl(day1_extra.frelieve_cnt, 0)) day1_rupt,
    concat(nvl(day2_basic.frupt_cnt, 0), '/', nvl(day2_extra.frelieve_cnt, 0)) day2_rupt,
    concat(nvl(day3_basic.frupt_cnt, 0), '/', nvl(day3_extra.frelieve_cnt, 0)) day3_rupt,
    concat(nvl(day4_basic.frupt_cnt, 0), '/', nvl(day4_extra.frelieve_cnt, 0)) day4_rupt,
    concat(nvl(day5_basic.frupt_cnt, 0), '/', nvl(day5_extra.frelieve_cnt, 0)) day5_rupt,
    concat(nvl(day6_basic.frupt_cnt, 0), '/', nvl(day6_extra.frelieve_cnt, 0)) day6_rupt,
    concat(nvl(day7_basic.frupt_cnt, 0), '/', nvl(day7_extra.frelieve_cnt, 0)) day7_rupt,
    concat(nvl(day1_extra.fgc_user_first, 0), '/', nvl(day1_extra.fgc_bank_first, 0), '/', case when day1_extra.fgc_user_first + day1_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day1_extra.fgc_user_last, '/', day1_extra.fgc_bank_last, '/', case when day1_extra.fgc_user_last < 3000 then '1' else '0' end) day1_100,
    concat(nvl(day2_extra.fgc_user_first, 0), '/', nvl(day2_extra.fgc_bank_first, 0), '/', case when day2_extra.fgc_user_first + day2_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day2_extra.fgc_user_last, '/', day2_extra.fgc_bank_last, '/', case when day2_extra.fgc_user_last < 3000 then '1' else '0' end) day2_100,
    concat(nvl(day3_extra.fgc_user_first, 0), '/', nvl(day3_extra.fgc_bank_first, 0), '/', case when day3_extra.fgc_user_first + day3_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day3_extra.fgc_user_last, '/', day3_extra.fgc_bank_last, '/', case when day3_extra.fgc_user_last < 3000 then '1' else '0' end) day3_100,
    concat(nvl(day4_extra.fgc_user_first, 0), '/', nvl(day4_extra.fgc_bank_first, 0), '/', case when day4_extra.fgc_user_first + day4_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day4_extra.fgc_user_last, '/', day4_extra.fgc_bank_last, '/', case when day4_extra.fgc_user_last < 3000 then '1' else '0' end) day4_100,
    concat(nvl(day5_extra.fgc_user_first, 0), '/', nvl(day5_extra.fgc_bank_first, 0), '/', case when day5_extra.fgc_user_first + day5_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day5_extra.fgc_user_last, '/', day5_extra.fgc_bank_last, '/', case when day5_extra.fgc_user_last < 3000 then '1' else '0' end) day5_100,
    concat(nvl(day6_extra.fgc_user_first, 0), '/', nvl(day6_extra.fgc_bank_first, 0), '/', case when day6_extra.fgc_user_first + day6_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day6_extra.fgc_user_last, '/', day6_extra.fgc_bank_last, '/', case when day6_extra.fgc_user_last < 3000 then '1' else '0' end) day6_100,
    concat(nvl(day7_extra.fgc_user_first, 0), '/', nvl(day7_extra.fgc_bank_first, 0), '/', case when day7_extra.fgc_user_first + day7_extra.fgc_bank_first > 500000 then '1' else '0' end, '/', day7_extra.fgc_user_last, '/', day7_extra.fgc_bank_last, '/', case when day7_extra.fgc_user_last < 3000 then '1' else '0' end) day7_100,
    y.fplay_n_gold_cnt,
    y.fplay_n_gold_100_cnt,
    y.fplay_gold_cnt,
    y.fplay_gold_100_cnt
from
(select
    fuid,
    sum(case when flogin_cnt > 0 then 1 else 0 end) login_days,
    sum(ftotal_usd_amt) pay_total,
    sum(fpay_cnt) pay_cnt
from veda.user_basic_day where dt >= date_add('%(statdate)s', -6) and dt <= '%(statdate)s'
group by fuid) x
left join
(select
    fuid,
    sum(fplay_n_gold_cnt) fplay_n_gold_cnt,
    sum(fplay_n_gold_100_cnt) fplay_n_gold_100_cnt,
    sum(fplay_gold_cnt) fplay_gold_cnt,
    sum(fplay_gold_100_cnt) fplay_gold_100_cnt,
    sum(fis_save) fis_save,
    sum(fis_get) fis_get,
    sum(fis_exchange_gold) fis_exchange_gold,
    sum(fis_exchange_fee) fis_exchange_fee,
    sum(fis_exchange_item) fis_exchange_item
from veda.user_extra_day where dt >= date_add('%(statdate)s', -6) and dt <= '%(statdate)s'
group by fuid) y on x.fuid = y.fuid
join
(select o1.fuid, o1.fip, o1.fm_imei, o1.fsignup_at from stage.user_signup_stg o1 left join dim.bpid_map o2 on o1.fbpid = o2.fbpid where o2.fgamename = '地方棋牌') o
on x.fuid = o.fuid
left join
(select z3.fuid, count(*) fm_imei_num, concat_ws(',', collect_set(z3.fm_imei)) fm_imei_list from
(select distinct fuid, fm_imei from stage.user_login_stg z1 left join dim.bpid_map z2 on z1.fbpid = z2.fbpid where z2.fgamename = '地方棋牌' and dt >= date_add('%(statdate)s', -6) and dt <= '%(statdate)s' order by fuid, fm_imei) z3
group by z3.fuid) d on x.fuid = d.fuid
left join
(select w3.fuid, count(*) fip_num, concat_ws(',', collect_set(w3.fip)) fip_list from
(select distinct fuid, fip from stage.user_login_stg w1 left join dim.bpid_map w2 on w1.fbpid = w2.fbpid where w2.fgamename = '地方棋牌' and w1.dt = '%(statdate)s' order by fuid, fip) w3
group by w3.fuid) e on x.fuid = e.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = date_add('%(statdate)s', -6)) day1_extra on x.fuid = day1_extra.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = date_add('%(statdate)s', -5)) day2_extra on x.fuid = day2_extra.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = date_add('%(statdate)s', -4)) day3_extra on x.fuid = day3_extra.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = date_add('%(statdate)s', -3)) day4_extra on x.fuid = day4_extra.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = date_add('%(statdate)s', -2)) day5_extra on x.fuid = day5_extra.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = date_add('%(statdate)s', -1)) day6_extra on x.fuid = day6_extra.fuid
left join
(select fuid, frelieve_cnt, fgc_user_first, fgc_bank_first, fgc_user_last, fgc_bank_last, fip_last, fimei_last, fis_simu_last, fip_cnt, fimei_cnt, fip_list, fimei_list from veda.user_extra_day where dt = '%(statdate)s') day7_extra on x.fuid = day7_extra.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = date_add('%(statdate)s', -6)) day1_basic on x.fuid = day1_basic.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = date_add('%(statdate)s', -5)) day2_basic on x.fuid = day2_basic.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = date_add('%(statdate)s', -4)) day3_basic on x.fuid = day3_basic.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = date_add('%(statdate)s', -3)) day4_basic on x.fuid = day4_basic.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = date_add('%(statdate)s', -2)) day5_basic on x.fuid = day5_basic.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = date_add('%(statdate)s', -1)) day6_basic on x.fuid = day6_basic.fuid
left join
(select fuid, frupt_cnt, flogin_cnt from veda.user_basic_day where dt = '%(statdate)s') day7_basic on x.fuid = day7_basic.fuid ;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_user_extra_week(sys.argv[1:])
a()
