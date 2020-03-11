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


class agg_fangzuobi_suspect_user_fin(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """insert overwrite table veda.user_behavior_path
partition(dt='%(statdate)s')
select o.fuid, regexp_replace(concat_ws('-', collect_set(o.tag)), 'B-A', 'A-B') behavior, regexp_replace(concat_ws(' - ', collect_set(o.desc)), 'B登录 - A注册', 'A注册 - B登录') behavior_desc, concat_ws(' - ', collect_set(o.timer)) behavior_timer, '', '', '', '' from
(select n.fuid, n.action_at, n.tag, n.desc, concat(n.desc, substring(n.action_at, 12)) timer from
(select m.fuid, m.action_at, m.tag, m.desc, row_number() over (partition by m.fuid order by m.action_at) rownum from
(select fuid, fsignup_at action_at, 'A' tag, 'A注册' desc from stage.user_signup_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, flogin_at action_at, 'B' tag, 'B登录' desc from stage.user_login_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, lts_at action_at, 'C' tag, 'C签到奖励' desc from stage.pb_gamecoins_stream_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s' and x1.act_id = 101
union all
select fuid, fs_timer action_at,
        case when split(x1.fsubname, '_')[1] in ('500', '600', '700') or x1.fsubname in ('定局赛', '定时赛', '定人赛') then 'F'
             when split(x1.fsubname, '_')[1] = '10100' then 'J'
             when split(x1.fsubname, '_')[1] = '30100' then 'I'
             when split(x1.fsubname, '_')[1] = '100' and x1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'G'
             when split(x1.fsubname, '_')[1] = '100' then 'H'
             when x1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'D'
             else 'E'
        end tag,
        case when split(x1.fsubname, '_')[1] in ('500', '600', '700') or x1.fsubname in ('定局赛', '定时赛', '定人赛') then 'F比赛场'
             when split(x1.fsubname, '_')[1] = '10100' then 'J金条约牌房'
             when split(x1.fsubname, '_')[1] = '30100' then 'I积分约牌房'
             when split(x1.fsubname, '_')[1] = '100' and x1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'G银币约牌房（金流）'
             when split(x1.fsubname, '_')[1] = '100' then 'H银币约牌房（非金流）'
             when x1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'D普通场（金流）'
             else 'E普通场（非金流）'
        end desc
from stage.user_gameparty_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, frupt_at action_at, 'K' tag, 'K破产' desc from stage.user_bankrupt_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, fdate action_at, 'L' tag, 'L付费' desc from stage.payment_stream_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, flts_at action_at, 'M' tag, 'M领取救济' desc from stage.user_bankrupt_relieve_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, flts_at action_at, 'N' tag, 'N保险箱操作' desc from stage.user_bank_stage x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
union all
select fuid, flogout_at action_at, 'O' tag, 'O登出' desc from stage.user_logout_stg x1 left join dim.bpid_map x2 on x1.fbpid = x2.fbpid where x2.fgamename = '地方棋牌' and x1.dt = '%(statdate)s'
) m) n
left join
(select b.fuid, b.action_at, b.tag, row_number() over (partition by b.fuid order by b.action_at) + 1 rownum from
(select fuid, fsignup_at action_at, 'A' tag, 'A注册' desc from stage.user_signup_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, flogin_at action_at, 'B' tag, 'B登录' desc from stage.user_login_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, lts_at action_at, 'C' tag, 'C签到奖励' desc from stage.pb_gamecoins_stream_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s' and y1.act_id = 101
union all
select fuid, fs_timer action_at,
        case when split(y1.fsubname, '_')[1] in ('500', '600', '700') or y1.fsubname in ('定局赛', '定时赛', '定人赛') then 'F'
             when split(y1.fsubname, '_')[1] = '10100' then 'J'
             when split(y1.fsubname, '_')[1] = '30100' then 'I'
             when split(y1.fsubname, '_')[1] = '100' and y1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'G'
             when split(y1.fsubname, '_')[1] = '100' then 'H'
             when y1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'D'
             else 'E'
        end tag,
        case when split(y1.fsubname, '_')[1] in ('500', '600', '700') or y1.fsubname in ('定局赛', '定时赛', '定人赛') then 'F比赛场'
             when split(y1.fsubname, '_')[1] = '10100' then 'J金条约牌房'
             when split(y1.fsubname, '_')[1] = '30100' then 'I积分约牌房'
             when split(y1.fsubname, '_')[1] = '100' and y1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'G银币约牌房（金流）'
             when split(y1.fsubname, '_')[1] = '100' then 'H银币约牌房（非金流）'
             when y1.fgame_id in (3, 10, 23, 204, 20, 21, 27, 29, 903) then 'D普通场（金流）'
             else 'E普通场（非金流）'
        end desc
from stage.user_gameparty_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, frupt_at action_at, 'K' tag, 'K破产' desc from stage.user_bankrupt_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, fdate action_at, 'L' tag, 'L付费' desc from stage.payment_stream_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, flts_at action_at, 'M' tag, 'M领取救济' desc from stage.user_bankrupt_relieve_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, flts_at action_at, 'N' tag, 'N保险箱操作' desc from stage.user_bank_stage y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
union all
select fuid, flogout_at action_at, 'O' tag, 'O登出' desc from stage.user_logout_stg y1 left join dim.bpid_map y2 on y1.fbpid = y2.fbpid where y2.fgamename = '地方棋牌' and y1.dt = '%(statdate)s'
) b) a
on n.fuid = a.fuid and n.rownum = a.rownum
where a.tag is null or a.tag <> n.tag
order by n.fuid, n.action_at) o
group by o.fuid;


insert overwrite table veda.user_behavior_extra
partition(dt='%(statdate)s')
select distinct n.fuid, n.fbehavior_path_basic, n.fbehavior_path_desc, n.fbehavior_path_1, m.fip, m.fm_imei, o.fphone, '', m.fbpid, m.fhallname from
(select fuid, fbehavior_path_basic, fbehavior_path_desc, fbehavior_path_1 from veda.user_behavior_path where dt = '%(statdate)s') n
left join
(select x.fuid, x.fip, x.fm_imei, x.fbpid, y.fhallname from stage.user_login_stg x left join dim.bpid_map y on x.fbpid = y.fbpid where y.fgamename = '地方棋牌' and x.dt = '%(statdate)s') m on m.fuid = n.fuid
left join
(select a.fuid, a.fphone from stage.user_async_stg a left join dim.bpid_map b on a.fbpid = b.fbpid where b.fgamename = '地方棋牌' and a.dt = '%(statdate)s') o on n.fuid = o.fuid
where n.fuid is not null;


insert overwrite table veda.suspect_user_p1
partition(dt='%(statdate)s')
--规则1
select 5 rule_id, m.fuid, z.fip, z.fm_imei, z.fsignup_at, '自动1：同IP同设备用户数量>=10 & bpid比例 >= 2 & BCO & 2017注册' rule, '%(statdate)s' fdate from
(select * from veda.user_behavior_extra where dt = '%(statdate)s' and fbehavior_path_basic in ('O-B-C', 'B-C-O', 'B-C')) m
join
(select fuid, dt, fsignup_at, fip, fm_imei from stage.user_signup_stg d1 left join dim.bpid_map d2 on d1.fbpid = d2.fbpid where d2.fgamename = '地方棋牌') z on m.fuid = z.fuid
join
(select fuid, sum(fparty_num) fparty_num from veda.user_basic_day where dt >= date_add('%(statdate)s', -29) and dt <= '%(statdate)s' group by fuid) n on m.fuid = n.fuid
left join
(select fm_imei, count(fm_imei) bpid_num from
(select distinct fm_imei, fbpid from veda.user_behavior_extra where dt = '%(statdate)s' and fbehavior_path_basic in ('O-B-C', 'B-C-O', 'B-C')) a group by a.fm_imei) o on m.fm_imei = o.fm_imei
left join
(select fm_imei, fip, count(fm_imei) user_num from
(select distinct fm_imei, fip, fuid from veda.user_behavior_extra where dt = '%(statdate)s' and fbehavior_path_basic in ('O-B-C', 'B-C-O', 'B-C')) a group by a.fm_imei, a.fip) p on m.fm_imei = p.fm_imei
where p.user_num >= 10 and n.fparty_num <= 1 and p.user_num/o.bpid_num >= 2 and z.dt >= '2017-01-01'
union all
--规则3
select 6 rule_id, m.fuid, z.fip, z.fm_imei, z.fsignup_at, '自动3：同设备用户数量>=100 & bpid比例 >= 3 & 任意行为 & 2017注册' rule, '%(statdate)s' fdate from
(select * from veda.user_behavior_extra where dt = '%(statdate)s') m
join
(select fuid, dt, fsignup_at, fip, fm_imei from stage.user_signup_stg d1 left join dim.bpid_map d2 on d1.fbpid = d2.fbpid where d2.fgamename = '地方棋牌') z on m.fuid = z.fuid
join
(select fuid, sum(fparty_num) fparty_num from veda.user_basic_day where dt >= date_add('%(statdate)s', -29) and dt <= '%(statdate)s' group by fuid) n on m.fuid = n.fuid
left join
(select fm_imei, count(fm_imei) bpid_num from
(select distinct fm_imei, fbpid from veda.user_behavior_extra where dt = '%(statdate)s') a group by a.fm_imei) o on m.fm_imei = o.fm_imei
left join
(select fm_imei, count(fm_imei) user_num from
(select distinct fm_imei, fuid from veda.user_behavior_extra where dt = '%(statdate)s') a group by a.fm_imei) p on m.fm_imei = p.fm_imei
where p.user_num >= 100 and n.fparty_num <= 1 and p.user_num/o.bpid_num >= 3 and z.dt >= '2017-01-01';

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_suspect_user_fin(sys.argv[1:])
a()
