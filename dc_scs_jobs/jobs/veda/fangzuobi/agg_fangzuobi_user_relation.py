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


class agg_fangzuobi_user_relation(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """

insert overwrite table veda.dfqp_user_relation_by_ip_day
partition(dt='%(statdate)s')
select x.fuid mid1, y.fuid mid2, concat(x.fuid, ':', y.fuid) relation_key, x.ip, 
concat('〖', x.fuid, '〗-【', x.ip, '】-〖', y.fuid, '〗') relation 
from 
(select fuid, ip from veda.dfqp_user_ip_relation where dt = '%(statdate)s') x 
cross join 
(select fuid, ip from veda.dfqp_user_ip_relation where dt = '%(statdate)s') y 
on x.ip = y.ip 
where x.fuid < y.fuid;


insert overwrite table veda.dfqp_user_relation_info_by_ip_day 
partition(dt='%(statdate)s')
select mid1, mid2, relation_key, concat_ws('◆', collect_set(ip))ip_list, count(1) ip_num 
from veda.dfqp_user_relation_by_ip_day where dt = '%(statdate)s' group by mid1, mid2, relation_key;

        
drop table if exists veda.tmp_gp_203_cheat_1;

create table veda.tmp_gp_203_cheat_1 as
select '%(statdate)s' dt, x.fgame_id, x.fsubname, x.finning_id, x.fuid f1, x.fis_king f1_is_king, x.is_win f1_is_win, y.fuid f2, y.fis_king f2_is_king, y.is_win f2_is_win,
concat(x.fuid, '-', y.fuid) key from
(select fgame_id, fsubname, fuid, finning_id, fis_king, case when fgamecoins > 0 then 1 when fgamecoins < 0 then 0 end is_win from stage_dfqp.user_gameparty_stg
where fbpid in (select fbpid from dim.bpid_map where fgamename = '地方棋牌') and dt = '%(statdate)s'
order by fgame_id, fsubname, finning_id, fuid, fis_king) x
cross join
(select fgame_id, fsubname, fuid, finning_id, fis_king, case when fgamecoins > 0 then 1 when fgamecoins < 0 then 0 end is_win from stage_dfqp.user_gameparty_stg
where fbpid in (select fbpid from dim.bpid_map where fgamename = '地方棋牌') and dt = '%(statdate)s'
order by fgame_id, fsubname, finning_id, fuid, fis_king) y
on x.finning_id = y.finning_id and x.fsubname = y.fsubname where x.fuid < y.fuid;

drop table if exists veda.tmp_gp_203_cheat_2;

create table veda.tmp_gp_203_cheat_2 as
select dt, fgame_id, fsubname,
count(1) total,
f1,
sum(case when f1_is_king = 1 and f1_is_win = 1 then 1 else 0 end) f1_king_win,
sum(case when f1_is_king = 1 then 1 else 0 end) f1_king,
sum(case when f1_is_win = 1 then 1 else 0 end) f1_win,
f2,
sum(case when f2_is_king = 1 and f2_is_win = 1 then 1 else 0 end) f2_king_win,
sum(case when f2_is_king = 1 then 1 else 0 end) f2_king,
sum(case when f2_is_win = 1 then 1 else 0 end) f2_win
from veda.tmp_gp_203_cheat_1 where dt = '%(statdate)s' group by dt, fgame_id, fsubname, f1, f2;

drop table if exists veda.tmp_gp_203_cheat_4;
create table veda.tmp_gp_203_cheat_4 as
select dt, fgame_id, fsubname, fuid,
count(1) play_total,
sum(case when fgamecoins > 0 then 1 when fgamecoins < 0 then 0 end) win_total,
sum(case when fcoins_type = 1 then fgamecoins else 0 end) gamecoins_total,
sum(case when fcoins_type = 11 then fgamecoins else 0 end) goldcoins_total
from stage_dfqp.user_gameparty_stg where fbpid in (select fbpid from dim.bpid_map where fgamename = '地方棋牌') and dt = '%(statdate)s'
group by dt, fgame_id, fsubname, fuid;




insert overwrite table veda.dfqp_fzb_user_relation
partition(dt='%(statdate)s')
select x.fsubname, x.f1, x.f2, x.total,
x.total/y.play_total f1_st_rate,
x.total/z.play_total f2_st_rate,
x.f1_king_win/x.f1_king f1_kw_rate,
x.f2_king_win/x.f2_king f2_kw_rate,
x.f1_win/x.total f1_st_win_rate,
x.f2_win/x.total f2_st_win_rate,
y.win_total/y.play_total f1_win_rate,
z.win_total/z.play_total f2_win_rate,
y.gamecoins_total f1_gamecoins,
y.goldcoins_total f1_goldcoins,
z.gamecoins_total f2_gamecoins,
z.goldcoins_total f2_goldcoins,
a.device_list,
e.ip_list,
b.device_list,
f.ip_list,
c.device_list,
c.device_num,
g.ip_list,
g.ip_num,
x.fgame_id, 
h.ip_list ip_list_day, 
h.ip_num ip_num_day 
from veda.tmp_gp_203_cheat_2 x
left join (select * from veda.tmp_gp_203_cheat_4 where dt = '%(statdate)s') y on x.f1 = y.fuid and x.fsubname = y.fsubname and x.fgame_id = y.fgame_id 
left join (select * from veda.tmp_gp_203_cheat_4 where dt = '%(statdate)s') z on x.f2 = z.fuid and x.fsubname = z.fsubname and x.fgame_id = z.fgame_id 
left join veda.dfqp_user_device_info a on x.f1 = a.fuid
left join veda.dfqp_user_device_info b on x.f2 = b.fuid
left join veda.dfqp_user_relation_info_by_device c on x.f1 = c.mid1 and x.f2 = c.mid2
left join veda.dfqp_user_ip_info e on x.f1 = e.fuid
left join veda.dfqp_user_ip_info f on x.f2 = f.fuid
left join veda.dfqp_user_relation_info_by_ip g on x.f1 = g.mid1 and x.f2 = g.mid2
left join veda.dfqp_user_relation_info_by_ip_day h on x.f1 = h.mid1 and x.f2 = h.mid2;


        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_user_relation(sys.argv[1:])
a()
