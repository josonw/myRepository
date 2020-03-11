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


class agg_fangzuobi_user_relation_auto(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """

insert overwrite table veda.dfqp_gameparty_relation_same_ip
partition(dt='%(statdate)s') 
select split(m.game_id, '_')[0] game_id, mid_a, mid_b, play_together_total, concat_ws('+', collect_set(type))type, n.subgame_desc, 
o.max_silver_coin max_silver_coins_a, p.max_silver_coin max_silver_coins_b from 
(select fsubname game_id, mid_a, mid_b, play_together_total, '1' type, same_ip_num_day, same_device_num
from veda.dfqp_fzb_user_relation  where same_ip_num_day >= 4 and play_together_total >= 10 and split(fsubname, '_')[1] not like '%%100'  and dt = '%(statdate)s' 
union all 
select fsubname game_id, mid_a, mid_b, play_together_total, '2' type, same_ip_num_day, same_device_num    
from veda.dfqp_fzb_user_relation  where same_device_num >= 1 and split(fsubname, '_')[1] not like '%%100'  and dt = '%(statdate)s' 
union all 
select fsubname game_id, mid_a, mid_b, play_together_total, '3' type, same_ip_num_day, same_device_num    
from veda.dfqp_fzb_user_relation where same_ip_num_day >= 1 and same_ip_num_day <4 and play_together_total >= 30 
and (together_win_rate_a > 0.9 or together_win_rate_b > 0.9 or together_win_rate_a < 0.1 or together_win_rate_b < 0.1) 
and split(fsubname, '_')[1] not like '%%100'  and dt = '%(statdate)s' and split(fsubname, '_')[0] not in (204,3,10,21,23,27,29,903)
union all 
select fsubname game_id, mid_a, mid_b, play_together_total, '4' type, same_ip_num_day, same_device_num    
from veda.dfqp_fzb_user_relation where same_ip_num_day >= 1 and same_ip_num_day <4 and play_together_total >= 30 
and (together_win_rate_a > 0.9 or together_win_rate_b > 0.9 or together_win_rate_a < 0.1 or together_win_rate_b < 0.1) 
and split(fsubname, '_')[1] not like '%%100'  and dt = '%(statdate)s' and split(fsubname, '_')[0] in (204,3,10,21,23,27,29,903)) m 
left join dw_dfqp.upd_subgame n on m.game_id = n.subgame_room_id
left join veda.dfqp_user_portrait_basic o on m.mid_a = o.mid 
left join veda.dfqp_user_portrait_basic p on m.mid_b = p.mid 
group by m.game_id, mid_a, mid_b, play_together_total, n.subgame_desc, o.max_silver_coin, p.max_silver_coin;


insert overwrite table veda.dfqp_fzb_suspect_user_day 
partition(dt='%(statdate)s')
select distinct mid from 
(select mid_a mid from veda.dfqp_gameparty_relation_same_ip where dt = '%(statdate)s' 
union all 
select mid_b mid from veda.dfqp_gameparty_relation_same_ip where dt = '%(statdate)s') m;

insert overwrite table veda.dfqp_fzb_suspect_user 
partition(dt='%(statdate)s')
select suspect_cnt rule_type, mid, suspect_cnt rule_id, '' from 
(select mid, count(1) suspect_cnt from veda.dfqp_fzb_suspect_user_day group by mid) m;


insert overwrite table veda.dfqp_gameparty_relation_same_ip_his 
select y.game_id, x.game_name, x.mid_a, o.max_silver_coin, x.mid_b, p.max_silver_coin, x.dt_first, x.dt_last, x.play_together_total, y.play_together_new, type_his, type from 
(select game_name, mid_a, mid_b, sum(play_together_total) play_together_total, min(dt) dt_first, max(dt) dt_last, concat_ws('+', collect_set(type)) type_his  
from veda.dfqp_gameparty_relation_same_ip group by game_name, mid_a, mid_b) x join 
(select game_id, game_name, mid_a, mid_b, play_together_total play_together_new, dt, type from 
(select game_id, game_name, mid_a, mid_b, play_together_total, dt, type, rank() over (partition by game_name, mid_a, mid_b order by dt desc) num 
from veda.dfqp_gameparty_relation_same_ip) m where num = 1) y 
on x.game_name = y.game_name and x.mid_a = y.mid_a and x.mid_b = y.mid_b
left join veda.dfqp_user_portrait_basic o on x.mid_a = o.mid 
left join veda.dfqp_user_portrait_basic p on y.mid_b = p.mid;

insert overwrite table veda.dfqp_gameparty_relation_different_ip
partition(dt='%(statdate)s') 
select split(fsubname, '_')[0] game_id, mid_a, mid_b, play_together_total   
from veda.dfqp_fzb_user_relation  where nvl(same_ip_num_day, 0) = 0 and play_together_total >= 20 and split(fsubname, '_')[1] not like '%%100' 
and (play_together_rate_a >= 0.8 or play_together_rate_b >= 0.8) and (king_win_rate_a >= 0.8 or king_win_rate_b >= 0.8) and dt = '%(statdate)s';

insert overwrite table veda.dfqp_gameparty_relation_different_ip_his 
select x.game_id, x.mid_a, x.mid_b, x.play_together_total, x.dt_first, y.play_together_new, x.dt_last from 
(select game_id, mid_a, mid_b, sum(play_together_total) play_together_total, min(dt) dt_first, max(dt) dt_last 
from veda.dfqp_gameparty_relation_different_ip group by game_id, mid_a, mid_b) x join 
(select game_id, mid_a, mid_b, play_together_total play_together_new, dt from 
(select game_id, mid_a, mid_b, play_together_total, dt, rank() over (partition by game_id, mid_a, mid_b order by dt desc) num 
from veda.dfqp_gameparty_relation_different_ip) m where num = 1) y 
on x.game_id = y.game_id and x.mid_a = y.mid_a and x.mid_b = y.mid_b;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fangzuobi_user_relation_auto(sys.argv[1:])
a()
