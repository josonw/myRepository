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


class agg_fzb_user_relation_by_device(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """

drop table if exists veda.tmp_dfqp_fzb_user_gamecoins_rank;
create table veda.tmp_dfqp_fzb_user_gamecoins_rank as 
select mid, total_gamecoins from veda.dfqp_user_portrait_history where dt = '%(statdate)s' order by total_gamecoins desc limit 1000;


insert overwrite table veda.dfqp_fzb_user_gamecoins_rank 
partition(dt='%(statdate)s')
select mid, total_gamecoins, Row_Number() over (order by total_gamecoins desc) rank 
from veda.tmp_dfqp_fzb_user_gamecoins_rank order by total_gamecoins desc limit 1000;


drop table if exists veda.tmp_user_ralation_by_device;
create table veda.tmp_user_ralation_by_device as
select mid1 mid, mid2 relation_user from veda.dfqp_user_relation_info_by_device 
where mid1 in (select mid from veda.dfqp_fzb_user_gamecoins_rank where dt = '%(statdate)s' and rank <= 200)
union all 
select mid2 mid, mid1 relation_user from veda.dfqp_user_relation_info_by_device 
where mid2 in (select mid from veda.dfqp_fzb_user_gamecoins_rank where dt = '%(statdate)s' and rank <= 200);

drop table if exists veda.tmp_user_relation_gamecoins;
create table veda.tmp_user_relation_gamecoins as 
select x.mid, x.wincoins, y.lostcoins from 
(select x1.fwinner mid, sum(x1.fwincoins) wincoins from veda.dfqp_user_gameparty_relation x1 join veda.tmp_user_ralation_by_device x2 on x1.fwinner = x2.mid and x1.floser = x2.relation_user 
group by x1.fwinner) x 
left join 
(select x1.floser mid, sum(x1.flostcoins) lostcoins from veda.dfqp_user_gameparty_relation x1 join veda.tmp_user_ralation_by_device x2 on x1.floser = x2.mid and x1.fwinner = x2.relation_user 
group by x1.floser) y on x.mid = y.mid;

drop table if exists veda.tmp_user_relation_gamecoins_day;
create table veda.tmp_user_relation_gamecoins_day as 
select x.mid, x.wincoins, y.lostcoins from 
(select x1.fwinner mid, sum(x1.fwincoins) wincoins from veda.dfqp_user_gameparty_relation x1 join veda.tmp_user_ralation_by_device x2 on x1.fwinner = x2.mid and x1.floser = x2.relation_user 
where x1.dt = '%(statdate)s'  
group by x1.fwinner) x 
left join 
(select x1.floser mid, sum(x1.flostcoins) lostcoins from veda.dfqp_user_gameparty_relation x1 join veda.tmp_user_ralation_by_device x2 on x1.floser = x2.mid and x1.fwinner = x2.relation_user 
where x1.dt = '%(statdate)s' 
group by x1.floser) y on x.mid = y.mid;

insert overwrite table veda.dfqp_fzb_user_gamecoins_ranking_info
partition(dt = '%(statdate)s')
select x.mid, x.rank, 
case when y.rank is not null and x.rank - y.rank > 0 then  concat('↓', cast(x.rank - y.rank as string))
when y.rank is not null and x.rank - y.rank < 0 then  concat('↑', cast(abs(x.rank - y.rank) as string))
when y.rank is null then 'new' 
when y.rank is not null and x.rank - y.rank = 0 then '-' end ranking_change , x.gamecoins gc_now, 
y.gamecoins gc_last, x.gamecoins - y.gamecoins gc_change, z.wincoins wincoins_his, z.lostcoins losecoins_his, abs(z.wincoins) + abs(z.lostcoins) total_coins_his, 
m.wincoins wincoins_day, m.lostcoins losecoins_day, abs(m.wincoins) + abs(m.lostcoins) total_coins_day, o.user_num, n.device_num 
from 
(select mid, gamecoins, rank from veda.dfqp_fzb_user_gamecoins_rank where dt = '%(statdate)s') x 
left join 
(select mid, gamecoins, rank from veda.dfqp_fzb_user_gamecoins_rank where dt = '%(ld_1day_ago)s') y 
on x.mid = y.mid 
left join veda.tmp_user_relation_gamecoins z on x.mid = z.mid 
left join veda.tmp_user_relation_gamecoins_day m on x.mid = m.mid 
left join veda.dfqp_user_relation_device_info_by_device n on x.mid = n.fuid 
left join veda.dfqp_user_relation_user_info_by_device o on x.mid = o.fuid
order by x.rank limit 200;


        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_user_relation_by_device(sys.argv[1:])
a()
