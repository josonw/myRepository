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


class agg_fzb_hwqp_user_relation_by_ip(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """

insert overwrite table veda.hwqp_user_ip_relation
partition(dt='%(statdate)s')
select distinct gamename, fuid, fip, fip_country, fip_province, fip_city from stage.user_login_stg x 
join veda.bpid_relation y on x.fbpid = y.fbpid
where dt = '%(statdate)s';

insert overwrite table veda.hwqp_user_ip_relation_all
select gamename, fuid, ip, ip_country, ip_province, ip_city, sum(days) days from 
(select gamename, fuid, ip, ip_country, ip_province, ip_city,  1 as days from veda.hwqp_user_ip_relation where dt = '%(statdate)s' 
union all 
select gamename, fuid, ip, ip_country, ip_province, ip_city, days from veda.hwqp_user_ip_relation_all) m group by gamename, fuid, ip, ip_country, ip_province, ip_city;

insert overwrite table veda.hwqp_user_ip_relation_last_week
select distinct gamename, fuid, ip from veda.hwqp_user_ip_relation where dt >= date_add('%(statdate)s', -6) and dt <= '%(statdate)s';

insert overwrite table veda.hwqp_user_ip_info 
select gamename, fuid, concat_ws('◆', collect_set(ip)) ip_list, count(1) ip_num from veda.hwqp_user_ip_relation_all group by gamename, fuid;

insert overwrite table veda.hwqp_user_relation_by_ip 
select x.gamename, x.fuid mid1, y.fuid mid2, concat(x.fuid, ':', y.fuid) relation_key, x.ip, 
concat('〖', x.fuid, '〗-【', x.ip, '】-〖', y.fuid, '〗') relation, '%(statdate)s'  
from 
(select gamename, fuid, ip from veda.hwqp_user_ip_relation_last_week) x 
cross join 
(select gamename, fuid, ip from veda.hwqp_user_ip_relation_last_week) y 
on x.ip = y.ip and x.gamename = y.gamename 
where x.fuid < y.fuid;

insert overwrite table veda.hwqp_user_relation_info_by_ip 
select gamename, mid1, mid2, relation_key, concat_ws('◆', collect_set(ip))ip_list, count(1) ip_num from veda.hwqp_user_relation_by_ip group by gamename,  mid1, mid2, relation_key;

insert overwrite table veda.hwqp_user_relation_user_info_by_ip 
select gamename, mid, concat_ws('◆', collect_set(mid2)) user_list, count(1) user_num from 
(select distinct gamename, mid, cast(mid2 as string) mid2 from 
(select gamename, mid1 mid, mid2 from veda.hwqp_user_relation_info_by_ip 
union all 
select gamename, mid2 mid, mid1 mid2 from veda.hwqp_user_relation_info_by_ip) n) m 
group by gamename, m.mid;

insert overwrite table veda.hwqp_user_relation_ip_info_by_ip 
select gamename, mid, concat_ws('◆', collect_set(ip)) ipe_list, count(1) ipe_num from 
(select distinct gamename, mid, ip from 
(select gamename, mid1 mid, ip from veda.hwqp_user_relation_by_ip 
union all 
select gamename, mid2 mid, ip from veda.hwqp_user_relation_by_ip) n) m 
group by gamename, m.mid;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_hwqp_user_relation_by_ip(sys.argv[1:])
a()
