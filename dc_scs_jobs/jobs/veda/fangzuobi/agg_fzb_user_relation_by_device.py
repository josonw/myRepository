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
insert overwrite table veda.dfqp_user_device_relation
partition(dt='%(statdate)s')
select fuid, fm_imei, fm_dtype, fm_pixel, fterminal_type, concat_ws('◆', collect_set(fversion_info)) fversion_info from 
(select distinct fuid, fm_imei, fm_dtype, fm_pixel, fterminaltypename fterminal_type, fversion_info from stage_dfqp.user_login_stg x 
where fgamename = '地方棋牌' and dt = '%(statdate)s') m 
group by fuid, fm_imei, fm_dtype, fm_pixel, fterminal_type;

insert overwrite table veda.dfqp_user_device_relation_all
select fuid, device, sum(days) days from 
(select distinct fuid, device, 1 as days from veda.dfqp_user_device_relation where dt = '%(statdate)s' 
union all 
select fuid, device, days from veda.dfqp_user_device_relation_all) m group by fuid, device;

insert overwrite table veda.dfqp_user_device_info 
select fuid, concat_ws('◆', collect_set(device)) device_list, count(1) device_num from veda.dfqp_user_device_relation_all group by fuid;

insert overwrite table veda.dfqp_user_relation_by_device 
select x.fuid mid1, y.fuid mid2, concat(x.fuid, ':', y.fuid) relation_key, x.device, 
concat('〖', x.fuid, '〗-【', x.device, '】-〖', y.fuid, '〗') relation, '%(statdate)s'  
from 
(select fuid, device from veda.dfqp_user_device_relation_all) x 
cross join 
(select fuid, device from veda.dfqp_user_device_relation_all) y 
on x.device = y.device 
where x.fuid < y.fuid;

insert overwrite table veda.dfqp_user_relation_info_by_device 
select mid1, mid2, relation_key, concat_ws('◆', collect_set(device)) device_list, count(1) device_num from veda.dfqp_user_relation_by_device group by mid1, mid2, relation_key;

insert overwrite table veda.dfqp_user_relation_user_info_by_device 
select mid, concat_ws('◆', collect_set(mid2)) user_list, count(1) user_num from 
(select distinct mid, cast(mid2 as string) mid2 from 
(select mid1 mid, mid2 from veda.dfqp_user_relation_info_by_device 
union all 
select mid2 mid, mid1 mid2 from veda.dfqp_user_relation_info_by_device) n) m 
group by m.mid;

insert overwrite table veda.dfqp_user_relation_device_info_by_device 
select mid, concat_ws('◆', collect_set(device)) device_list, count(1) device_num from 
(select distinct mid, device from 
(select mid1 mid, device from veda.dfqp_user_relation_by_device 
union all 
select mid2 mid, device from veda.dfqp_user_relation_by_device) n) m 
group by m.mid;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_user_relation_by_device(sys.argv[1:])
a()
