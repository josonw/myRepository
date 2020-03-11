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


class agg_fzb_jl_user_relation_by_device(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        hql = """
		insert overwrite table veda.hwqp_user_device_relation
		partition(dt='%(statdate)s')
		select gamename, fuid, fm_imei, fm_dtype, fm_pixel, fterminal_type, concat_ws('◆', collect_set(fversion_info)) fversion_info from 
		(select distinct y.gamename, fuid, fm_imei, fm_dtype, fm_pixel, terminaltypename fterminal_type, fversion_info from stage.user_login_stg x 
		join veda.bpid_relation y on x.fbpid = y.fbpid
		where x.dt = '%(statdate)s') m 
		group by gamename, fuid, fm_imei, fm_dtype, fm_pixel, fterminal_type;
		
		insert overwrite table veda.hwqp_user_device_relation_all
		select gamename, fuid, device, sum(days) days from 
		(select distinct gamename, fuid, device, 1 as days from veda.hwqp_user_device_relation where dt = '%(statdate)s' 
		union all 
		select gamename, fuid, device, days from veda.hwqp_user_device_relation_all) m group by gamename, fuid, device;
		
		
		insert overwrite table veda.hwqp_user_device_relation_last_month
		select distinct gamename, fuid, device from veda.hwqp_user_device_relation where dt >= date_add('%(statdate)s', -15) and dt <= '%(statdate)s';

		
		insert overwrite table veda.hwqp_user_device_info 
		select gamename, fuid, concat_ws('◆', collect_set(device)) device_list, count(1) device_num from veda.hwqp_user_device_relation_all group by gamename, fuid;
		
		insert overwrite table veda.hwqp_user_relation_by_device 
		select x.gamename, x.fuid mid1, y.fuid mid2, concat(x.fuid, ':', y.fuid) relation_key, x.device, 
		concat('〖', x.fuid, '〗-【', x.device, '】-〖', y.fuid, '〗') relation, '%(statdate)s'  
		from 
		(select gamename, fuid, device from veda.hwqp_user_device_relation_all where device <> 'd41d8cd98f00b204e9800998ecf8427e') x 
		cross join 
		(select gamename, fuid, device from veda.hwqp_user_device_relation_all where device <> 'd41d8cd98f00b204e9800998ecf8427e') y 
		on x.device = y.device and x.gamename = y.gamename
		where x.fuid < y.fuid;
		
		insert overwrite table veda.hwqp_user_relation_info_by_device 
		select gamename, mid1, mid2, relation_key, concat_ws('◆', collect_set(device)) device_list, count(1) device_num from veda.hwqp_user_relation_by_device group by gamename, mid1, mid2, relation_key;
		
		insert overwrite table veda.hwqp_user_relation_user_info_by_device 
		select gamename, mid, concat_ws('◆', collect_set(mid2)) user_list, count(1) user_num from 
		(select distinct gamename, mid, cast(mid2 as string) mid2 from 
		(select gamename, mid1 mid, mid2 from veda.hwqp_user_relation_info_by_device 
		union all 
		select gamename, mid2 mid, mid1 mid2 from veda.hwqp_user_relation_info_by_device) n) m 
		group by gamename, m.mid;
		
		insert overwrite table veda.hwqp_user_relation_device_info_by_device 
		select gamename, mid, concat_ws('◆', collect_set(device)) device_list, count(1) device_num from 
		(select distinct gamename, mid, device from 
		(select gamename, mid1 mid, device from veda.hwqp_user_relation_by_device 
		union all 
		select gamename, mid2 mid, device from veda.hwqp_user_relation_by_device) n) m 
		group by gamename, m.mid;

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_fzb_jl_user_relation_by_device(sys.argv[1:])
a()
