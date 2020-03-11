#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserAbnormalRelatedUser(BaseStatModel):
    """异常登录用户同设备关联用户信息统计"""
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_abnormal_related_user(
              mid1 bigint COMMENT '异常MID',
              mid2 bigint COMMENT '同设备登录MID',
              device_imei string COMMENT '设备标识',
              device_type string COMMENT '设备型号',
              ip_list string COMMENT 'mid2登录过的IP',
              ip_num int COMMENT 'IP一致数量',
              app_version string COMMENT '版本号',
              login_days int COMMENT 'mid2在该设备登录天数',
              latest_login_time string COMMENT 'mid2最后登录时间',
              remain_silver_coin bigint COMMENT 'mid2剩余银币（含保险箱）',
              remain_gold_bar bigint COMMENT 'mid2剩余金条（含保险箱）'
              ) COMMENT '异常登录用户同设备关联信息表'
            partitioned by (dt string)
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            --异常登录用户
            with abnormal_users as
             (select distinct fuid, fdevice
                from veda.user_abnormal_login_info
               where dt = '%(statdate)s'
               order by fdevice),
            --异常登录用户关联用户
            related_users as
             (select v1.fuid as mid1, t2.fuid as mid2, v1.fdevice, t2.days
                from abnormal_users v1
               inner join veda.dfqp_user_device_relation_all t2
                  on v1.fdevice = t2.device
               order by mid2),
            --用户IP记录
            ip_history as
             (select v2.mid1, v2.mid2, v2.fdevice, t3.ip_list, v2.days
                from related_users v2
               inner join veda.dfqp_user_ip_info t3
                  on v2.mid2 = t3.fuid
               order by least(v2.mid1, v2.mid2), greatest(v2.mid1, v2.mid2)),
            --共同IP数量
            ip_relation as
             (select v3.mid1, v3.mid2, v3.fdevice, v3.ip_list, t4.ip_num, v3.days
                from ip_history v3
               inner join veda.dfqp_user_relation_info_by_ip t4
                  on least(v3.mid1, v3.mid2) = t4.mid1
                 and greatest(v3.mid1, v3.mid2) = t4.mid2
               order by v3.mid2)
            --结果汇总
            insert overwrite table veda.user_abnormal_related_user partition (dt = '%(statdate)s')
            select mid1,
                   mid2,
                   fdevice,
                   latest_device_type,
                   ip_list,
                   ip_num,
                   latest_app_version,
                   days,
                   latest_login_time,
                   total_silver_coin,
                   total_gold_bar
              from ip_relation v4
             inner join veda.dfqp_user_portrait t5
                on v4.mid2 = t5.mid
             order by mid1, mid2, fdevice
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化，运行
a = UserAbnormalRelatedUser(sys.argv[1:])
a()