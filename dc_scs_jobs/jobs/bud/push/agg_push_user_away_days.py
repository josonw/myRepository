#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class PushUserAwayDays(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS bud_dm.bud_push_user_away_days
            (
              fdate date,
              fpush_id int COMMENT '推送id',
              away_days int COMMENT '流失天数(推送前后登录时间差，NULL表示未召回)',
              persons int COMMENT '人数'
            )
            COMMENT '不同流失时长召回人数分布'
            partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            with pre_login_info as
            (
                select t1.fpush_id, t2.fuid, max(t3.dt) as pre_latest_login_date
                  from stage.push_config_stg t1
                 inner join dim.user_push_report t2
                    on t1.fpush_id = t2.fpush_id
                   and t2.faction = 1
                  left join dim.user_login_additional t3
                    on (t1.fbpid = t3.fbpid and t2.fuid = t3.fuid)
                 where t3.dt < to_date(t1.fbegin_time)
                   and date_add(t1.fbegin_time, 7) = '%(ld_1day_ago)s'
                   and t1.ftoken_id = 0
                 group by t1.fpush_id, t2.fuid
             ),
            post_login_info as
            (
                select t1.fpush_id, t2.fuid, min(t3.dt) as post_first_login_date
                  from stage.push_config_stg t1
                 inner join dim.user_push_report t2
                    on t1.fpush_id = t2.fpush_id
                   and t2.faction = 1
                  left join dim.user_login_additional t3
                    on (t1.fbpid = t3.fbpid and t2.fuid = t3.fuid)
                 where t3.dt >= to_date(t1.fbegin_time)
                   and t3.dt < date_add(t1.fbegin_time, 7)
                   and date_add(t1.fbegin_time, 7) = '%(ld_1day_ago)s'
                   and t1.ftoken_id = 0
                 group by t1.fpush_id, t2.fuid
             )
            insert overwrite table bud_dm.bud_push_user_away_days partition (dt='%(ld_1day_ago)s')
            select '%(ld_1day_ago)s' fdate,
                   v1.fpush_id,
                   datediff(v2.post_first_login_date, v1.pre_latest_login_date) as away_days,
                   count(distinct v1.fuid) as persons
              from pre_login_info v1
             inner join post_login_info v2
                on (v1.fpush_id = v2.fpush_id and v1.fuid = v2.fuid)
             group by v1.fpush_id, datediff(v2.post_first_login_date, v1.pre_latest_login_date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = PushUserAwayDays(sys.argv[1:])
a()
