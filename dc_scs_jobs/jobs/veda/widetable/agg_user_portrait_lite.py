#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserPortraitLite(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_portrait_lite(
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid BIGINT COMMENT '用户ID',
              signup_time string COMMENT '注册时间',
              latest_login_time string COMMENT '最后登录时间'
            )
            COMMENT '用户精简画像表（所有平台）'
            PARTITIONED BY (dt STRING)
            STORED AS ORC
        """
        res = self.sql_exe(hql)
        return res

    def stat(self):
        hql = """
            --注册信息
            with signup_info as
             (select fgamefsk, fgamename, fplatformfsk, fplatformname, fuid, signup_time
                from (select fgamefsk,
                             fgamename,
                             fplatformfsk,
                             fplatformname,
                             fuid,
                             fsignup_at as signup_time,
                             row_number() over(partition by fgamefsk, fplatformfsk, fuid order by fsignup_at desc nulls last) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_signup_stg t2
                          on t1.fbpid = t2.fbpid
                       where dt = '%(statdate)s') m1
               where ranking = 1),

            --登录信息
            login_info as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     latest_login_time
                from (select fgamefsk,
                             fgamename,
                             fplatformfsk,
                             fplatformname,
                             fuid,
                             flogin_at as latest_login_time,
                             row_number() over(partition by fgamefsk, fplatformfsk, fuid order by flogin_at desc nulls last) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_login_stg t2
                          on t1.fbpid = t2.fbpid
                       where dt = '%(statdate)s') m2
               where ranking = 1)

            --插入更新
            insert overwrite table veda.user_portrait_lite partition (dt = '%(statdate)s')
            select v1.fgamefsk,
                   v1.fgamename,
                   v1.fplatformfsk,
                   v1.fplatformname,
                   v1.fuid,
                   v2.signup_time,
                   v1.latest_login_time
              from login_info v1
              left join signup_info v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fplatformfsk = v2.fplatformfsk and v1.fuid = v2.fuid)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserPortraitLite(sys.argv[1:])
a()