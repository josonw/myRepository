#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserSignupLoginTime(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_signup_login_time
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid BIGINT COMMENT '用户ID',
              signup_time string COMMENT '注册时间',
              signup_to_now_days INT COMMENT '注册至今天数',
              latest_login_time string COMMENT '最后登录时间',
              latest_login_to_now_days INT COMMENT '最后登录距今天数'
            )
            COMMENT '用户注册和最后登录时间（所有平台）'
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
               where ranking = 1),
            
            --用户列表
            target_users as
             (select distinct fgamefsk, fgamename, fplatformfsk, fplatformname, fuid
                from signup_info
              union distinct
              select distinct fgamefsk, fgamename, fplatformfsk, fplatformname, fuid
                from veda.user_signup_login_time)
            
            --插入更新
            insert overwrite table veda.user_signup_login_time
            select v1.fgamefsk,
                   v1.fgamename,
                   v1.fplatformfsk,
                   v1.fplatformname,
                   v1.fuid,
                   if(v2.signup_time > t.signup_time, v2.signup_time, t.signup_time) as signup_time,
                   if(datediff(current_date(),if(v2.signup_time>t.signup_time,v2.signup_time,t.signup_time))>=9999,
                      9999,
                      datediff(current_date(),if(v2.signup_time>t.signup_time,v2.signup_time,t.signup_time))+1) as signup_to_now_days,
                   if(v3.latest_login_time > t.latest_login_time, v3.latest_login_time, t.latest_login_time) as latest_login_time,
                   datediff(current_date(),if(v3.latest_login_time>t.latest_login_time,v3.latest_login_time,t.latest_login_time)) as latest_login_to_now_days
              from target_users v1
              left join signup_info v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fgamename = v2.fgamename and
                   v1.fplatformfsk = v2.fplatformfsk and
                   v1.fplatformname = v2.fplatformname and v1.fuid = v2.fuid)
              left join login_info v3
                on (v1.fgamefsk = v3.fgamefsk and v1.fgamename = v3.fgamename and
                   v1.fplatformfsk = v3.fplatformfsk and
                   v1.fplatformname = v3.fplatformname and v1.fuid = v3.fuid)
              left join veda.user_signup_login_time t
                on (v1.fgamefsk = t.fgamefsk and v1.fgamename = t.fgamename and
                   v1.fplatformfsk = t.fplatformfsk and
                   v1.fplatformname = t.fplatformname and v1.fuid = t.fuid)
        """
        res = self.sql_exe(hql)
        return res


# 实例化执行
a = UserSignupLoginTime(sys.argv[1:])
a()