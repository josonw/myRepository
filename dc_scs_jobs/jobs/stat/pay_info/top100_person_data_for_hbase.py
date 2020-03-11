#! /usr/local/python272/bin/python
# coding: utf-8

"""
"""

import datetime
import os
import sys
sys.path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat


class top100_person_data_for_hbase(BaseStat):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.top100_gamecoin_category_hbase(
            fbpid string,
            fuid bigint,
            fdate date,
            fact_type tinyint,
            fact_id string,
            fcnt int comment '该类操作的次数',
            ftotal bigint comment '该类操作的总游戏币数量'
        )
        partitioned by (dt date);

        create table if not exists analysis.top100_props_category_hbase(
            fbpid string,
            fuid bigint,
            fdate date,
            fact_type tinyint,
            fprop_id string,
            fcnt int comment '该道具操作次数',
            ftotal bigint comment '该道具的操作数量'
        )
        partitioned by (dt date);

        create table if not exists analysis.top100_pay_stream_hbase(
            fbpid string,
            fuid bigint,
            fdate bigint,
            fpay_scene string comment '付费场景',
            fpm_name string comment '支付方式',
            fp_name string comment '购买产品名称',
            fcurrency_type int comment '币种类型',
            fmoney decimal(20, 2) comment '消费金额'
        )
        partitioned by (dt date);

        create table if not exists analysis.top100_signup_user_hbase(
            fbpid string,
            fuid bigint,
            fmnick string,
            fsignup_at string,
            fchannel_code string,
            fcountry string,
            fcity string,
            fm_dtype string,
            flanguage string,
            fgender tinyint,
            fage tinyint,
            fmobilesms string
        )
        partitioned by (dt date);

        create table if not exists analysis.top100_login_user_hbase(
            fbpid string,
            fuid bigint,
            flogin_at string,
            user_gamecoins bigint,
            user_bycoins bigint
        )
        partitioned by (dt date);

        create table if not exists analysis.top100_pay_user_hbase(
            fbpid string,
            fuid bigint,
            ffirst_pay_at string,
            flast_pay_at string,
            fpay_cnt int comment '付费次数',
            fpay_money bigint comment '付费金额',
            fmax_pay_num bigint comment '最高付费金额'
        )
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0
        res = 0

        args_dic = {
            "ld_begin": statDate
        }

        # 游戏币
        hql = """
        insert overwrite table analysis.top100_gamecoin_category_hbase partition(dt="%(ld_begin)s")
            select p.fbpid, p.fuid, to_date(lts_at), act_type, act_id, count(1), sum(abs(act_num))
              from stage.pb_gamecoins_stream_stg p
              join stage.pay_user_mid u
                on u.dt <= "%(ld_begin)s"
               and p.fbpid = u.fbpid
               and p.fuid = u.fuid
             where p.dt = "%(ld_begin)s"
             group by p.fbpid, p.fuid, to_date(lts_at), act_type, act_id
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 道具
        hql = """
        insert overwrite table analysis.top100_props_category_hbase partition(dt="%(ld_begin)s")
            select p.fbpid, p.fuid, to_date(lts_at), act_type, prop_id, count(1), sum(abs(act_num))
              from stage.pb_props_stream_stg p
              join stage.pay_user_mid u
                on u.dt <= "%(ld_begin)s"
               and p.fbpid = u.fbpid
               and p.fuid = u.fuid
             where p.dt = "%(ld_begin)s"
             group by p.fbpid, p.fuid, to_date(lts_at), act_type, prop_id
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 付费
        hql = """
        insert overwrite table analysis.top100_pay_stream_hbase partition(dt="%(ld_begin)s")
            select p.fbpid,
                   pu.fuid,
                   unix_timestamp(p.fdate),
                   u.fpay_scene,
                   p.fm_name,
                   p.fp_name,
                   u.fcurrency_type,
                   round(p.fcoins_num * p.frate, 2),
                   p.forder_id
              from stage.payment_stream_stg p
              left join stage.pay_user_mid pu
                on pu.dt <= "%(ld_begin)s"
               and p.fbpid = pu.fbpid
               and p.fplatform_uid = pu.fplatform_uid
              left join (select forder_id, fpay_scene, fpm_name, fcurrency_type,
                                row_number() over(partition by forder_id order by forder_at desc) rown
                           from stage.user_generate_order_stg
                          where dt="%(ld_begin)s") u
                on p.forder_id = u.forder_id
               and rown = 1
             where p.dt = "%(ld_begin)s"
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.top100_signup_user_hbase partition(dt="%(ld_begin)s")
            select p.fbpid, p.fuid, fmnick, fsignup_at, fchannel_code, fcountry, fcity, fm_dtype, flanguage, fgender, fage ,t.fmobilesms
              from pay_user_mid p
              join user_dim u
                on p.fbpid = u.fbpid
               and p.fuid = u.fuid
               and u.dt <= "%(ld_begin)s"
              join (
                    select fbpid, fuid, max(fmnick) fmnick, max(fchannel_code) fchannel_code,max(fmobilesms) fmobilesms
                      from stage.user_signup_stg
                     where dt <= "%(ld_begin)s"
                     group by fbpid, fuid
                   ) t
                on p.fbpid = t.fbpid
               and p.fuid = t.fuid
             where p.dt = "%(ld_begin)s"
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.top100_login_user_hbase partition(dt="%(ld_begin)s")
            select t.fbpid, t.fuid, flogin_at, user_gamecoins, user_bycoins
              from (
                    select fbpid, fuid, flogin_at, user_gamecoins, user_bycoins,
                           row_number() over(partition by fbpid, fuid order by flogin_at desc) rown
                      from stage.user_login_stg
                     where dt = "%(ld_begin)s"
                   ) t
              join stage.pay_user_mid p
                on p.dt <= "%(ld_begin)s"
               and t.fbpid = p.fbpid
               and t.fuid = p.fuid
             where rown = 1;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.top100_pay_user_hbase partition(dt="%(ld_begin)s")
            select p.fbpid, pu.fuid, pu.ffirst_pay_at, max(p.fdate), count(1),
                   round(sum(fcoins_num*frate),2)*100,
                   round(max(fcoins_num*frate),2)*100
              from stage.payment_stream_stg p
         left join stage.pay_user_mid pu
                on p.fbpid = pu.fbpid
               and p.fplatform_uid = pu.fplatform_uid
             where p.dt = "%(ld_begin)s"
             group by p.fbpid, pu.fuid, pu.ffirst_pay_at
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


# 愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(
        datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
else:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


# 生成统计实例
a = top100_person_data_for_hbase(statDate)
a()
