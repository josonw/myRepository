#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class load_user_pay_payscene(BaseStat):
    """ 付费用户场景中间表 """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_pay_payscene
        (
        fbpid                               string,
        fuid                                bigint,
        fplatform_uid                       string,
        fgame_id                            bigint,
        fchannel_code                       bigint,
        fentrance_id                        string,
        forder_at                           date,
        forder_id                           string,
        fcurrency_type                      bigint,
        fcurrency_num                       bigint,
        fitem_category                      bigint,
        fitem_id                            bigint,
        fitem_num                           bigint,
        fbalance                            bigint,
        fgrade                              bigint,
        fgameparty_pname                    string,
        fgameparty_subname                  string,
        fgameparty_anto                     bigint,
        fbankrupt                           bigint,
        fpay_scene                          string,
        fip                                 string,
        fplatform_order_id                  string,
        fb_order_id                         bigint,
        ffee                                decimal(20,2),
        fpm_name                            string,
        fincome                             decimal(20,6)
        )partitioned by (dt date);

        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        query = {}
        query.update(sql_const.const_dict())
        query.update(PublicFunc.date_define(self.stat_date))

        hql = """
        insert overwrite table dim.user_pay_payscene partition (dt = "%(ld_daybegin)s")
        select  a.fbpid fbpid,
                a.fuid,
                a.fplatform_uid,
                coalesce(b.fgame_id,cast(0 as bigint)) fgame_id,
                coalesce(ci.ftrader_id,%(null_int_report)d) fchannel_code,
                fentrance_id,
                a.fdate forder_at,
                a.forder_id,
                fcurrency_type,
                fcurrency_num,
                fitem_category,
                fitem_id,
                fitem_num,
                fbalance,
                fgrade,
                coalesce(fgameparty_pname,'%(null_str_report)s') fgameparty_pname,
                coalesce(fgameparty_subname,'%(null_str_report)s') fgameparty_subname,
                coalesce(fgameparty_anto ,%(null_int_report)d) fgameparty_anto,
                coalesce(cast (fbankrupt as bigint) ,%(null_int_report)d) fbankrupt,
                coalesce(fpay_scene,'%(null_str_report)s') fpay_scene,
                b.fip,
                fplatform_order_id,
                fb_order_id,
                ffee,
                coalesce(a.fm_name , c.fm_name ) fpm_name,
                round(c.frate * c.fcoins_num, 6) fincome
          from stage.payment_stream_all_stg a
          left join stage.user_generate_order_stg b
            on a.forder_id = b.forder_id
           and b.dt = '%(ld_daybegin)s'
          left join stage.payment_stream_stg c
            on a.forder_id = c.forder_id
           and c.dt = '%(ld_daybegin)s'
          left join analysis.marketing_channel_pkg_info ci
            on b.fchannel_code = ci.fid
         where a.dt = '%(ld_daybegin)s'

        """ % query
        hql_list.append(hql)

        res = self.exe_hql_list(hql_list)
        return res


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        # 没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        # 从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    # 生成统计实例
    a = load_user_pay_payscene(stat_date)
    a()
