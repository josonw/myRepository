#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class user_order_stg(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists stage.user_order_stg
        (
        fbpid                    varchar(50),
        fuid                     bigint,
        fplatform_uid            varchar(50),
        forder_at                string,
        forder_id                varchar(100),
        fpay_mode                bigint,
        fpay_cnt                 bigint,
        fitem_category           bigint,
        fitem_num                bigint,
        fitem_id                 bigint,
        fitem_desc               varchar(200),
        fplatform_order_id       varchar(100),
        fbank_order_id           varchar(100),
        fcurrency                varchar(50),
        fcurrency_num            bigint,
        ferror_code              bigint,
        ferror_message           varchar(100),
        ffee                     decimal(20,2),
        fbalance                 bigint,
        fb_order_id              bigint,
        fpay_rate                decimal(20,2),
        fversion_info            varchar(50),
        fchannel_code            varchar(100)
        )
        partitioned by (dt date)
        location '/dw/stage/user_order/';
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        self.hql_dict = {'stat_date':self.stat_date}
        hql_list = []

        hql = """
        use stage;
        alter table user_order_stg add if not exists partition(dt='%(stat_date)s')
        location '/dw/stage/user_order/%(stat_date)s'
        """ % self.hql_dict
        result = self.exe_hql(hql)
        if result != 0:
            return result


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = user_order_stg(stat_date)
    a()