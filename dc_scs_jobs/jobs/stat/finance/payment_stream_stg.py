#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class payment_stream_stg(BaseStat):

    def create_tab(self):
        hql = """
        create external table if not exists stage.payment_stream_stg
        (
        fbpid               varchar(50),
        fdate               string,
        fplatform_uid       varchar(50),
        fis_platform_uid    tinyint,
        forder_id           varchar(255),
        fcoins_num          decimal(20,2),
        frate               decimal(20,7),
        fm_id               varchar(256),
        fm_name             varchar(256),
        fp_id               varchar(256),
        fp_name             varchar(256),
        fchannel_id         varchar(64),
        fimei               varchar(64),
        fsucc_time          string,
        fcallback_time      string,
        fp_type             bigint,
        fp_num              bigint
        )
        partitioned by (dt date)
        location '/dw/stage/pay_stream/';
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        self.hql_dict = {'stat_date':self.stat_date}
        hql_list = []

        hql = """
        use stage;
        alter table payment_stream_stg add if not exists partition(dt='%(stat_date)s')
        location '/dw/stage/payment_stream_stg/%(stat_date)s'
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
    a = payment_stream_stg(stat_date)
    a()