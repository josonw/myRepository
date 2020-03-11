#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class user_push_stg(BaseStat):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists stage.user_push_orc
        (
        fbpid                   varchar(50),
        fappid                  varchar(50),
        fuid                    bigint,
        ftoken                  varchar(100),
        flts_at                 string,
        flogin_at               string,
        fsignup_at              string,
        fis_open                int,
        fversion_info           varchar(100),
        fuser_gamecoins         bigint,
        fm_dtype                varchar(100),
        fentrance_id            int,
        fis_paid                int,
        fvip_level              int,
        fgender                 int,
        fchannel_code           varchar(100),
        fparty_num              bigint,
        fpay_num                bigint
        )
        partitioned by (dt string)
        clustered by (fuid) sorted by (fbpid) into 256 buckets
        stored as orc
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": statDate
        }

        hql = """
        use stage;
        alter table user_push_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/user_push/%(ld_begin)s';
        alter table user_push_stg add if not exists partition(dt='%(ld_end)s') location '/dw/stage/user_push/%(ld_end)s';
        insert overwrite table stage.user_push_orc
        partition( dt='%(ld_begin)s' )
        select
        fbpid,
        fappid,
        fuid,
        ftoken,
        flts_at,
        flogin_at,
        fsignup_at,
        fis_open,
        fversion_info,
        fuser_gamecoins,
        fm_dtype,
        fentrance_id,
        fis_paid,
        fvip_level,
        fgender,
        fchannel_code,
        fparty_num,
        fpay_num
        from stage.user_push_stg
        where dt='%(ld_begin)s'
        distribute by fbpid
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        return res


# 愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    # 没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
else:
    # 从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]

# 生成统计实例
import_job = user_push_stg(statDate)
import_job()
