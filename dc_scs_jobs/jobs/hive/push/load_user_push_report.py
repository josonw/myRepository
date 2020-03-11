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


class load_user_push_report(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_push_report
        (
            fbpid                   varchar(50)    comment 'bpid',
            fuid                    bigint         comment '用户id',
            faction                 bigint         comment '操作',
            ftoken                  varchar(520)   comment 'token',
            fpush_platform          varchar(50)    comment '推送平台'
        )
        partitioned by (fpush_id varchar(50) comment '推送id')
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容  """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.hq.debug = 0

        hql = """
        insert overwrite table dim.user_push_report
        partition(fpush_id)
        select t3.fbpid,t2.fuid,t1.faction,t2.ftoken,t2.fpush_platform,t3.fpush_id
        from (select distinct fclient_id,t.faction,t.fmsgid
                from stage.user_push_report_stg t
             ) t1
        join (select distinct fuid,ftoken,fpushid,t.fpush_platform
                from pushdb.push_user_parquet t
             ) t2
          on t1.fmsgid = t2.fpushid and t1.fclient_id = t2.ftoken
        join (select distinct fbpid,t.fpush_id,t.dt from stage.push_config_stg t where dt >= '%(ld_7day_ago)s' and dt <= '%(statdate)s') t3
          on t1.fmsgid = t3.fpush_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_user_push_report(sys.argv[1:])
a()
