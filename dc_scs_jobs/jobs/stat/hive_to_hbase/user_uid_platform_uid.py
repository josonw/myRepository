#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class user_uid_platform_uid(BaseStat):

    def create_tab(self):
        hql = """
        -- fbpid:fuid 作为key
        create external table if not exists analysis.user_uid_platform_uid_hbase
         (key string,
         fplatform_uid string
        )
        stored by 'org.apache.hadoop.hive.hbase.hbasestoragehandler'
        with serdeproperties
        ("hbase.columns.mapping" = ":key, f:fplatform_uid")
        tblproperties ("hbase.table.name" = "user_uid_platform_uid");
        """
        # result = self.exe_hql(hql)
        # if result != 0:
        #     return result

    def stat(self):
        hql_list = []

        hql = """
        insert into analysis.user_uid_platform_uid_hbase(key, fplatform_uid)
        select concat(fbpid, ':', fuid) key, fplatform_uid from stage.user_async_stg where dt = '%(ld_begin)s'
        and fuid is not null and fplatform_uid is not null
        """ % self.hql_dict
        hql_list.append( hql )


        result = self.exe_hql_list(hql_list)
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
    a = user_uid_platform_uid(stat_date)
    a()
