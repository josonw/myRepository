#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class other_third_party_api_clear(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.third_party_cpa_clear_data
        (
          fbpid         varchar(50),
          fuid          bigint,
          fplatform_uid varchar(64),
          fchannel_id   varchar(64),
          fet_id        bigint,
          flts_at       string,
          fip           varchar(100),
          fcli_verinfo  varchar(100),
          fimei         varchar(128),
          fremark       varchar(256)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        # hql结尾不要带分号';'
        hql_list = []

        hql = """
        insert overwrite table analysis.third_party_cpa_clear_data partition (dt = "%(ld_begin)s")
        select fbpid,
               fuid,
               fplatform_uid,
               fchannel_id,
               fet_id,
               flts_at,
               fip,
               fcli_verinfo,
               fimei,
               fremark
          from stage.fd_user_channel_stg
         where dt = '%(ld_begin)s'
           and fet_id in (2, 4)
           and instr(fremark, 'first=1') > 0
        """% self.hql_dict
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
    a = other_third_party_api_clear(stat_date)
    a()
