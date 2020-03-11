#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经停止计算
class market_channel_device_type(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.channel_market_device_type_fct
        (
        fdate date,
        fgamefsk bigint,
        fplatformfsk bigint,
        fversionfsk bigint,
        fterminalfsk bigint,
        fchannel_id varchar(64),
        fdevice_type varchar(100),
        fdtype_num bigint
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

        insert overwrite table analysis.channel_market_device_type_fct partition
        (dt = "%(ld_begin)s")
        select '%(ld_begin)s' fdate,
               fgamefsk,
               fplatformfsk,
               fversion_old fversionfsk,
               fterminalfsk,
               fchannel_id,
               fdevice_type,
               count(fdevice_type) fdtype_num
          from stage.fd_user_channel_stg a
          join dim.bpid_map b
          on a.fbpid = b.fbpid
         where a.dt = '%(ld_begin)s'
           and fet_id = 3
           group by  fgamefsk,
               fplatformfsk,
               fversion_old,
               fterminalfsk,
               fchannel_id,
               fdevice_type;


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
    a = market_channel_device_type(stat_date)
    a()
