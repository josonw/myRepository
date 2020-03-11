#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

#计算首付用户首付次数，即首付当日付费次数
#因原数据表增加比较麻烦，故新增一数据表计算首付次数
#20170207新增

class agg_user_payment_first_fct(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_payment_first_fct
            (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fversionfsk         bigint,
            fterminalfsk        bigint,
            fpayusernum         bigint   comment '首付用户数',
            fpayusercnt         bigint   comment '首付当日总次数'
        )
        partitioned by (dt date);

        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        """ 重要部分，统计内容  """
        hql_list = []
        # self.hq.debug = 1


        hql = """
          insert overwrite table analysis.user_payment_first_fct  partition(dt = "%(stat_date)s")
        -- 首付用户数，首付次数
        select '%(stat_date)s' fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            count(distinct npt.fplatform_uid) fpayusernum,
            count(npt.fplatform_uid) fpayusercnt
        from stage.pay_user_mid npt
        join stage.payment_stream_stg a
          on a.fbpid = npt.fbpid
         and a.fplatform_uid = npt.fplatform_uid
         and a.dt = '%(stat_date)s'
        join analysis.bpid_platform_game_ver_map b
          on npt.fbpid = b.fbpid
        where npt.dt = '%(stat_date)s'
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
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
    a = agg_user_payment_first_fct(stat_date)
    a()
