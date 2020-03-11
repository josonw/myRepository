#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_payment_user_entrance_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_pay_entrance
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fentrance_id                bigint,
            fusercnt                    bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1

        # 付费用户，账号类型(入口)分布
        hql = """
        insert overwrite table analysis.user_pay_entrance partition
        (dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
             c.fgamefsk,
             c.fplatformfsk,
             c.fversionfsk,
             c.fterminalfsk,
             b.fentrance_id,
             count(distinct a.fuid) fusercnt
        from stage.user_pay_info a
        left join stage.user_dim b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
         and a.dt = '%(stat_date)s'
        join analysis.bpid_platform_game_ver_map c
          on a.fbpid = c.fbpid
       group by c.fgamefsk,
                c.fplatformfsk,
                c.fversionfsk,
                c.fterminalfsk,
                b.fentrance_id
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
    a = agg_payment_user_entrance_data(stat_date)
    a()
