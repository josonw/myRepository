#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_pay_channel_ver_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.pay_channel_ver_fct
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fm_fsk                      string,
            fversion_info               string,
            fusernum                    bigint,
            fmoney                      decimal(20,2),
            fcnt                        bigint,
            fnewusernum                 bigint,
            fnewmoney                   decimal(20,2),
            fnewcnt                     bigint
        )
        partitioned by (dt date)
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        use analysis;
        alter table pay_channel_ver_fct drop partition(dt = "%(stat_date)s")
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
          insert into table analysis.pay_channel_ver_fct partition(dt = "%(stat_date)s")
         select '%(stat_date)s' fdate,
           m.fgamefsk,
           m.fplatformfsk,
           m.fversionfsk,
           m.fterminalfsk,
           b.fm_fsk,
           coalesce(c.fversion_info,'weizhi007') fversion_info,
           count(distinct a.fplatform_uid) fusernum,
           sum(a.fcoins_num * a.frate) fmoney,
           count(distinct a.forder_id) fcnt,
           null fnewusernum,
           null fnewmoney,
           null fnewcnt
         from stage.payment_stream_stg a
         left join stage.user_generate_order_stg c
         on a.forder_id = c.forder_id
         and c.dt = '%(stat_date)s'
         join analysis.payment_channel_dim b
         on a.fm_id = b.fm_id
         join analysis.bpid_platform_game_ver_map m
         on a.fbpid = m.fbpid
         where a.dt = '%(stat_date)s'
         group by
           m.fgamefsk,
           m.fplatformfsk,
           m.fversionfsk,
           m.fterminalfsk,
           b.fm_fsk,
           coalesce(c.fversion_info,'weizhi007')

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        insert into table analysis.pay_channel_ver_fct partition(dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
              d.fgamefsk,
              d.fplatformfsk,
              d.fversionfsk,
              d.fterminalfsk,
              c.fm_fsk,
              coalesce(fversion_info,'weizhi007') fversion_info,
              null fusernum,
              null fmoney,
              null fcnt,
              count(distinct a.fplatform_uid) fnewusernum,
              sum(a.fcoins_num * a.frate) fnewmoney,
              count(distinct a.forder_id) fnewcnt
         from stage.payment_stream_stg a
          left join stage.user_generate_order_stg cc
         on a.forder_id = cc.forder_id
         and cc.dt = '%(stat_date)s'
         join stage.pay_user_mid b
           on a.fbpid = b.fbpid
          and a.fplatform_uid = b.fplatform_uid
          and b.dt='%(stat_date)s'
         join analysis.payment_channel_dim c
           on a.fm_id = c.fm_id
         join analysis.bpid_platform_game_ver_map d
           on a.fbpid = d.fbpid
        where a.dt = '%(stat_date)s'
        group by d.fgamefsk,
                 d.fplatformfsk,
                 d.fversionfsk,
                 d.fterminalfsk,
                 c.fm_fsk,
                 coalesce(fversion_info,'weizhi007')
        """ % self.hql_dict
        hql_list.append( hql )



        hql = """
        insert overwrite table analysis.pay_channel_ver_fct partition(dt = "%(stat_date)s")
        select
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fm_fsk,
            fversion_info,
            max(fusernum)       fusernum,
            max(fmoney)       fmoney,
            max(fcnt)         fcnt,
            max(fnewusernum)    fnewusernum,
            max(fnewmoney)    fnewmoney,
            max(fnewcnt)      fnewcnt
        from analysis.pay_channel_ver_fct
        where dt = "%(stat_date)s"
        group by
            fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            fm_fsk,
            fversion_info
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
    a = agg_pay_channel_ver_data(stat_date)
    a()
