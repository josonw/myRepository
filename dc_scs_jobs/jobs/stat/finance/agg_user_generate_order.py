#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

"""
此脚本的内容分两部分，后续可以考虑分离
analysis.user_payment_fct_order_bankrupt_part 的内容是更新analysis.user_payment_fct用的

analysis.user_generate_order_usernum 是一个独立的统计
"""

class agg_user_generate_order(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_generate_order_usernum
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fusernum                    bigint,
            fcnt                        bigint,
            fpay_scene                  varchar(100)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        # self.hq.debug = 1

        hql_list = []

        hql = """
        drop table if exists analysis.user_payment_fct_order_bankrupt_part_%(num_begin)s;

        create table if not exists analysis.user_payment_fct_order_bankrupt_part_%(num_begin)s
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fordercnt                   bigint,
            forderusercnt               bigint,
            fbankruptincome             decimal(38,2),
            fbankruptusercnt            bigint
        );

        """ % self.hql_dict
        hql_list.append( hql )


        # 付费场景，汇总数据
        # 如果付费场景有数据，以付费场景为准，否则以下的那表为准
        hql = """
        insert into table analysis.user_payment_fct_order_bankrupt_part_%(num_begin)s
        select '%(stat_date)s' fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            case when max(f2) = 1 then max(fordercnt2) else max(fordercnt) end as fordercnt,
            case when max(f2) = 1 then max(forderusercnt2) else max(forderusercnt) end as forderusercnt,
            max(fbankruptincome) as fbankruptincome, max(fbankruptusercnt) as fbankruptusercnt
        from
        (
            select fgamefsk,fplatformfsk,fversionfsk,fterminalfsk, 1 as f1, 0 as f2,
                count(1) fordercnt,
                count(distinct sitemid) forderusercnt,
                0 as fordercnt2,
                0 as forderusercnt2,
                0 fbankruptincome,
                0 fbankruptusercnt
            from stage.pay_order_info_stg a
            join analysis.paycenter_apps_dim p
              on a.appid = p.appid and a.sid = p.sid
            join analysis.bpid_platform_game_ver_map b
              on p.bpid = b.fbpid
            where a.dt = '%(stat_date)s'
            group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk


            union all

            select fgamefsk,fplatformfsk,fversionfsk,fterminalfsk, 0 as f1, 1 as f2,
                0 as fordercnt,
                0 as forderusercnt,
                count(1) fordercnt2,
                count(distinct fplatform_uid) forderusercnt2,
                sum( case when fbankrupt = 1 then fincome else 0 end ) fbankruptincome,
                count(distinct case when fincome>0 and fbankrupt>0 then fplatform_uid end) fbankruptusercnt
            from stage.user_payscene_mid npt
            join analysis.bpid_platform_game_ver_map b
              on npt.fbpid = b.fbpid
            where npt.dt = '%(stat_date)s'
            group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        ) t
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict
        hql_list.append(hql)

        hql = """
        insert overwrite table analysis.user_generate_order_usernum partition
        (dt = "%(stat_date)s")
        select '%(stat_date)s' fdate,
             b.fgamefsk,
             b.fplatformfsk,
             b.fversionfsk,
             b.fterminalfsk,
             count(distinct fplatform_uid) fusernum,
             count(1) fcnt,
             fpay_scene
        from stage.user_payscene_mid a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        where a.dt = '%(stat_date)s'
       group by b.fgamefsk,
                b.fplatformfsk,
                b.fversionfsk,
                b.fterminalfsk,
                fpay_scene
        """ % self.hql_dict
        hql_list.append( hql )

        res = self.exe_hql_list(hql_list)
        return res


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
    a = agg_user_generate_order(stat_date)
    a()
