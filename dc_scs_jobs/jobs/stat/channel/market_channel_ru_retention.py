#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class market_channel_ru_retention(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求,60天
        create table if not exists analysis.user_channel_ru_retention
        (
        fdate            date,
        fgamefsk         bigint,
        fplatformfsk     bigint,
        fversionfsk      bigint,
        fterminalfsk     bigint,
        fchannel_id      varchar(100),
        fdru_day         int,
        fdru_num_type    int,
        fdru_num         bigint,
        fdru_d_num       bigint,
        fdru_money       decimal(20,2)
        )
        partitioned by (dt date)"""
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        #加上当天的分区
        hql = """
        use analysis;
        alter table user_channel_ru_retention add if not exists partition (dt='%(ld_begin)s');
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists stage.user_channel_ru_retention_tmp1_%(num_begin)s;

        create table if not exists stage.user_channel_ru_retention_tmp1_%(num_begin)s as
        select c.dt fdate,
               fgamefsk,
               fplatformfsk,
               fversion_old fversionfsk,
               fterminalfsk,
               fchannel_id,
               datediff('%(ld_begin)s', c.dt) fdru_day,
               1 fdru_num_type,
               count(distinct c.fuid) fdru_num,
               count(distinct c.fudid) fdru_d_num,
               cast(null as decimal(20,2)) fdru_money
          from stage.active_user_mid a
          join stage.channel_market_reg_mid c
            on a.fbpid = c.fbpid
           and a.fuid = c.fuid
           and c.dt >= date_add('%(ld_begin)s', -45)
           and c.dt < '%(ld_end)s'
          join dim.bpid_map d
            on a.fbpid = d.fbpid
         where a.dt = '%(ld_begin)s'
         group by c.dt,
                  fgamefsk,
                  fplatformfsk,
                  fversion_old,
                  fterminalfsk,
                  fchannel_id,
                  datediff('%(ld_begin)s', c.dt)
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists stage.user_channel_ru_retention_tmp2_%(num_begin)s;

        create table if not exists stage.user_channel_ru_retention_tmp2_%(num_begin)s as
        select c.dt fdate,
               fgamefsk,
               fplatformfsk,
               fversion_old fversionfsk,
               fterminalfsk,
               fchannel_id,
               datediff('%(ld_begin)s', c.dt) fdru_day,
               2 fdru_num_type,
               count(distinct c.fuid) fdru_num,
               count(distinct c.fudid) fdru_d_num,
               cast(null as decimal(20,2)) fdru_money
          from stage.user_pay_info a
          join stage.channel_market_reg_mid c
            on a.fbpid = c.fbpid
           and a.fuid = c.fuid
           and c.dt >= date_add('%(ld_begin)s', -45)
           and c.dt < '%(ld_end)s'
          join dim.bpid_map d
            on a.fbpid = d.fbpid
         where a.dt = '%(ld_begin)s'
         group by c.dt,
                  fgamefsk,
                  fplatformfsk,
                  fversion_old,
                  fterminalfsk,
                  fchannel_id,
                  datediff('%(ld_begin)s', c.dt)
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        insert overwrite table analysis.user_channel_ru_retention partition
          (dt)
        select  coalesce(b.fdate, a.fdate),
                coalesce(b.fgamefsk, a.fgamefsk),
                coalesce(b.fplatformfsk, a.fplatformfsk),
                coalesce(b.fversionfsk, a.fversionfsk),
                coalesce(b.fterminalfsk, a.fterminalfsk),
                coalesce(b.fchannel_id, a.fchannel_id),
                coalesce(b.fdru_day, a.fdru_day),
                coalesce(b.fdru_num_type, a.fdru_num_type),
                coalesce(b.fdru_num, a.fdru_num),
                coalesce(b.fdru_d_num, a.fdru_d_num),
                coalesce(b.fdru_money, a.fdru_money),
                coalesce(b.fdate, a.fdate) dt
          from (
            select * from analysis.user_channel_ru_retention
            where dt >= date_add('%(ld_begin)s', -45)
              and dt < '%(ld_end)s' ) a
          full outer join (
            select * from stage.user_channel_ru_retention_tmp1_%(num_begin)s
            union all
            select * from stage.user_channel_ru_retention_tmp2_%(num_begin)s
            ) b
             on a.fdate         = b.fdate
            and a.fgamefsk      = b.fgamefsk
            and a.fplatformfsk  = b.fplatformfsk
            and a.fversionfsk   = b.fversionfsk
            and a.fchannel_id   = b.fchannel_id
            and a.fdru_day      = b.fdru_day
            and a.fdru_num_type = b.fdru_num_type
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists stage.user_channel_ru_retention_tmp1_%(num_begin)s;

        drop table if exists stage.user_channel_ru_retention_tmp2_%(num_begin)s;
        """ % self.hql_dict
        hql_list.append( hql )



        hql = """
        insert overwrite table analysis.user_channel_ru_retention partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        fdru_day,
        fdru_num_type,
        fdru_num,
        fdru_d_num,
        fdru_money
        from analysis.user_channel_ru_retention
        where dt >= date_add('%(ld_begin)s', -45)
          and dt < '%(ld_end)s'
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
    a = market_channel_ru_retention(stat_date)
    a()
