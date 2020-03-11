#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class promotion_channel_day_loss(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        hql = """
        drop table if exists stage.channel_loss_user_mid_%(num_begin)s;

        create table if not exists stage.channel_loss_user_mid_%(num_begin)s
        (
            fbpid string,
            fchannel_id string,
            fuid bigint,
            fflag bigint
        )
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert into table stage.channel_loss_user_mid_%(num_begin)s
          select fbpid, fchannel_id, fuid, 30
            from (select fbpid,
                         fdate,
                         fchannel_id,
                         fuid,
                         max(flag1) flag1,
                         max(flag2) flag2
                    from (select fbpid, dt fdate, fchannel_id, fuid, 1 flag1, 0 flag2
                            from stage.fd_user_channel_stg a
                           where a.dt >= date_add('%(ld_begin)s', -59)
                             and a.dt < date_add('%(ld_begin)s', -29)
                             and a.fet_id in (3, 4, 5)
                          union all
                          select fbpid, dt fdate, fchannel_id, fuid, 0 flag1, 1 flag2
                            from stage.fd_user_channel_stg a
                           where a.dt >= date_add('%(ld_begin)s', -29)
                             and a.dt < '%(ld_end)s'
                             and a.fet_id = 2) a
                   group by fbpid, fdate, fchannel_id, fuid) a
           where flag1 = 1
             and flag2 = 0;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
       insert into table stage.channel_loss_user_mid_%(num_begin)s
      select fbpid, fchannel_id, fuid, 14
        from (select fbpid,
                     fdate,
                     fchannel_id,
                     fuid,
                     max(flag1) flag1,
                     max(flag2) flag2
                from (select fbpid, dt fdate, fchannel_id, fuid, 1 flag1, 0 flag2
                        from stage.fd_user_channel_stg a
                       where a.dt >= date_add('%(ld_begin)s', -27)
                         and a.dt < date_add('%(ld_begin)s', -13)
                         and a.fet_id in (3, 4, 5)
                      union all
                      select fbpid, dt fdate, fchannel_id, fuid, 0 flag1, 1 flag2
                        from stage.fd_user_channel_stg a
                       where a.dt >= date_add('%(ld_begin)s', -13)
                         and a.dt < '%(ld_end)s'
                         and a.fet_id = 2) a
               group by fbpid, fdate, fchannel_id, fuid) a
       where flag1 = 1
         and flag2 = 0;
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert into table stage.channel_loss_user_mid_%(num_begin)s
      select fbpid, fchannel_id, fuid, 7
        from (select fbpid,
                     fdate,
                     fchannel_id,
                     fuid,
                     max(flag1) flag1,
                     max(flag2) flag2
                from (select fbpid, dt fdate, fchannel_id, fuid, 1 flag1, 0 flag2
                        from stage.fd_user_channel_stg a
                       where a.dt >= date_add('%(ld_begin)s', -13)
                         and a.dt < date_add('%(ld_begin)s', -6)
                         and a.fet_id in (3, 4, 5)
                      union all
                      select fbpid, dt fdate, fchannel_id, fuid, 0 flag1, 1 flag2
                        from stage.fd_user_channel_stg a
                       where a.dt >= date_add('%(ld_begin)s', -6)
                         and a.dt < '%(ld_end)s'
                         and a.fet_id = 2) a
               group by fbpid, fdate, fchannel_id, fuid) a
       where flag1 = 1
         and flag2 = 0;
        """ % self.hql_dict
        hql_list.append( hql )


        hql ="""
        -- table analysis.user_channel_fct_day_loss_part
        drop table if exists analysis.user_channel_fct_day_loss_part_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_day_loss_part_%(num_begin)s as
        select '%(ld_begin)s' fdate,
                   fgamefsk,
                   fplatformfsk,
                   fversion_old fversionfsk,
                   fterminalfsk,
                   c.fpkg_channel_id fchannel_id,
                   count( case when fflag = 7 then a.fuid end ) f7dayuserloss,
                   count( case when fflag = 30 then a.fuid end ) f30dayuserloss
              from stage.channel_loss_user_mid_%(num_begin)s a
              join dim.bpid_map b
                   on a.fbpid = b.fbpid
              join analysis.dc_channel_package c
                on a.fchannel_id = c.fpkg_id
             group by fgamefsk, fplatformfsk, fversion_old, fterminalfsk, c.fpkg_channel_id

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists stage.channel_loss_user_mid_%(num_begin)s;
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
    a = promotion_channel_day_loss(stat_date)
    a()
