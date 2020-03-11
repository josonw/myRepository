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


class agg_user_channel_loss_user_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_channel_loss_user_info (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fchannel_id            bigint   comment '渠道id',
               f7dayuserloss          bigint   comment '7日流失',
               f30dayuserloss         bigint   comment '30日流失'
               )comment '渠道流失'
               partitioned by(dt date)
        location '/dw/dcnew/user_channel_loss_user_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fchannel_id'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """--新增用户
                  drop table if exists work.user_channel_loss_user_info_tmp_a_%(statdatenum)s;
                create table work.user_channel_loss_user_info_tmp_a_%(statdatenum)s as
                    select c.fgamefsk,
                           c.fplatformfsk,
                           c.fhallfsk,
                           c.fterminaltypefsk,
                           c.fversionfsk,
                           %(null_int_report)d fgame_id,
                           %(null_int_report)d fchannel_code,
                           c.hallmode,
                           b.fpkg_channel_id fchannel_id,
                           a.fuid,
                           a.fflag
                      from (select fbpid, fchannel_id, fuid, 30 fflag
                              from (select fbpid,
                                           fdate,
                                           fchannel_id,
                                           fuid,
                                           max(flag1) flag1,
                                           max(flag2) flag2
                                      from (select fbpid, dt fdate, fchannel_id, fuid, 1 flag1, 0 flag2
                                              from stage.fd_user_channel_stg a
                                             where a.dt >= date_add('%(statdate)s', -59)
                                               and a.dt < date_add('%(statdate)s', -29)
                                               and a.fet_id in (3, 4, 5)
                                            union all
                                            select fbpid, dt fdate, fchannel_id, fuid, 0 flag1, 1 flag2
                                              from stage.fd_user_channel_stg a
                                             where a.dt >= date_add('%(statdate)s', -29)
                                               and a.dt <= '%(statdate)s'
                                               and a.fet_id = 2) a
                                     group by fbpid, fdate, fchannel_id, fuid) a
                             where flag1 = 1
                               and flag2 = 0
                             union all
                            select fbpid, fchannel_id, fuid, 14 fflag
                              from (select fbpid,
                                           fdate,
                                           fchannel_id,
                                           fuid,
                                           max(flag1) flag1,
                                           max(flag2) flag2
                                      from (select fbpid, dt fdate, fchannel_id, fuid, 1 flag1, 0 flag2
                                              from stage.fd_user_channel_stg a
                                             where a.dt >= date_add('%(statdate)s', -27)
                                               and a.dt < date_add('%(statdate)s', -13)
                                               and a.fet_id in (3, 4, 5)
                                            union all
                                            select fbpid, dt fdate, fchannel_id, fuid, 0 flag1, 1 flag2
                                              from stage.fd_user_channel_stg a
                                             where a.dt >= date_add('%(statdate)s', -13)
                                               and a.dt <= '%(statdate)s'
                                               and a.fet_id = 2) a
                                     group by fbpid, fdate, fchannel_id, fuid) a
                             where flag1 = 1
                               and flag2 = 0
                             union all
                            select fbpid, fchannel_id, fuid, 7 fflag
                              from (select fbpid,
                                           fdate,
                                           fchannel_id,
                                           fuid,
                                           max(flag1) flag1,
                                           max(flag2) flag2
                                      from (select fbpid, dt fdate, fchannel_id, fuid, 1 flag1, 0 flag2
                                              from stage.fd_user_channel_stg a
                                             where a.dt >= date_add('%(statdate)s', -13)
                                               and a.dt < date_add('%(statdate)s', -6)
                                               and a.fet_id in (3, 4, 5)
                                            union all
                                            select fbpid, dt fdate, fchannel_id, fuid, 0 flag1, 1 flag2
                                              from stage.fd_user_channel_stg a
                                             where a.dt >= date_add('%(statdate)s', -6)
                                               and a.dt <= '%(statdate)s'
                                               and a.fet_id = 2) a
                                     group by fbpid, fdate, fchannel_id, fuid) a
                             where flag1 = 1
                               and flag2 = 0
                           ) a
                      join analysis.dc_channel_package b
                        on a.fchannel_id = b.fpkg_id
                      join dim.bpid_map c
                        on a.fbpid=c.fbpid;
          """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--活跃
                select  "%(statdate)s" fdate
                       ,fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,count( case when fflag = 7 then fuid end ) f7dayuserloss
                       ,count( case when fflag = 30 then fuid end ) f30dayuserloss
                  from work.user_channel_loss_user_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ insert overwrite table dcnew.user_channel_loss_user_info partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_channel_loss_user_info_tmp_a_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_channel_loss_user_info(sys.argv[1:])
a()
