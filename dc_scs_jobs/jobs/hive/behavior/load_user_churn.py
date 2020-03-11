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


class load_user_churn(BaseStatModel):
    def create_tab(self):

        hql = """--
        create external table if not exists dim.user_churn (
                    dt             date,
                    fbpid          varchar(50),
                    fgame_id       bigint,
                    fchannel_code  bigint,
                    fuid           bigint,
                    days           int,              --流失天数
                    is_pay         int,              --是否付费用户
                    gamecoins      decimal(32,0),    --流失时资产
                    dip            decimal(38,4),    --付费额度
                    paycnt         bigint,           --付费次数
                    ruptcnt        bigint,           --破产次数
                    fgrade         bigint            --等级
          )comment '流失用户中间表'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.exec.dynamic.partition.mode=nonstrict;set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--
        insert overwrite table dim.user_churn
               select a.dt,
                      a.fbpid,
                      a.fgame_id,
                      a.fchannel_code,
                      a.fuid,
                      case cast (a.dt as string) when '%(ld_2day_ago)s' then 2
                                when '%(ld_5day_ago)s' then 5
                                when '%(ld_7day_ago)s' then 7
                                when '%(ld_14day_ago)s' then 14
                                when '%(ld_30day_ago)s' then 30
                      end days,
                      case when b.fuid is not null then 1 else 0 end is_pay,
                      c.fgamecoins,
                      b.dip,
                      b.paycnt,
                      d.ruptcnt,
                      fgrade
                 from dim.user_act a
                 left join (select b.fbpid, b.fuid, count(1) paycnt,round(sum(fcoins_num * frate)) dip
                                  from dim.user_pay b
                                  join stage.payment_stream_stg c
                                    on b.fbpid = c.fbpid
                                   and b.fplatform_uid = c.fplatform_uid
                                   and c.fdate <= '%(statdate)s'
                                 group by b.fbpid, b.fuid
                           ) b
                   on a.fbpid = b.fbpid
                  and a.fuid = b.fuid
                 left join (select dt,fbpid, fuid,fgamecoins
                              from dim.user_gamecoin_balance_day c
                             where cast (c.dt as string) in ('%(ld_2day_ago)s','%(ld_5day_ago)s','%(ld_7day_ago)s','%(ld_14day_ago)s','%(ld_30day_ago)s')
                           ) c
                   on a.fbpid = c.fbpid
                  and a.fuid = c.fuid
                  and a.dt = c.dt
                 left join (select c.dt, c.fbpid, c.fuid, count(1) ruptcnt
                              from stage.user_bankrupt_stg c
                             where cast (c.dt as string) in ('%(ld_2day_ago)s','%(ld_5day_ago)s','%(ld_7day_ago)s','%(ld_14day_ago)s','%(ld_30day_ago)s')
                             group by c.dt, c.fbpid, c.fuid
                           ) d
                   on a.fbpid = d.fbpid
                  and a.fuid = d.fuid
                  and a.dt = d.dt
                 left join (select fbpid,fuid,nvl(fgrade,0) fgrade
                              from dim.user_attribute
                           ) e
                   on a.fbpid = e.fbpid
                  and a.fuid = e.fuid
                 left join (select a.fbpid, a.fuid, max(dt) dt
                              from dim.user_act a
                             where a.dt between '%(ld_30day_ago)s' and '%(statdate)s'
                             group by a.fbpid, a.fuid
                           ) f
                   on a.fbpid = f.fbpid
                  and a.fuid = f.fuid
                where cast (a.dt as string) in ('%(ld_2day_ago)s','%(ld_5day_ago)s','%(ld_7day_ago)s','%(ld_14day_ago)s','%(ld_30day_ago)s')
                  and a.dt >= f.dt;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = load_user_churn(sys.argv[1:])
a()
