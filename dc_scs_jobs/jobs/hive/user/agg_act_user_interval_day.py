# -*- coding: UTF-8 -*-
#src     :stage.payment_stream_stg,dim.user_act,dim.user_pay,dim.bpid_map
#dst     :dcnew.loss_user_act_pay
#authot  :SimonRen
#date    :2016-09-02


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


class agg_act_user_interval_day(BaseStatModel):
    def create_tab(self):

        hql = """create table if not exists dcnew.act_user_interval_day
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fdq1 bigint comment '距离上一次活跃天数为1的活跃用户人数',
                fdq2 bigint comment '距离上一次活跃天数为2的活跃用户人数',
                fdq3 bigint comment '距离上一次活跃天数为3的活跃用户人数',
                fdq4 bigint comment '距离上一次活跃天数为4的活跃用户人数',
                fdq5 bigint comment '距离上一次活跃天数为5的活跃用户人数',
                fdq6 bigint comment '距离上一次活跃天数为6的活跃用户人数',
                fdq7 bigint comment '距离上一次活跃天数为7的活跃用户人数',
                fdmq8lq14 bigint comment '距离上一次活跃天数大于等于且8小于等于14的活跃用户人数',
                fdmq15lq30 bigint comment '距离上一次活跃天数大于等于且15小于等于30的活跃用户人数',
                fdm30 bigint comment '距离上一次活跃天数大于30的活跃用户人数'
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fuid'],
                        'groups':[[1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """--取用户活跃数据
        drop table if exists work.act_user_interval_day_%(statdatenum)s;
        create table work.act_user_interval_day_%(statdatenum)s  as
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                       coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                       a.fuid,
                       max(coalesce(b.dt,cast ('%(ld_60day_ago)s' as date))) days
                  from dim.user_act a
                  left join (select fbpid, fuid, max(dt) dt
                              from (select fbpid, fuid, dt
                                      from dim.user_act b
                                     where b.dt < '%(statdate)s'
                                       and b.dt >= '%(ld_30day_ago)s'
                                     union all
                                    select fbpid, fuid, cast (dt as date)
                                      from dim.reg_user_main_additional b
                                     where b.dt < '%(statdate)s'
                                       and b.dt >= '%(ld_30day_ago)s'
                                   ) t
                             group by fbpid, fuid
                            ) b
                    on a.fbpid = b.fbpid
                   and a.fuid = b.fuid
                  join dim.bpid_map c
                    on a.fbpid=c.fbpid
                 where a.dt = '%(statdate)s'
                 group by c.fgamefsk,
                          c.fplatformfsk,
                          c.fhallfsk,
                          c.fterminaltypefsk,
                          c.fversionfsk,
                          c.hallmode, a.fuid,a.fgame_id,a.fchannel_code
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取组合之后的用户等级数据
                 select fgamefsk,
                        coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                        coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                        coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                        coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                        coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                        coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                        datediff('%(statdate)s', max(days)) days,
                        fuid
                   from work.act_user_interval_day_%(statdatenum)s
                  where hallmode = %(hallmode)s
                  group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.act_user_interval_day_2_%(statdatenum)s;
        create table work.act_user_interval_day_2_%(statdatenum)s  as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.act_user_interval_day
              partition(dt='%(statdate)s')
                select '%(statdate)s' fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       count(distinct case when days = 1 then fuid end)  fdq1,
                       count(distinct case when days = 2 then fuid end)  fdq2,
                       count(distinct case when days = 3 then fuid end)  fdq3,
                       count(distinct case when days = 4 then fuid end)  fdq4,
                       count(distinct case when days = 5 then fuid end)  fdq5,
                       count(distinct case when days = 6 then fuid end)  fdq6,
                       count(distinct case when days = 7 then fuid end)  fdq7,
                       count(distinct case when days >= 8  and days <= 14 then fuid end)  fdmq8lq14,
                       count(distinct case when days >= 15 and days <= 30 then fuid end)  fdmq15lq30,
                       count(distinct case when days > 30  then fuid end)  fdm30
                  from work.act_user_interval_day_2_%(statdatenum)s
                 group by fgamefsk,
                          fplatformfsk,
                          fhallfsk,
                          fgame_id,
                          fterminaltypefsk,
                          fversionfsk,
                          fchannel_code
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.act_user_interval_day_%(statdatenum)s;
                 drop table if exists work.act_user_interval_day_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_act_user_interval_day(sys.argv[1:])
a()
