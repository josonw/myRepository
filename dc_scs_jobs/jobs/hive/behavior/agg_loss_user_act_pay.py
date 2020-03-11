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


class agg_loss_user_act_pay(BaseStatModel):
    def create_tab(self):

        hql = """--流失漏斗_付费用户活跃流失
        create table if not exists dcnew.loss_user_act_pay (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flossdays           int        comment '流失天数',
               floss_unum          bigint     comment '流失用户数'
               )comment '流失漏斗_付费用户'
               partitioned by(dt date)
        location '/dw/dcnew/loss_user_act_pay'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fdate','dt'],
                        'groups':[[1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        #加上当天的分区
        hql = """
        alter table dcnew.loss_user_act_pay add if not exists partition (dt='%(statdate)s')
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--
        drop table if exists work.loss_act_tmp_b_%(statdatenum)s;
        create table work.loss_act_tmp_b_%(statdatenum)s as
                select forder_at fdate,
                       c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       a.fgame_id,
                       a.fchannel_code,
                       a.fuid,
                       case when fdate >= date_add(forder_at, 1) then 1 else 0 end  f1dusernum,
                       case when fdate >= date_add(forder_at, 2) then 1 else 0 end  f2dusernum,
                       case when fdate >= date_add(forder_at, 3) then 1 else 0 end  f3dusernum,
                       case when fdate >= date_add(forder_at, 4) then 1 else 0 end  f4dusernum,
                       case when fdate >= date_add(forder_at, 5) then 1 else 0 end  f5dusernum,
                       case when fdate >= date_add(forder_at, 6) then 1 else 0 end  f6dusernum,
                       case when fdate >= date_add(forder_at, 7) then 1 else 0 end  f7dusernum,
                       case when fdate >= date_add(forder_at, 14) then 1 else 0 end  f14dusernum,
                       case when fdate >= date_add(forder_at, 30) then 1 else 0 end  f30dusernum,
                       forder_at dt
                  from (select a.fbpid,
                               forder_at,
                               b.fgame_id,
                               b.fchannel_code,
                               b.dt fdate,
                               b.fuid
                          from (select distinct a.fbpid, a.dt forder_at, b.fuid
                                  from stage.payment_stream_stg a
                                  join dim.user_pay b
                                    on a.fbpid = b.fbpid
                                   and a.fplatform_uid = b.fplatform_uid
                                 where a.dt>= '%(ld_90day_ago)s' and a.dt <="%(statdate)s"
                               ) a
                          join dim.user_act b
                            on a.fbpid=b.fbpid
                           and a.fuid=b.fuid
                         where b.dt > '%(ld_90day_ago)s' and b.dt <="%(statdate)s"
                       ) a
                  join dim.bpid_map c
                    on a.fbpid=c.fbpid

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        hql = """
                select fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       count(distinct case when f1dusernum = 1 then fuid end) f1dusernum,
                       count(distinct case when f2dusernum = 1 then fuid end) f2dusernum,
                       count(distinct case when f3dusernum = 1 then fuid end) f3dusernum,
                       count(distinct case when f4dusernum = 1 then fuid end) f4dusernum,
                       count(distinct case when f5dusernum = 1 then fuid end) f5dusernum,
                       count(distinct case when f6dusernum = 1 then fuid end) f6dusernum,
                       count(distinct case when f7dusernum = 1 then fuid end) f7dusernum,
                       count(distinct case when f14dusernum = 1 then fuid end) f14dusernum,
                       count(distinct case when f30dusernum = 1 then fuid end) f30dusernum,
                       dt
                  from work.loss_act_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fdate,fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       dt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.loss_act_tmp_b_agg_%(statdatenum)s;
        create table work.loss_act_tmp_b_agg_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--按天数汇总流失用户
        insert overwrite table dcnew.loss_user_act_pay partition(dt)
                select fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       flossdays,
                       floss_unum,
                       dt
                  from (
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               1 flossdays,
                               f1dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               2 flossdays,
                               f2dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               3 flossdays,
                               f3dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               4 flossdays,
                               f4dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               5 flossdays,
                               f5dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               6 flossdays,
                               f6dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               7 flossdays,
                               f7dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               14 flossdays,
                               f14dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                         union all
                        select fdate,
                               fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fgame_id,
                               fchannel_code,
                               30 flossdays,
                               f30dusernum floss_unum,
                               dt
                          from work.loss_act_tmp_b_agg_%(statdatenum)s
                      ) t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.loss_user_act_pay partition(dt='3000-01-01')
                select fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fsubgamefsk,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannelcode,
                       flossdays,
                       floss_unum
                  from dcnew.loss_user_act_pay gs
                 where dt>= '%(ld_90day_ago)s' and dt <="%(statdate)s"
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.loss_act_tmp_b_%(statdatenum)s;
                 drop table if exists work.loss_act_tmp_b_agg_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_loss_user_act_pay(sys.argv[1:])
a()
