# -*- coding: UTF-8 -*-
#src     :dim.user_churn,dim.user_reflux,dim.user_pay,dim.bpid_map
#dst     :dcnew.loss_user_everyday_loss_reflux
#authot  :SimonRen
#date    :2016-09-02


import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class agg_loss_user_everyday_loss_reflux(BaseStatModel):
    def create_tab(self):

        hql = """--每日流失/回流
        create table if not exists dcnew.loss_user_everyday_loss_reflux
              (fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flossdays           int        comment '不活跃天数',
               floss_unum          bigint     comment '流失用户数',
               floss_pay_unum      bigint     comment '付费用户流失数',
               freflux_unum        bigint     comment '流失回流用户数',
               freflux_pay_unum    bigint     comment '付费用户流失回流数'
              )comment '流失用户每日流失回流'
               partitioned by(dt date)
        location '/dw/dcnew/loss_user_everyday_loss_reflux'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['flossdays'],
                        'groups':[[1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--取付费
        drop table if exists work.day_loss_tmp_pay_%(statdatenum)s;
        create table work.day_loss_tmp_pay_%(statdatenum)s as
               select c.fgamefsk,
                      c.fplatformfsk,
                      c.fhallfsk,
                      c.fterminaltypefsk,
                      c.fversionfsk,
                      c.hallmode,
                      a.fgame_id,
                      a.fchannel_code,
                      a.days flossdays,
                      a.fuid, --付费用户
                      is_pay
                 from dim.user_churn a
                 join dim.bpid_map c
                   on a.fbpid=c.fbpid
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合流失回流数据
               select fgamefsk,
                      coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                      coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                      coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                      coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                      coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                      coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                      flossdays,
                      count(distinct fuid) floss_unum,
                      count(distinct case when is_pay = 1 then fuid end) floss_pay_unum,
                      0 freflux_unum,
                      0 freflux_pay_unum
                 from work.day_loss_tmp_pay_%(statdatenum)s b
                where hallmode = %(hallmode)s
                group by fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         fversionfsk,
                         fchannel_code,
                         flossdays
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.day_loss_tmp_%(statdatenum)s;
        create table work.day_loss_tmp_%(statdatenum)s as
            %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合数据并导入到正式表上
      insert into table work.day_loss_tmp_%(statdatenum)s
               select  fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       freflux flossdays,
                       0 floss_unum,
                       0 floss_pay_unum,
                       count(distinct fuid) freflux_unum,
                       count(distinct case when is_pay = 1 then fuid end) freflux_pay_unum
                  from dim.user_reflux_array gs
                 where dt='%(statdate)s' and freflux_type='day'
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       freflux
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--汇总目标表
        insert overwrite table dcnew.loss_user_everyday_loss_reflux
        partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       flossdays,
                       sum(floss_unum) floss_unum,
                       sum(floss_pay_unum) floss_pay_unum,
                       sum(freflux_unum) freflux_unum,
                       sum(freflux_pay_unum) freflux_pay_unum
                  from work.day_loss_tmp_%(statdatenum)s gs
                 group by fgamefsk,
                          fplatformfsk,
                          fhallfsk,
                          fgame_id,
                          fterminaltypefsk,
                          fversionfsk,
                          fchannel_code,
                          flossdays

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.day_loss_tmp_pay_%(statdatenum)s;
                 drop table if exists work.day_loss_tmp_%(statdatenum)s;

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_loss_user_everyday_loss_reflux(sys.argv[1:])
a()
