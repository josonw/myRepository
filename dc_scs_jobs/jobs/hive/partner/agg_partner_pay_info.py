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


class agg_partner_pay_info(BaseStatModel):
    def create_tab(self):

        hql = """--代理渠道付费数据
        create table if not exists dcnew.partner_pay_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fpartner_info       varchar(32)     comment '合作属性',
               fact_unum           bigint          comment '活跃用户数',
               forder_cnt          bigint          comment '下单数',
               fpay_unum           bigint          comment '付费用户数',
               fpay_cnt            bigint          comment '付费次数',
               fpay_income         decimal(20,2)   comment '付费金额',
               ff_pay_uunm         bigint          comment '首付用户数',
               ffpay_cnt           bigint          comment '首付次数',
               ffpay_income        decimal(20,2)   comment '首付金额'
               )comment '代理渠道付费数据'
               partitioned by(dt date)
        location '/dw/dcnew/partner_pay_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fpartner_info'],
                        'groups':[[1],
                                  [0]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--下单数、付费用户数、付费次数、付费金额、首付用户数、首付次数、首付金额
        drop table if exists work.partner_pay_info_tmp_b_%(statdatenum)s;
        create table work.partner_pay_info_tmp_b_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fversionfsk,
                 c.hallmode,
                 coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id,
                 coalesce(cast (t1.fchannel_id as bigint),%(null_int_report)d) fchannel_code,
                 coalesce(t2.fpartner_info,'%(null_str_report)s') fpartner_info,
                 t1.forder_id,
                 t1.fplatform_uid,
                 coalesce(t3.fcoins_num * t3.frate,0) fincome,
                 case when t3.forder_id is not null then 1 else 0 end is_sus,
                 case when t4.fplatform_uid is not null then 1 else 0 end is_first
            from stage.payment_stream_all_stg t1    --所有订单
            left join stage.user_generate_order_stg t2
              on t1.forder_id = t2.forder_id
             and t2.dt = '%(statdate)s'
            left join stage.payment_stream_stg t3   --支付成功订单
              on t1.forder_id = t3.forder_id
             and t3.dt = '%(statdate)s'
            left join dim.user_pay t4               --当日首付
              on t3.fbpid = t4.fbpid
             and t3.fplatform_uid = t4.fplatform_uid
             and t4.dt = '%(statdate)s'
            join (select *
                    from dim.bpid_map c
                   where c.fgamefsk = 4132314431
                      or c.fbpid = 'E2069973242BA5F77656813E2772547A'
                      or c.fbpid = '0637975C4E8EF0951127A0A73E190700'  --地方棋牌+贵阳麻将
                 ) c
              on t1.fbpid=c.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--下单数、付费用户数、付费次数、付费金额、首付用户数、首付次数、首付金额
                select '%(statdate)s' fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fpartner_info,%(null_str_group_rule)s) fpartner_info,
                       0 fact_unum,
                       count(distinct forder_id) forder_cnt,
                       count(distinct case when is_sus = 1 then fplatform_uid end) fpay_unum,
                       count(case when is_sus = 1 then fplatform_uid end) fpay_cnt,
                       sum(case when is_sus = 1 then fincome end) fpay_income,
                       count(distinct case when is_first = 1 then fplatform_uid end) ff_pay_uunm,
                       count(case when is_first = 1 then fplatform_uid end) ffpay_cnt,
                       cast (0 as decimal(20,2)) ffpay_income
                  from work.partner_pay_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fpartner_info
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.partner_pay_info_tmp_b_agg_%(statdatenum)s;
        create table work.partner_pay_info_tmp_b_agg_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--首付金额
        drop table if exists work.partner_pay_info_tmp_f_%(statdatenum)s;
        create table work.partner_pay_info_tmp_f_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ distinct c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fversionfsk,
                 c.hallmode,
                 t1.fgame_id,
                 t1.fchannel_code,
                 t1.fpartner_info,
                 t1.fuid,
                 t1.ffirst_pay_income
            from (select t1.fbpid,
                        coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id,
                        coalesce(cast (t1.fchannel_id as bigint),%(null_int_report)d) fchannel_code,
                        max(coalesce(t2.fpartner_info,'%(null_str_report)s')) fpartner_info,
                        t4.fuid,
                        t4.ffirst_pay_income
                   from stage.payment_stream_all_stg t1    --所有订单
                   left join stage.user_generate_order_stg t2
                     on t1.forder_id = t2.forder_id
                    and t2.dt = '%(statdate)s'
                   join stage.payment_stream_stg t3   --支付成功订单
                     on t1.forder_id = t3.forder_id
                    and t3.dt = '%(statdate)s'
                   join dim.user_pay t4               --当日首付
                     on t3.fbpid = t4.fbpid
                    and t3.fplatform_uid = t4.fplatform_uid
                    and t4.dt = '%(statdate)s'
                  where t1.dt = '%(statdate)s'
                  group by t1.fbpid,t2.fgame_id,t1.fchannel_id,t4.fuid,t4.ffirst_pay_income
                 ) t1
            join (select *
                    from dim.bpid_map c
                   where c.fgamefsk = 4132314431
                      or c.fbpid = 'E2069973242BA5F77656813E2772547A'
                      or c.fbpid = '0637975C4E8EF0951127A0A73E190700'  --地方棋牌+贵阳麻将
                 ) c
              on t1.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--首付金额
                select '%(statdate)s' fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fpartner_info,%(null_str_group_rule)s) fpartner_info,
                       0 fact_unum,
                       0 forder_cnt,
                       0 fpay_unum,
                       0 fpay_cnt,
                       0 fpay_income,
                       0 ff_pay_uunm,
                       0 ffpay_cnt,
                       sum(ffirst_pay_income) ffpay_income
                  from work.partner_pay_info_tmp_f_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fpartner_info
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table work.partner_pay_info_tmp_b_agg_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--活跃
        drop table if exists work.partner_pay_info_tmp_a_%(statdatenum)s;
        create table work.partner_pay_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fversionfsk,
                 c.hallmode,
                 t1.fgame_id,
                 t1.fchannel_code,
                 coalesce(t2.fpartner_info,t3.fpartner_info,t4.fpartner_info,'%(null_str_report)s') fpartner_info,
                 t1.fuid
            from dim.user_act t1
            left join (select t1.fbpid,
                              t1.fuid,
                              t1.fpartner_info,
                              row_number() over(partition by t1.fbpid,t1.fuid order by t1.flogin_at desc) row_num --取最后一次登陆时的fpartner_info
                         from dim.user_login_additional t1
                         join (select *
                                 from dim.bpid_map c
                                where c.fgamefsk = 4132314431
                                   or c.fbpid = 'E2069973242BA5F77656813E2772547A'
                                   or c.fbpid = '0637975C4E8EF0951127A0A73E190700'  --地方棋牌+贵阳麻将
                              ) c
                           on t1.fbpid=c.fbpid
                        where t1.dt = '%(statdate)s'
               ) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.row_num = 1  --取最后一次登陆时的fpartner_info
            left join (select t1.fbpid,
                              t1.fuid,
                              t1.fpartner_info,
                              row_number() over(partition by t1.fbpid,t1.fuid order by t1.fe_timer desc) row_num --取最后一次牌局时的fpartner_info
                         from stage.user_gameparty_stg t1
                         join (select *
                                 from dim.bpid_map c
                                where c.fgamefsk = 4132314431
                                   or c.fbpid = 'E2069973242BA5F77656813E2772547A'
                                   or c.fbpid = '0637975C4E8EF0951127A0A73E190700'  --地方棋牌+贵阳麻将
                              ) c
                           on t1.fbpid=c.fbpid
                        where t1.dt = '%(statdate)s'
               ) t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.row_num = 1  --取最后一次牌局时的fpartner_info
            left join (select t1.fbpid,
                              t1.fuid,
                              t1.fpartner_info,
                              row_number() over(partition by t1.fbpid,t1.fuid order by t1.lts_at desc) row_num --取最后一次金流时的fpartner_info
                         from stage.pb_gamecoins_stream_stg t1
                         join (select *
                                 from dim.bpid_map c
                                where c.fgamefsk = 4132314431
                                   or c.fbpid = 'E2069973242BA5F77656813E2772547A'
                                   or c.fbpid = '0637975C4E8EF0951127A0A73E190700'  --地方棋牌+贵阳麻将
                              ) c
                           on t1.fbpid=c.fbpid
                        where t1.dt = '%(statdate)s'
               ) t4
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
             and t4.row_num = 1  --取最后一次金流时的fpartner_info
            join (select *
                    from dim.bpid_map c
                   where c.fgamefsk = 4132314431
                      or c.fbpid = 'E2069973242BA5F77656813E2772547A'
                      or c.fbpid = '0637975C4E8EF0951127A0A73E190700'  --地方棋牌+贵阳麻将
                 ) c
              on t1.fbpid=c.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--活跃用户数
                select '%(statdate)s' fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fpartner_info,%(null_str_group_rule)s) fpartner_info,
                       count(distinct fuid) fact_unum,
                       0 forder_cnt,
                       0 fpay_unum,
                       0 fpay_cnt,
                       0 fpay_income,
                       0 ff_pay_uunm,
                       0 ffpay_cnt,
                       0 ffpay_income
                  from work.partner_pay_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fpartner_info
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table work.partner_pay_info_tmp_b_agg_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res



        hql = """insert overwrite table dcnew.partner_pay_info
                    partition(dt='%(statdate)s')
                  select fdate,
                         fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         fversionfsk,
                         fchannel_code,
                         fpartner_info,
                         sum(fact_unum) fact_unum,
                         sum(forder_cnt) forder_cnt,
                         sum(fpay_unum) fpay_unum,
                         sum(fpay_cnt) fpay_cnt,
                         sum(fpay_income) fpay_income,
                         sum(ff_pay_uunm) ff_pay_uunm,
                         sum(ffpay_cnt) ffpay_cnt,
                         sum(ffpay_income) ffpay_income
                    from work.partner_pay_info_tmp_b_agg_%(statdatenum)s gs
                   group by fdate,fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                            fpartner_info

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.partner_info_tmp_%(statdatenum)s;
                 drop table if exists work.partner_pay_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.partner_pay_info_tmp_b_%(statdatenum)s;
                 drop table if exists work.partner_pay_info_tmp_f_%(statdatenum)s;
                 drop table if exists work.partner_pay_info_tmp_b_agg_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_partner_pay_info(sys.argv[1:])
a()
