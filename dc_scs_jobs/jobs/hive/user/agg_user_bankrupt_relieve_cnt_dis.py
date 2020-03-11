# -*- coding: UTF-8 -*-
#src     :stage.user_bankrupt_stg,dim.bpid_map,dim.user_act,dim.user_pay,stage.payment_stream_stg
#dst     :dcnew.user_bankrupt_relieve_cnt_dis
#authot  :SimonRen
#date    :2016-09-05


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


class agg_user_bankrupt_relieve_cnt_dis(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_bankrupt_relieve_cnt_dis (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fuser_type             varchar(20)     comment '用户类型:活跃brp_act 付费brp_pay 注册brp_reg 历史付费brp_paid 破产brp_rupt 救济rel_rupt',
               fcnt1_unum             bigint          comment '1次的用户数',
               fcnt2_unum             bigint          comment '2次的用户数',
               fcnt3_unum             bigint          comment '3次的用户数',
               fcnt4_unum             bigint          comment '4次的用户数',
               fcnt5_unum             bigint          comment '5次的用户数',
               fcnt6_unum             bigint          comment '6次的用户数',
               fcnt7_unum             bigint          comment '7次的用户数',
               fcnt8_unum             bigint          comment '8次的用户数',
               fcnt9_unum             bigint          comment '9次的用户数',
               fcnt10_unum            bigint          comment '10次的用户数',
               fcntm10_unum           bigint          comment '大于10次的用户数'
               )comment '破产救济次数分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_bankrupt_relieve_cnt_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fuser_type','fuid'],
                        'groups':[[1,1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;
                           """)
        if res != 0: return res


        hql = """--取出当日破产用户数
        drop table if exists work.user_bankrupt_cnt_tmp_a_%(statdatenum)s;
        create table work.user_bankrupt_cnt_tmp_a_%(statdatenum)s as
                select fbpid, fuid, coalesce(fgame_id,cast (0 as bigint)) fgame_id, fchannel_code, count(1) bankrupt_num
                  from stage.user_bankrupt_stg
                 where dt="%(statdate)s"
                 group by fbpid, fuid, fgame_id, fchannel_code
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--当日破产用户数
        drop table if exists work.user_bankrupt_cnt_tmp_b_%(statdatenum)s;
        create table work.user_bankrupt_cnt_tmp_b_%(statdatenum)s as
        select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               coalesce(fgame_id,%(null_int_report)d) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               'brp_rupt' fuser_type,
               a.bankrupt_num bank_num,
               a.fuid
          from work.user_bankrupt_cnt_tmp_a_%(statdatenum)s a
          join dim.bpid_map c
            on a.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--活跃用户破产次数
        insert into table work.user_bankrupt_cnt_tmp_b_%(statdatenum)s
        select distinct fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
               coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               case when b.dt = "%(statdate)s" then 'brp_act'
                    when b.dt = "%(ld_1day_ago)s" then 'brp_1dact'
                    when b.dt = "%(ld_7day_ago)s" then 'brp_7dact'
                    when b.dt = "%(ld_30day_ago)s" then 'brp_30dact'
                    end fuser_type,
               a.bankrupt_num bank_num,
               b.fuid
          from work.user_bankrupt_cnt_tmp_a_%(statdatenum)s a
          join dim.user_act b
            on a.fbpid = b.fbpid
           and a.fuid = b.fuid
           and cast(b.dt as string) in ("%(statdate)s", "%(ld_1day_ago)s", "%(ld_7day_ago)s", "%(ld_30day_ago)s")
          join dim.bpid_map c
            on a.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--注册用户破产次数
        insert into table work.user_bankrupt_cnt_tmp_b_%(statdatenum)s
        select distinct fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
               coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               case when b.dt = "%(statdate)s" then 'brp_reg'
                    when b.dt = "%(ld_1day_ago)s" then 'brp_1dreg'
                    when b.dt = "%(ld_7day_ago)s" then 'brp_7dreg'
                    when b.dt = "%(ld_30day_ago)s" then 'brp_30dreg'
                    end fuser_type,
               a.bankrupt_num bank_num,
               b.fuid
          from work.user_bankrupt_cnt_tmp_a_%(statdatenum)s a
          join dim.reg_user_main_additional b
            on a.fbpid = b.fbpid
           and a.fuid = b.fuid
           and cast(b.dt as string) in ("%(statdate)s", "%(ld_1day_ago)s", "%(ld_7day_ago)s", "%(ld_30day_ago)s")
          join dim.bpid_map  c
            on a.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--付费用户破产次数
        insert into table work.user_bankrupt_cnt_tmp_b_%(statdatenum)s
        select distinct fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               coalesce(fgame_id,%(null_int_report)d) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               case when b.dt = "%(statdate)s" then 'brp_pay'
                    when b.dt = "%(ld_1day_ago)s" then 'brp_1dpay'
                    when b.dt = "%(ld_7day_ago)s" then 'brp_7dpay'
                    when b.dt = "%(ld_30day_ago)s" then 'brp_30dpay'
                    end fuser_type,
               a.bankrupt_num bank_num,
               b.fuid
          from work.user_bankrupt_cnt_tmp_a_%(statdatenum)s a
          join (select distinct d.fbpid, d.dt, c.fuid
                  from dim.user_pay_day c
                  join stage.payment_stream_stg d
                    on c.fbpid = d.fbpid
                   and c.fplatform_uid = d.fplatform_uid
                   and cast(d.dt as string) in("%(statdate)s", "%(ld_1day_ago)s", "%(ld_7day_ago)s", "%(ld_30day_ago)s")
               ) b
            on a.fbpid=b.fbpid
           and a.fuid=b.fuid
          join dim.bpid_map c
            on a.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--历史付费用户破产次数
        insert into table work.user_bankrupt_cnt_tmp_b_%(statdatenum)s
        select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               coalesce(fgame_id,%(null_int_report)d) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               'brp_paid' fuser_type,
               a.bankrupt_num bank_num,
               b.fuid
          from work.user_bankrupt_cnt_tmp_a_%(statdatenum)s a
          join (select distinct fbpid, fuid
                  from dim.user_pay
                 where ffirst_pay_at<"%(statdate)s"
               ) b
            on a.fbpid = b.fbpid and a.fuid = b.fuid
          join dim.bpid_map  c
            on a.fbpid=c.fbpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--当日救济用户数
        drop table if exists work.user_relieve_cnt_tmp_b_%(statdatenum)s;
        create table work.user_relieve_cnt_tmp_b_%(statdatenum)s as
        select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               coalesce(fgame_id,cast (0 as bigint)) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               'rel_rupt' fuser_type,
               count(1) frelieve_cnt,
               a.fuid
          from stage.user_bankrupt_relieve_stg a
          join dim.bpid_map c
            on a.fbpid=c.fbpid
         where a.dt="%(statdate)s"
         group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
               fgame_id,
               fchannel_code,
               a.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,
                       fuid,
                       sum(bank_num) cnt
                  from work.user_bankrupt_cnt_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.bank_relieve_tmp_b_%(statdatenum)s;
        create table work.bank_relieve_tmp_b_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,
                       fuid,
                       sum(frelieve_cnt) cnt
                  from work.user_relieve_cnt_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into table work.bank_relieve_tmp_b_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--插入目标表
        insert overwrite table dcnew.user_bankrupt_relieve_cnt_dis
        partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,
                       count(distinct case when cnt = 1 then fuid end) fcnt1_unum,
                       count(distinct case when cnt = 2 then fuid end) fcnt2_unum,
                       count(distinct case when cnt = 3 then fuid end) fcnt3_unum,
                       count(distinct case when cnt = 4 then fuid end) fcnt4_unum,
                       count(distinct case when cnt = 5 then fuid end) fcnt5_unum,
                       count(distinct case when cnt = 6 then fuid end) fcnt6_unum,
                       count(distinct case when cnt = 7 then fuid end) fcnt7_unum,
                       count(distinct case when cnt = 8 then fuid end) fcnt8_unum,
                       count(distinct case when cnt = 9 then fuid end) fcnt9_unum,
                       count(distinct case when cnt = 10 then fuid end) fcnt10_unum,
                       count(distinct case when cnt > 10 then fuid end) fcntm10_unum
                  from work.bank_relieve_tmp_b_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.user_bankrupt_cnt_tmp_a_%(statdatenum)s;
                 drop table if exists work.user_bankrupt_cnt_tmp_b_%(statdatenum)s;
                 drop table if exists work.user_relieve_cnt_tmp_b_%(statdatenum)s;
                 drop table if exists work.bank_relieve_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_bankrupt_relieve_cnt_dis(sys.argv[1:])
a()
