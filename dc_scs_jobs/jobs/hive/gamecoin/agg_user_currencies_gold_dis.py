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


class agg_user_currencies_gold_dis(BaseStatModel):
    def create_tab(self):

        hql = """
        --用户博雅币分布，当日用户博雅币初始携带量分布
        create table if not exists dcnew.user_currencies_gold_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flft                bigint     comment '区间下界',
               frgt                bigint     comment '区间上界',
               flog_unum           bigint     comment '携带游戏币登陆的用户数',
               fbr_unum            bigint     comment '破产用户数',
               fpay_unum           bigint     comment '付费用户数',
               fbank_unum          bigint     comment '保险箱金币数在该区间内的用户数',
               ftotal_unum         bigint     comment '保险箱+携带金币数在该区间内的用户数',
               fby_unum            bigint     comment '携带博雅币的用户数'
               )comment '用户金币分布'
               partitioned by(dt date)
        location '/dw/dcnew/user_currencies_gold_dis';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['flft','frgt'],
                        'groups':[[1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--算出当日用户初始金币、博雅币携带量，以及当日是否破产，是否付费
        drop table if exists work.currencies_gold_b_%(statdatenum)s;
        create table work.currencies_gold_b_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     coalesce(t.fgame_id,%(null_int_report)d) fgame_id,
                     coalesce(t.fchannel_code,%(null_int_report)d) fchannel_code,
                     t.fuid,
                     case when t2.fuid is not null then 1 else 0 end rupt_f, --是否破产
                     t.user_gamecoins, --金币携带量
                     t.bank_gamecoins, --保险箱金币量
                     t.total_gamecoins, --总金币量
                     t.user_bycoins --博雅币量
                from (select t1.fbpid,
                             t1.fuid,
                             t2.fgame_id,
                             cast (t2.fchannel_code as bigint) fchannel_code,
                             t1.user_gamecoins, --金币携带量
                             t1.bank_gamecoins, --保险箱金币量
                             t1.user_bycoins, --博雅币量
                             t1.user_gamecoins + t1.bank_gamecoins total_gamecoins  --总金币量
                        from (select t1.fbpid,
                                     t1.fuid,
                                     t1.user_gamecoins, --金币携带量
                                     t1.bank_gamecoins, --保险箱金币量
                                     t1.user_bycoins, --博雅币量
                                     row_number() over(partition by t1.fbpid, t1.fuid order by t1.flogin_at, t1.user_gamecoins) rown
                                from dim.user_login_additional t1
                               where t1.dt = "%(statdate)s"
                             ) t1
                        left join dim.user_act t2
                          on t1.fbpid = t2.fbpid
                         and t1.fuid = t2.fuid
                         and t2.dt = "%(statdate)s"
                       where t1.rown = 1
                     ) t
                join dim.bpid_map c
                  on t.fbpid=c.fbpid
                left join (select fbpid, fuid, count(1) frupt_num --破产次数
                             from stage.user_bankrupt_stg
                            where dt = "%(statdate)s"
                            group by fbpid, fuid
                          ) t2
                  on t.fbpid = t2.fbpid
                 and t.fuid = t2.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--是否付费
        drop table if exists work.currencies_gold_p_%(statdatenum)s;
        create table work.currencies_gold_p_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     p.fgame_id,
                     p.fchannel_code,
                     p.fuid,
                     p.fgamecoins user_gamecoins_num
                from dim.user_gamecoin_balance_day p
                join dim.user_pay pu
                  on p.fbpid = pu.fbpid
                 and p.fuid = pu.fuid
                join dim.bpid_map c
                  on p.fbpid=c.fbpid
               where p.dt = "%(statdate)s";
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--将用户当日金币、博雅币携带量，转换为区间数据，并合并数据1
              select fgamefsk,
                     coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                     coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                     coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                     coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                     coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                     coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                     flft,
                     frgt,
                     count(distinct a.fuid) flog_unum,
                     count(distinct case when a.rupt_f = 1 then a.fuid end) fbr_unum,
                     0 fpay_unum,
                     0 fbank_unum,
                     0 ftotal_unum,
                     0 fby_unum
                from work.currencies_gold_b_%(statdatenum)s a
                join (select distinct flft,frgt from stage.jw_qujian) b
               where a.user_gamecoins >= b.flft
                 and a.user_gamecoins < b.frgt
                 and a.hallmode = %(hallmode)s
               group by fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        flft,
                        frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
          drop table if exists work.currencies_dis_tmp_b_%(statdatenum)s;
        create table work.currencies_dis_tmp_b_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--将用户当日金币、博雅币携带量，转换为区间数据，并合并数据2
              select fgamefsk,
                     coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                     coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                     coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                     coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                     coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                     coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                     flft,
                     frgt,
                     0 flog_unum,
                     0 fbr_unum,
                     0 fpay_unum,
                     count(distinct a.fuid) fbank_unum,
                     0 ftotal_unum,
                     0 fby_unum
                from work.currencies_gold_b_%(statdatenum)s a
                join (select distinct flft,frgt from stage.jw_qujian) b
               where a.bank_gamecoins >= b.flft
                 and a.bank_gamecoins < b.frgt
                 and a.hallmode = %(hallmode)s
               group by fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        flft,
                        frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert into table work.currencies_dis_tmp_b_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--将用户当日金币、博雅币携带量，转换为区间数据，并合并数据2
              select fgamefsk,
                     coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                     coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                     coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                     coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                     coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                     coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                     flft,
                     frgt,
                     0 flog_unum,
                     0 fbr_unum,
                     count(distinct a.fuid) fpay_unum,
                     0 fbank_unum,
                     0 ftotal_unum,
                     0 fby_unum
                from work.currencies_gold_p_%(statdatenum)s a
                join (select distinct flft,frgt from stage.jw_qujian) b
               where a.user_gamecoins_num >= b.flft
                 and a.user_gamecoins_num < b.frgt
                 and a.hallmode = %(hallmode)s
               group by fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        flft,
                        frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert into table work.currencies_dis_tmp_b_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res




        hql = """--将用户当日金币、博雅币携带量，转换为区间数据，并合并数据3
              select fgamefsk,
                     coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                     coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                     coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                     coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                     coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                     coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                     flft,
                     frgt,
                     0 flog_unum,
                     0 fbr_unum,
                     0 fpay_unum,
                     0 fbank_unum,
                     count(distinct a.fuid) ftotal_unum,
                     0 fby_unum
                from work.currencies_gold_b_%(statdatenum)s a
                join (select distinct flft,frgt from stage.jw_qujian) b
               where a.total_gamecoins >= b.flft
                 and a.total_gamecoins < b.frgt
                 and a.hallmode = %(hallmode)s
               group by fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        flft,
                        frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert into table work.currencies_dis_tmp_b_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res



        hql = """--将用户当日金币、博雅币携带量，转换为区间数据，并合并数据4
              select fgamefsk,
                     coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                     coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                     coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                     coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                     coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                     coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                     flft,
                     frgt,
                     0 flog_unum,
                     0 fbr_unum,
                     0 fpay_unum,
                     0 fbank_unum,
                     0 ftotal_unum,
                     count(distinct a.fuid) fby_unum
                from work.currencies_gold_b_%(statdatenum)s a
                join (select distinct flft,frgt from stage.jw_qujian) b
               where a.user_bycoins >= b.flft
                 and a.user_bycoins < b.frgt
                 and a.hallmode = %(hallmode)s
               group by fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fgame_id,
                        fchannel_code,
                        flft,
                        frgt
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert into table work.currencies_dis_tmp_b_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据并导入到正式表上
        insert overwrite table dcnew.user_currencies_gold_dis
        partition (dt = '%(statdate)s')
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       flft,
                       frgt,
                       sum(flog_unum) flog_unum,
                       sum(fbr_unum) fbr_unum,
                       sum(fpay_unum) fpay_unum,
                       sum(fbank_unum) fbank_unum,
                       sum(ftotal_unum) ftotal_unum,
                       sum(fby_unum) fby_unum
                  from work.currencies_dis_tmp_b_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       flft,
                       frgt
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.currencies_gold_b_%(statdatenum)s;
                 drop table if exists work.currencies_dis_tmp_b_%(statdatenum)s;
                 drop table if exists work.currencies_gold_p_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_currencies_gold_dis(sys.argv[1:])
a()
