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


class agg_bud_pay_income_sharing_party(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_pay_income_sharing_party (
               fdate               string,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fpname              varchar(100)     comment '一级场次',
               fsubname            varchar(100)     comment '二级场次',
               fgsubname           varchar(100)     comment '三级场次',
               fshare_type         varchar(100)     comment '消耗类型（报名，台费）',
               fcoins_type         varchar(100)     comment '货币类型（银币，金条）',
               fnumber             bigint           comment '货币数量',
               fsharing_income     decimal(20,2)    comment '分摊收入',
               fsharing_rate       decimal(20,18)   comment '分摊比例'
               )comment '付费收入分摊'
               partitioned by(dt string);

        create table if not exists bud_dm.bud_pay_income_sharing_party_uid (
               fdate               string,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuid                bigint,
               fpname              varchar(100)     comment '一级场次',
               fsubname            varchar(100)     comment '二级场次',
               fgsubname           varchar(100)     comment '三级场次',
               fshare_type         varchar(100)     comment '消耗类型（报名，台费）',
               fcoins_type         varchar(100)     comment '货币类型（银币，金条）',
               fnumber             bigint           comment '货币数量',
               fsharing_income     decimal(20,2)    comment '分摊收入',
               fsharing_rate       decimal(20,18)   comment '分摊比例'
               )comment '付费收入分摊'
               partitioned by(dt string);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--付费数据
            drop table if exists work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s;
          create table work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s as
          select t.fbpid
                 ,t.fuid
                 ,t.fdate
                 ,tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t.fcoins_num
                 ,t.frate
                 ,t.forder_id
                 ,case when fproduct_name like '%%币%%' then '银币'
                       when fproduct_name like '%%幣%%' then '银币'
                       when fproduct_name like '%%金%%' then '金条'
                       else concat('其他_',fproduct_name) end fproduct_type
                 ,fproduct_name
                 ,t.fpamount_usd
            from stage.payment_stream_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--牌局
            drop table if exists work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s;
          create table work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s as
          select t.fbpid, fuid,t.flts_at,tt.fhallfsk,tt.fplatformfsk,fgame_id,t.fpname,t.fsubname,t.fgsubname,t.fgamecoins
                 ,case when fcoins_type = '1' then '银币'
                       when fcoins_type = '11' then '金条' else fcoins_type end fproduct_type
            from stage.user_gameparty_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = '%(statdate)s'
          union all
          select t.fbpid, fuid,t.flts_at,tt.fhallfsk,tt.fplatformfsk,fgame_id,t.fpname,t.fsubname,t.fgsubname,t.fgamecoins
                 ,case when fcoins_type = '1' then '银币'
                       when fcoins_type = '11' then '金条' else fcoins_type end fproduct_type
            from stage.user_gameparty_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = date_add('%(statdate)s',1) and flts_at <= concat(dt,' 01:00:00')
          union all
          select t.fbpid, fuid,t.flts_at,tt.fhallfsk,tt.fplatformfsk,fgame_id,t.fpname,t.fsubname,t.fgsubname,t.fgamecoins
                 ,case when fcoins_type = '1' then '银币'
                       when fcoins_type = '11' then '金条' else fcoins_type end fproduct_type
            from stage.user_gameparty_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = date_sub('%(statdate)s',1) and flts_at >= concat(dt,' 23:00:00')

           union all


          select t.fbpid, fuid,t.flts_at,tt.fhallfsk,tt.fplatformfsk,fgame_id,t.fpname,t.fname fsubname,t.fsubname fgsubname,t.fentry_fee fgamecoins
                 ,case when fitem_id = '0' then '银币'
                       when fitem_id = '1' then '金条' else fitem_id end fproduct_type
            from stage.join_gameparty_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = '%(statdate)s'
          union all
          select t.fbpid, fuid,t.flts_at,tt.fhallfsk,tt.fplatformfsk,fgame_id,t.fpname,t.fname fsubname,t.fsubname fgsubname,t.fentry_fee fgamecoins
                 ,case when fitem_id = '0' then '银币'
                       when fitem_id = '1' then '金条' else fitem_id end fproduct_type
            from stage.join_gameparty_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = date_add('%(statdate)s',1) and flts_at <= concat(dt,' 01:00:00')
          union all
          select t.fbpid, fuid,t.flts_at,tt.fhallfsk,tt.fplatformfsk,fgame_id,t.fpname,t.fname fsubname,t.fsubname fgsubname,t.fentry_fee fgamecoins
                 ,case when fitem_id = '0' then '银币'
                       when fitem_id = '1' then '金条' else fitem_id end fproduct_type
            from stage.join_gameparty_stg t
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where dt = date_sub('%(statdate)s',1) and flts_at >= concat(dt,' 23:00:00');
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """-- 从支付数据出发，将每一条支付数据关联到时间在其前面且距离其最近的一条记录
            drop table if exists work.bud_pay_income_sharing_party_tmp_3_%(statdatenum)s;
          create table work.bud_pay_income_sharing_party_tmp_3_%(statdatenum)s as
        select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
            fdate, pay_time,
            fcoins_num,frate,fproduct_name,fpamount_usd,
            flts_at,party_time,
            coalesce(fgame_id, -13658) fgame_id,
            coalesce(fpname, '-13658') fpname,
            coalesce(fsubname, '-13658') fsubname,
            coalesce(fgsubname, '-13658') fgsubname,
            fgamecoins,
            type
        from
        (
            select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
                fdate, pay_time,fgame_id,
                fcoins_num,frate,fproduct_name,fpamount_usd,
                flts_at,party_time,fpname,fsubname,fgsubname,fgamecoins,type
                -- 每条支付记录做一个分区，后面会确保每条支付记录只取一次，这样收入才不会被放大
                -------------------------------------------------------------------------------
                -- 取支付前时间最近的一条牌局流水
                ,row_number() over(partition by fuid,fplatformfsk,type, pay_id order by case when interval_time < 0 then interval_time else -864000 end desc) as rank
                -------------------------------------------------------------------------------
            from
            (
                select ta.fuid,ta.fhallfsk,ta.fplatformfsk,ta.fproduct_type,ta.forder_id,
                    ta.fdate, ta.pay_time,
                    ta.fcoins_num,ta.frate,ta.fproduct_name,ta.fpamount_usd,ta.pay_id,
                    tb.flts_at,tb.party_time,fgame_id,tb.fpname,tb.fsubname,tb.fgsubname,tb.fgamecoins, 1 type,
                    -- 求出支付和后面牌局的时间差
                    party_time-pay_time as interval_time
                from
                (
                    -- 付费用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
                        fdate, unix_timestamp(fdate) as pay_time,
                        fcoins_num,frate,fproduct_name,fpamount_usd,
                        row_number() over(partition by fuid,fplatformfsk order by fdate) as pay_id -- 每条支付记录做个标记
                    from work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s
                   where fproduct_type in ('银币','金条')
                ) ta
                left join
                (
                    -- 牌局用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,
                        flts_at, unix_timestamp(flts_at) as party_time,fgame_id,
                        fpname,fsubname,fgsubname,fgamecoins
                    from work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s
                ) tb
                on ta.fplatformfsk = tb.fplatformfsk
                and ta.fuid = tb.fuid
                and ta.fproduct_type = tb.fproduct_type
               union all
                select ta.fuid,ta.fhallfsk,ta.fplatformfsk,ta.fproduct_type,ta.forder_id,
                    ta.fdate, ta.pay_time,
                    ta.fcoins_num,ta.frate,ta.fproduct_name,ta.fpamount_usd,ta.pay_id,
                    tb.flts_at,tb.party_time,fgame_id,tb.fpname,tb.fsubname,tb.fgsubname,tb.fgamecoins, 2 type,
                    -- 求出支付和后面牌局的时间差
                    party_time-pay_time as interval_time
                from
                (
                    -- 付费用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
                        fdate, unix_timestamp(fdate) as pay_time,
                        fcoins_num,frate,fproduct_name,fpamount_usd,
                        row_number() over(partition by fuid,fplatformfsk order by fdate) as pay_id -- 每条支付记录做个标记
                    from work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s
                ) ta
                left join
                (
                    -- 牌局用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,
                        flts_at, unix_timestamp(flts_at) as party_time,fgame_id,
                        fpname,fsubname,fgsubname,fgamecoins
                    from work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s
                ) tb
                on ta.fplatformfsk = tb.fplatformfsk
                and ta.fuid = tb.fuid
            ) t1
        ) t2
        where rank = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """-- 从支付数据出发，将每一条支付数据关联到时间在其后面且距离其最近的一条记录
            drop table if exists work.bud_pay_income_sharing_party_tmp_4_%(statdatenum)s;
          create table work.bud_pay_income_sharing_party_tmp_4_%(statdatenum)s as
        select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
            fdate, pay_time,
            fcoins_num,frate,fproduct_name,fpamount_usd,
            flts_at,party_time,
            coalesce(fgame_id, -13658) fgame_id,
            coalesce(fpname, '-13658') fpname,
            coalesce(fsubname, '-13658') fsubname,
            coalesce(fgsubname, '-13658') fgsubname,
            fgamecoins,
            type
        from
        (
            select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
                fdate, pay_time,fgame_id,
                fcoins_num,frate,fproduct_name,fpamount_usd,
                flts_at,party_time,fpname,fsubname,fgsubname,fgamecoins,type
                -- 每条支付记录做一个分区，后面会确保每条支付记录只取一次，这样收入才不会被放大
                -------------------------------------------------------------------------------
                -- 取支付后时间最近的一条牌局流水
                ,row_number() over(partition by fuid,fplatformfsk, pay_id order by case when interval_time > 0 then interval_time else 864000 end) as rank
                -------------------------------------------------------------------------------
            from
            (
                select ta.fuid,ta.fhallfsk,ta.fplatformfsk,ta.fproduct_type,ta.forder_id,
                    ta.fdate, ta.pay_time,
                    ta.fcoins_num,ta.frate,ta.fproduct_name,ta.fpamount_usd,ta.pay_id,
                    tb.flts_at,tb.party_time,fgame_id,tb.fpname,tb.fsubname,tb.fgsubname,tb.fgamecoins, 1 type,
                    -- 求出支付和后面牌局的时间差
                    party_time-pay_time as interval_time
                from
                (
                    -- 付费用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
                        fdate, unix_timestamp(fdate) as pay_time,
                        fcoins_num,frate,fproduct_name,fpamount_usd,
                        row_number() over(partition by fuid,fplatformfsk order by fdate) as pay_id -- 每条支付记录做个标记
                    from work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s
                   where fproduct_type in ('银币','金条')
                ) ta
                left join
                (
                    -- 牌局用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,
                        flts_at, unix_timestamp(flts_at) as party_time,fgame_id,
                        fpname,fsubname,fgsubname,fgamecoins
                    from work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s
                ) tb
                on ta.fplatformfsk = tb.fplatformfsk
                and ta.fuid = tb.fuid
                and ta.fproduct_type = tb.fproduct_type
               union all
                select ta.fuid,ta.fhallfsk,ta.fplatformfsk,ta.fproduct_type,ta.forder_id,
                    ta.fdate, ta.pay_time,
                    ta.fcoins_num,ta.frate,ta.fproduct_name,ta.fpamount_usd,ta.pay_id,
                    tb.flts_at,tb.party_time,fgame_id,tb.fpname,tb.fsubname,tb.fgsubname,tb.fgamecoins, 2 type,
                    -- 求出支付和后面牌局的时间差
                    party_time-pay_time as interval_time
                from
                (
                    -- 付费用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,forder_id,
                        fdate, unix_timestamp(fdate) as pay_time,
                        fcoins_num,frate,fproduct_name,fpamount_usd,
                        row_number() over(partition by fuid,fplatformfsk order by fdate) as pay_id -- 每条支付记录做个标记
                    from work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s
                ) ta
                left join
                (
                    -- 牌局用户
                    select fuid,fhallfsk,fplatformfsk,fproduct_type,
                        flts_at, unix_timestamp(flts_at) as party_time,fgame_id,
                        fpname,fsubname,fgsubname,fgamecoins
                    from work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s
                ) tb
                on ta.fplatformfsk = tb.fplatformfsk
                and ta.fuid = tb.fuid
            ) t1
        ) t2
        where rank = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """--
            insert overwrite table bud_dm.bud_pay_income_sharing_party_uid  partition (dt = "%(statdate)s" )
          select '%(statdate)s' fdate
                 ,t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,coalesce(t1.fgame_id,t2.fgame_id,t3.fgame_id,t4.fgame_id,-13658) fsubgamefsk
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fuid
                 ,coalesce(t1.fpname, t2.fpname, t3.fpname, t4.fpname, '-13658') fpname
                 ,coalesce(t1.fsubname, t2.fsubname, t3.fsubname, t4.fsubname, '-13658') fsubname
                 ,coalesce(t1.fgsubname, t2.fgsubname, t3.fgsubname, t4.fgsubname, '-13658') fgsubname
                 ,case when t1.forder_id is not null then 'after'
                       when t2.forder_id is not null then 'before'
                       when t3.forder_id is not null then 'after'
                       when t4.forder_id is not null then 'before'
                       else 'unknow' end fshare_type
                 ,t.fproduct_type fcoins_type
                 ,t.fcoins_num fnumber
                 ,coalesce(t1.fpamount_usd, t2.fpamount_usd, t3.fpamount_usd, t4.fpamount_usd, t.fpamount_usd) fsharing_income
                 ,0 fsharing_rate
            from work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s t
            left join work.bud_pay_income_sharing_party_tmp_4_%(statdatenum)s t1 --先取付费后-分货币
              on t.forder_id = t1.forder_id
             and t.fproduct_type = t1.fproduct_type
             and t1.type = 1
            left join work.bud_pay_income_sharing_party_tmp_3_%(statdatenum)s t2 --再取付费前-分货币
              on t.forder_id = t2.forder_id
             and t.fproduct_type = t2.fproduct_type
             and t2.type = 1
            left join work.bud_pay_income_sharing_party_tmp_4_%(statdatenum)s t3 --先取付费后-不分货币
              on t.forder_id = t3.forder_id
             and t3.type = 2
            left join work.bud_pay_income_sharing_party_tmp_3_%(statdatenum)s t4 --再取付费前-不分货币
              on t.forder_id = t4.forder_id
             and t4.type = 2
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """--
            insert overwrite table bud_dm.bud_pay_income_sharing_party  partition (dt = "%(statdate)s" )
          select '%(statdate)s' fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fsubgamefsk
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fpname
                 ,fsubname
                 ,fgsubname
                 ,fshare_type
                 ,fcoins_type
                 ,sum(fnumber) fnumber
                 ,sum(fsharing_income) fsharing_income
                 ,sum(fsharing_rate) fsharing_rate
            from bud_dm.bud_pay_income_sharing_party_uid t
           where dt = "%(statdate)s"
           group by fgamefsk
                    ,fplatformfsk
                    ,fhallfsk
                    ,fsubgamefsk
                    ,fterminaltypefsk
                    ,fversionfsk
                    ,fpname
                    ,fsubname
                    ,fgsubname
                    ,fshare_type
                    ,fcoins_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_pay_income_sharing_party_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_party_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_party_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_party_tmp_4_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_pay_income_sharing_party(sys.argv[1:])
a()
