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


class agg_bud_pay_income_sharing_charge(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_pay_income_sharing_charge (
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


        create table if not exists bud_dm.bud_pay_income_sharing_charge_uid (
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
        hql = """--总金条银币比例
            drop table if exists work.bud_pay_income_sharing_charge_tmp_1_%(statdatenum)s;
          create table work.bud_pay_income_sharing_charge_tmp_1_%(statdatenum)s as
          select fgamefsk,t.fuid,
                 case when tt.fgamefsk = 4132314431 and fproduct_name like '%%币%%' then '银币'
                      when tt.fgamefsk = 4132314431 and fproduct_name like '%%幣%%' then '银币'
                      when tt.fgamefsk = 4132314431 and t1.fuid is not null and fproduct_name like '%%金%%' then '比赛金条'
                      when tt.fgamefsk = 4132314431 and t1.fuid is null and fproduct_name like '%%金%%' then '非比赛金条'
                      when tt.fgamefsk = 4132314431 then concat('其他_',fproduct_name)
                      else '其他' end fproduct_type,
                 sum(fpamount_usd) fpay_income
            from stage.payment_stream_stg t
            left join (select distinct fbpid, fuid
                         from dim.join_gameparty t1
                        where t1.dt = '%(statdate)s'
                          and t1.fitem_id = '1'
                          and t1.fentry_fee > 0
                 ) t1
              on t1.fbpid = t.fbpid
             and t1.fuid = t.fuid
            join dim.bpid_map tt
              on t.fbpid = tt.fbpid
           where t.dt = '%(statdate)s'
           group by fgamefsk,t.fuid,
                 case when tt.fgamefsk = 4132314431 and fproduct_name like '%%币%%' then '银币'
                      when tt.fgamefsk = 4132314431 and fproduct_name like '%%幣%%' then '银币'
                      when tt.fgamefsk = 4132314431 and t1.fuid is not null and fproduct_name like '%%金%%' then '比赛金条'
                      when tt.fgamefsk = 4132314431 and t1.fuid is null and fproduct_name like '%%金%%' then '非比赛金条'
                      when tt.fgamefsk = 4132314431 then concat('其他_',fproduct_name)
                      else '其他' end;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--台费
            drop table if exists work.bud_pay_income_sharing_charge_tmp_2_%(statdatenum)s;
          create table work.bud_pay_income_sharing_charge_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpname
                 ,t1.fsubname
                 ,t1.fgsubname
                 ,t1.fuid
                 ,case when tt.fgamefsk = 4132314431 and fcoins_type = '1' then '银币'
                       when tt.fgamefsk = 4132314431 and fcoins_type = '11' then '非比赛金条'
                  else fcoins_type end fcoins_type
                 ,sum(fcharge+0.0001) fcharge
            from stage.user_gameparty_stg t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt='%(statdate)s'
           group by tt.fgamefsk
                   ,tt.fplatformfsk
                   ,tt.fhallfsk
                   ,coalesce(t1.fgame_id,cast (0 as bigint))
                   ,tt.fterminaltypefsk
                   ,tt.fversionfsk
                   ,t1.fpname
                   ,t1.fsubname
                   ,t1.fgsubname
                   ,t1.fuid
                   ,case when tt.fgamefsk = 4132314431 and fcoins_type = '1' then '银币'
                         when tt.fgamefsk = 4132314431 and fcoins_type = '11' then '非比赛金条' else fcoins_type end
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名、复活、抽水
            drop table if exists work.bud_pay_income_sharing_charge_tmp_3_%(statdatenum)s;
          create table work.bud_pay_income_sharing_charge_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpname
                 ,t1.fsubname
                 ,t1.fgsubname
                 ,t1.fuid
                 ,case when tt.fgamefsk = 4132314431 and fitem_id = '0' then '银币'
                       when tt.fgamefsk = 4132314431 and fitem_id = '1' then '比赛金条' end fcoins_type
                 ,sum(fitem_num+0.0001) fcharge
            from stage.match_economy_stg t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt='%(statdate)s'
             and fio_type = 2              --消耗
             and fact_id in ('1','5','6')  --报名、复活、抽水
           group by  tt.fgamefsk
                    ,tt.fplatformfsk
                    ,tt.fhallfsk
                    ,coalesce(t1.fgame_id,cast (0 as bigint))
                    ,tt.fterminaltypefsk
                    ,tt.fversionfsk
                    ,t1.fpname
                    ,t1.fsubname
                    ,t1.fgsubname
                    ,t1.fuid
                    ,case when tt.fgamefsk = 4132314431 and fitem_id = '0' then '银币'
                          when tt.fgamefsk = 4132314431 and fitem_id = '1' then '比赛金条' end
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """--
            drop table if exists work.bud_pay_income_sharing_charge_tmp_4_%(statdatenum)s;
          create table work.bud_pay_income_sharing_charge_tmp_4_%(statdatenum)s as
          select  fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fpname
                 ,fsubname
                 ,fgsubname
                 ,fuid
                 ,fshare_type
                 ,fcoins_type
                 ,fcharge
                 ,fcharge/sum(fcharge) over(partition by fgamefsk,fcoins_type,fuid) frate
            from (select  fgamefsk
                         ,fplatformfsk
                         ,fhallfsk
                         ,fgame_id
                         ,fterminaltypefsk
                         ,fversionfsk
                         ,fpname
                         ,fsubname
                         ,fgsubname
                         ,fuid
                         ,'台费' fshare_type
                         ,fcoins_type
                         ,fcharge
                    from work.bud_pay_income_sharing_charge_tmp_2_%(statdatenum)s t
                   where t.fgamefsk = 4132314431
                   union all
                  select  fgamefsk
                         ,fplatformfsk
                         ,fhallfsk
                         ,fgame_id
                         ,fterminaltypefsk
                         ,fversionfsk
                         ,fpname
                         ,fsubname
                         ,fgsubname
                         ,fuid
                         ,'台费' fshare_type
                         ,'0' fcoins_type
                         ,sum(fcharge) fcharge
                    from work.bud_pay_income_sharing_charge_tmp_2_%(statdatenum)s t
                   where t.fgamefsk <> 4132314431
                   group by fgamefsk
                            ,fplatformfsk
                            ,fhallfsk
                            ,fgame_id
                            ,fterminaltypefsk
                            ,fversionfsk
                            ,fpname
                            ,fsubname
                            ,fgsubname
                            ,fuid
                   union all
                  select  fgamefsk
                         ,fplatformfsk
                         ,fhallfsk
                         ,fgame_id
                         ,fterminaltypefsk
                         ,fversionfsk
                         ,fpname
                         ,fsubname
                         ,fgsubname
                         ,fuid
                         ,'报名' fshare_type
                         ,fcoins_type
                         ,fcharge
                    from work.bud_pay_income_sharing_charge_tmp_3_%(statdatenum)s t
                 ) t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """--
            drop table if exists work.bud_pay_income_sharing_charge_tmp_5_%(statdatenum)s;
          create table work.bud_pay_income_sharing_charge_tmp_5_%(statdatenum)s as
          select  fgamefsk
                 ,fpay_income
                 ,fproduct_type
                 ,fuid
                 ,fpay_income/sum(fpay_income) over(partition by fgamefsk,fuid) frate
            from work.bud_pay_income_sharing_charge_tmp_1_%(statdatenum)s t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """--
            insert overwrite table bud_dm.bud_pay_income_sharing_charge_uid  partition (dt = "%(statdate)s" )
          select '%(statdate)s' fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fsubgamefsk
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fuid
                 ,fpname
                 ,fsubname
                 ,fgsubname
                 ,fshare_type
                 ,fcoins_type
                 ,fnumber
                 ,fsharing_income
                 ,fsharing_rate
            from (select fgamefsk
                         ,-13658 fplatformfsk
                         ,-13658 fhallfsk
                         ,-13658 fsubgamefsk
                         ,-13658 fterminaltypefsk
                         ,-13658 fversionfsk
                         ,fuid
                         ,'-13658' fpname
                         ,'-13658' fsubname
                         ,'-13658' fgsubname
                         ,'其他' fshare_type
                         ,fproduct_type fcoins_type
                         ,fpay_income fnumber
                         ,fpay_income fsharing_income
                         ,frate fsharing_rate
                    from work.bud_pay_income_sharing_charge_tmp_5_%(statdatenum)s t
                   where t.fgamefsk = 4132314431 and fproduct_type like '其他_%%'
                   union all
                   select t1.fgamefsk
                         ,coalesce(t2.fplatformfsk, -13658) fplatformfsk
                         ,coalesce(t2.fhallfsk, -13658) fhallfsk
                         ,coalesce(t2.fgame_id, -13658) fsubgamefsk
                         ,coalesce(t2.fterminaltypefsk, -13658) fterminaltypefsk
                         ,coalesce(t2.fversionfsk, -13658) fversionfsk
                         ,t1.fuid
                         ,coalesce(t2.fpname, '-13658') fpname
                         ,coalesce(t2.fsubname, '-13658') fsubname
                         ,coalesce(t2.fgsubname, '-13658') fgsubname
                         ,fshare_type
                         ,fcoins_type
                         ,fcharge fnumber
                         ,coalesce(t2.frate,1) * t1.fpay_income fsharing_income
                         ,round(t1.frate,10) * round(coalesce(t2.frate,1),10) fsharing_rate
                    from work.bud_pay_income_sharing_charge_tmp_5_%(statdatenum)s t1
                    left join work.bud_pay_income_sharing_charge_tmp_4_%(statdatenum)s t2
                      on t1.fgamefsk = t2.fgamefsk
                     and t2.fcoins_type = t1.fproduct_type
                     and t1.fuid = t2.fuid
                   where t1.fgamefsk = 4132314431 and fproduct_type in ('银币', '比赛金条', '非比赛金条')
                   union all
                   select t1.fgamefsk
                         ,coalesce(t2.fplatformfsk, -13658) fplatformfsk
                         ,coalesce(t2.fhallfsk, -13658) fhallfsk
                         ,coalesce(t2.fgame_id, -13658) fsubgamefsk
                         ,coalesce(t2.fterminaltypefsk, -13658) fterminaltypefsk
                         ,coalesce(t2.fversionfsk, -13658) fversionfsk
                         ,t1.fuid
                         ,coalesce(t2.fpname, '-13658') fpname
                         ,coalesce(t2.fsubname, '-13658') fsubname
                         ,coalesce(t2.fgsubname, '-13658') fgsubname
                         ,fshare_type
                         ,fcoins_type
                         ,fcharge fnumber
                         ,coalesce(t2.frate,1) * t1.fpay_income fsharing_income
                         ,round(t1.frate,10) * round(coalesce(t2.frate,1),10) fsharing_rate
                    from work.bud_pay_income_sharing_charge_tmp_5_%(statdatenum)s t1
                    left join work.bud_pay_income_sharing_charge_tmp_4_%(statdatenum)s t2
                      on t1.fgamefsk = t2.fgamefsk
                     and t1.fuid = t2.fuid
                   where t1.fgamefsk <> 4132314431
                     ) t
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """--
            insert overwrite table bud_dm.bud_pay_income_sharing_charge  partition (dt = "%(statdate)s" )
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
            from bud_dm.bud_pay_income_sharing_charge_uid t
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
        hql = """drop table if exists work.bud_pay_income_sharing_charge_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_charge_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_charge_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_charge_tmp_4_%(statdatenum)s;
                 drop table if exists work.bud_pay_income_sharing_charge_tmp_5_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_pay_income_sharing_charge(sys.argv[1:])
a()
