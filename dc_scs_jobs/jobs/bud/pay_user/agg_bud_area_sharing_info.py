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


class agg_bud_area_sharing_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_area_sharing_info (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fhallfsk            bigint,
            frel_income         decimal(20,2)  comment '实收流水',
            fboyaa_sharing      decimal(20,2)  comment '博雅分成',
            farea_sharing       decimal(20,2)  comment '地面分成',
            fmatch_bill         decimal(20,2)  comment '线上（比赛）话费',
            fmatch_goods        decimal(20,2)  comment '线上（比赛）实物',
            factivity_bill      decimal(20,2)  comment '线上（活动）话费',
            factivity_goods     decimal(20,2)  comment '线上（活动）实物',
            ftake_bill          decimal(20,2)  comment '代理商充值提现（话费）',
            farea_rel_income    decimal(20,2)  comment '地面应收',
            fbill_to_gold       decimal(20,2)  comment '话费回兑金条',
            fbill_purchase_cost decimal(20,2)  comment '话费实际采购成本'
            )comment '地面合作公司分成数据'
            partitioned by(dt date)
        location '/dw/bud_dm/bud_area_sharing_info';
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

        hql = """--更新商品表
          insert into table dim.match_goods_dim
        select distinct t1.fitem_id,t3.fname fitem_name,case when fitem_name like '%%话费%%' then '话费' end ftype,1 status,t3.fcost fcost,t3.fpurchase_cost fpurchase_cost
          from (select distinct fitem_id
                  from stage.match_economy_stg t1
                  join dim.bpid_map_bud tt
                    on t1.fbpid = tt.fbpid
                    and tt.fgamefsk = 4132314431
                 where t1.dt <= '%(ld_1month_ago_end)s'
                   and t1.dt >= '%(ld_1month_ago_begin)s' and fpurchase_cost > 0
                union all
                select distinct fitem_id
                  from stage.game_activity_stg t1
                  join dim.bpid_map_bud tt
                    on t1.fbpid = tt.fbpid
                    and tt.fgamefsk = 4132314431
                 where t1.dt <= '%(ld_1month_ago_end)s'
                   and t1.dt >= '%(ld_1month_ago_begin)s' and fpurchase_cost > 0) t1
          left join dim.match_goods_dim t2
            on t1.fitem_id = t2.fitem_id
          left join dim.goods_dim t3
            on t1.fitem_id = t3.id
         where t2.fitem_id is null;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--实收流水\博雅分成\地面分成
        insert overwrite table bud_dm.bud_area_sharing_info_1_relincome partition (dt = '%(ld_1month_ago_begin)s')
        select '%(ld_1month_ago_begin)s' fdate
               ,tt.fgamefsk
               ,tt.fplatformfsk
               ,tt.fhallfsk
               ,t1.fm_name
               ,t1.companyname
               ,t2.sharing_rate
               ,t2.bad_rate
               ,sum(coalesce(fpamount_usd,0)) fpamount_usd
               ,sum(coalesce(fpamount_usd,0)*6.4331) fpamount
               ,sum(coalesce(fpamount_usd,0)*coalesce(t2.sharing_rate, 1)*(1-coalesce(t2.bad_rate,0))) frel_income_usd
               ,sum(coalesce(fpamount_usd,0)*coalesce(t2.sharing_rate, 1)*(1-coalesce(t2.bad_rate,0)) * 0.3) fboyaa_sharing_usd
               ,sum(coalesce(fpamount_usd,0)*coalesce(t2.sharing_rate, 1)*(1-coalesce(t2.bad_rate,0)) * 0.7) farea_sharing_usd
               ,sum(coalesce(fpamount_usd,0)*6.4331*coalesce(t2.sharing_rate, 1)*(1-coalesce(t2.bad_rate,0))) frel_income
               ,sum(coalesce(fpamount_usd,0)*6.4331*coalesce(t2.sharing_rate, 1)*(1-coalesce(t2.bad_rate,0)) * 0.3) fboyaa_sharing
               ,sum(coalesce(fpamount_usd,0)*6.4331*coalesce(t2.sharing_rate, 1)*(1-coalesce(t2.bad_rate,0)) * 0.7) farea_sharing
          from (select t1.bpid fbpid,t.income,t.income_usd fpamount_usd,t.pamount_rate,t2.pmodename fm_name,t3.companyname
                  from stage.paycenter_finance t
                  join stage.paycenter_apps t1
                    on t.appid = t1.appid
                   and t.sid = t1.sid
                   and t1.dt = '%(statdate)s'
                  join stage.paycenter_chanel t2
                    on t.pmode = t2.pmode
                   and t.companyid = t2.companyid
                   and t.sid = t2.sid
                   and t2.use_status = 0
                   and t2.dt = '%(statdate)s'
                  left join stage.paycenter_company t3
                    on t.companyid = t3.companyid
                   and t3.dt = '%(statdate)s'
                  join dim.bpid_map tt
                    on t1.bpid = tt.fbpid
                   and tt.fgamefsk = 4132314431
                 where t.dt = '%(statdate)s' and ptime = substr(date_sub('%(statdate)s',3),1,7)
               ) t1
          left join dim.dfqp_channel_sharing_rate t2
            on t1.fm_name = t2.fm_name
          join dim.bpid_map tt
            on t1.fbpid = tt.fbpid
           and tt.fgamefsk = 4132314431
         group by tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fm_name
                 ,t1.companyname
                 ,t2.sharing_rate
                 ,t2.bad_rate
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取需要分摊的比赛
            drop table if exists work.bud_area_sharing_info_tmp_2_%(statdatenum)s;
          create table work.bud_area_sharing_info_tmp_2_%(statdatenum)s as
        select t1.fgsubname
               ,t1.fpname
               ,coalesce(t2.ftype, '实物') ftype
               ,t1.fitem_id
               ,t2.fitem_name
               ,sum(t1.fitem_num) fitem_num
               ,sum(t1.fpurchase_cost) fpurchase_cost         --物品成本RMB
          from stage.match_economy_stg t1
          left join dim.match_goods_dim t2
            on t1.fitem_id = t2.fitem_id
          join dim.bpid_map tt
            on t1.fbpid = tt.fbpid
           and tt.fgamefsk = 4132314431
         where t1.dt <= '%(ld_1month_ago_end)s'
           and t1.dt >= '%(ld_1month_ago_begin)s'
           and fio_type = '1'
           and t1.fpurchase_cost > 0
         group by t1.fgsubname
                  ,t1.fpname
                  ,t1.fitem_id
                  ,t2.fitem_name
                  ,coalesce(t2.ftype, '实物')
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--match_log_id正常上报的
        insert overwrite table bud_dm.bud_area_sharing_info_2_relincome partition (dt = '%(ld_1month_ago_begin)s')
        select '%(ld_1month_ago_begin)s' fdate,
               t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgsubname,
               t1.fparty_type,t1.fpname,t3.ftype,t3.fitem_id,t3.fitem_name,
               t3.fitem_num,t3.fpurchase_cost,fmatch_cnt,fmatch_all
          from (select t1.fgamefsk,
                       t1.fplatformfsk,
                       t1.fhallfsk,
                       t1.fgsubname,
                       t1.fpname,
                       t1.fparty_type,
                       fmatch_cnt,
                       sum(fmatch_cnt) over(partition by t1.fparty_type,t1.fpname,t1.fgsubname) fmatch_all
                  from (select t1.fgamefsk,
                               t1.fplatformfsk,
                               t1.fhallfsk,
                               t1.fgsubname,
                               t1.fpname,
                               t1.fparty_type,
                               sum(fmatch_cnt) fmatch_cnt
                          from bud_dm.bud_user_match_gsubgame_detail t1
                         where t1.dt <= '%(ld_1month_ago_end)s'
                           and t1.dt >= '%(ld_1month_ago_begin)s'
                           and t1.fgamefsk = 4132314431
                           and t1.fsubgamefsk = -21379
                           and t1.fhallfsk <> -21379
                           and t1.fterminaltypefsk = -21379
                           and t1.fversionfsk = -21379
                           and t1.fgsubname <> '-21379'
                           and t1.fpname <> '-21379'
                         group by t1.fgamefsk,
                                  t1.fplatformfsk,
                                  t1.fhallfsk,
                                  t1.fgsubname,
                                  t1.fpname,
                                  t1.fparty_type
                       ) t1
             ) t1
          join work.bud_area_sharing_info_tmp_2_%(statdatenum)s t3
            on t1.fgsubname = t3.fgsubname
           and t1.fpname = t3.fpname
         where t1.fmatch_cnt > 0
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--match_log_id未正常上报的
        insert into table bud_dm.bud_area_sharing_info_2_relincome partition (dt = '%(ld_1month_ago_begin)s')
        select '%(ld_1month_ago_begin)s' fdate,
               t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgsubname,
               t1.fparty_type,t1.fpname,t3.ftype,t3.fitem_id,t3.fitem_name,
               t3.fitem_num,t3.fpurchase_cost,fmatch_cnt,fmatch_all
          from (select t1.fgamefsk,
                       t1.fplatformfsk,
                       t1.fhallfsk,
                       t1.fgsubname,
                       t1.fpname,
                       t1.fparty_type,
                       fmatch_cnt,
                       sum(fmatch_cnt) over(partition by t1.fparty_type,t1.fpname,t1.fgsubname) fmatch_all
                  from (select fgamefsk
                               ,fplatformfsk
                               ,fhallfsk
                               ,fgsubname
                               ,fpname
                               ,fparty_type
                               ,count(distinct concat_ws('0', fmatch_id, cast (fuid as string))) fmatch_cnt
                          from stage_dfqp.user_gameparty_stg t
                         where dt <= '%(ld_1month_ago_end)s'
                           and dt >= '%(ld_1month_ago_begin)s'
                           and coalesce(fmatch_log_id,'0') = '0'
                           and coalesce(fmatch_id,'0') <> '0'
                         group by fgamefsk
                                 ,fplatformfsk
                                 ,fhallfsk
                                 ,fgsubname
                                 ,fpname
                                 ,fparty_type
                       ) t1
               ) t1
          join (select t1.fgsubname,t1.fpname
                  from (select distinct fgsubname,fpname
                          from work.bud_area_sharing_info_tmp_2_%(statdatenum)s t
                       ) t1
                  left join (select distinct fgsubname,fpname
                               from bud_dm.bud_area_sharing_info_2_relincome t
                              where dt = '%(ld_1month_ago_begin)s'
                            ) t3
                    on t1.fgsubname = t3.fgsubname
                   and t1.fpname = t3.fpname
                 where t3.fpname is null
               ) t2
            on t1.fgsubname = t2.fgsubname
           and t1.fpname = t2.fpname
          join work.bud_area_sharing_info_tmp_2_%(statdatenum)s t3
            on t1.fgsubname = t3.fgsubname
           and t1.fpname = t3.fpname
         where t1.fmatch_cnt > 0
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取需要分摊的活动
        insert into table bud_dm.bud_area_sharing_info_3_relincome partition (dt = '%(ld_1month_ago_begin)s')
        select '%(ld_1month_ago_begin)s' fdate
               ,tt.fgamefsk,tt.fplatformfsk,tt.fhallfsk
               ,t1.fact_id
               ,coalesce(t2.ftype, '实物') ftype
               ,t1.fitem_id
               ,t2.fitem_name
               ,sum(t1.fitem_num) fitem_num
               ,sum(t1.fpurchase_cost) fpurchase_cost         --物品成本RMB
          from stage.game_activity_stg t1
          left join dim.match_goods_dim t2
            on t1.fitem_id = t2.fitem_id
          join dim.bpid_map_bud tt
            on t1.fbpid = tt.fbpid
           and tt.fgamefsk = 4132314431
         where t1.dt <= '%(ld_1month_ago_end)s'
           and t1.dt >= '%(ld_1month_ago_begin)s'
           and fio_type = '1'
           and t1.fpurchase_cost > 0
         group by tt.fgamefsk,tt.fplatformfsk,tt.fhallfsk,t1.fact_id
                  ,coalesce(t2.ftype, '实物')
                  ,t1.fitem_id
                  ,t2.fitem_name
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--员工提取的话费
        insert into table bud_dm.bud_area_sharing_info_4_relincome partition (dt = '%(ld_1month_ago_begin)s')
        select distinct '%(ld_1month_ago_begin)s' fdate
               ,tt.fgamefsk
               ,tt.fplatformfsk
               ,tt.fhallfsk
               ,fcztype      --充值类型(话费充值type为0,流量充值为1)
               ,fuid
               ,fsporderid   --sp商户订单号
               ,fordercash   --实际扣款金额
               ,ffee         --充值金额
               ,fmobile      --充值手机号码
          from stage.phone_bill_coupon_exchange_stg t1  --话费券兑换
          join dim.bpid_map_bud tt
            on t1.fbpid = tt.fbpid
         where t1.dt <= '%(ld_1month_ago_end)s'
           and t1.dt >= '%(ld_1month_ago_begin)s'
           and fstatus = 2  --充值成功
           and fspid =100003 --烈火
           and fext_1 in ('1','2') --兑换用户类型(1:代理商,2:推广员,3:玩家)
           and fext_2 = '2'        --兑换类型(1:大厅充值,2:代理商提现)
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取需要分摊的比赛
            insert overwrite table bud_dm.bud_area_sharing_info partition (dt = '%(ld_1month_ago_begin)s')
            select '%(ld_1month_ago_begin)s' fdate
                   ,fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,sum(frel_income) frel_income
                   ,sum(fboyaa_sharing) fboyaa_sharing
                   ,sum(farea_sharing) farea_sharing
                   ,sum(fmatch_bill) fmatch_bill
                   ,sum(fmatch_goods) fmatch_goods
                   ,sum(factivity_bill) factivity_bill
                   ,sum(factivity_goods) factivity_goods
                   ,sum(ftake_bill) ftake_bill
                   ,sum(farea_sharing)
                    - sum(fmatch_goods)
                    - sum(factivity_goods)
                    - sum(fmatch_bill+factivity_bill+ftake_bill) * 1.08
                    + sum(fbill_to_gold) farea_rel_income
                   ,sum(fbill_to_gold) fbill_to_gold
                   ,sum(fmatch_bill+factivity_bill+ftake_bill) * 1.08 fbill_purchase_cost
              from (select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,frel_income
                           ,fboyaa_sharing
                           ,farea_sharing
                           ,cast (0 as decimal(20,2)) fmatch_bill
                           ,cast (0 as decimal(20,2)) fmatch_goods
                           ,cast (0 as decimal(20,2)) factivity_bill
                           ,cast (0 as decimal(20,2)) factivity_goods
                           ,cast (0 as decimal(20,2)) ftake_bill
                           ,cast (0 as decimal(20,2)) fbill_to_gold
                      from bud_dm.bud_area_sharing_info_1_relincome
                     where dt = '%(ld_1month_ago_begin)s'
                     union all
                    select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,0 frel_income
                           ,0 fboyaa_sharing
                           ,0 farea_sharing
                           ,sum(case when ftype = '话费' then fpurchase_cost*fmatch_cnt/fmatch_all end) fmatch_bill
                           ,sum(case when ftype <> '话费' then fpurchase_cost*fmatch_cnt/fmatch_all end) fmatch_goods
                           ,cast (0 as decimal(20,2)) factivity_bill
                           ,cast (0 as decimal(20,2)) factivity_goods
                           ,cast (0 as decimal(20,2)) ftake_bill
                           ,cast (0 as decimal(20,2)) fbill_to_gold
                      from bud_dm.bud_area_sharing_info_2_relincome
                     where dt = '%(ld_1month_ago_begin)s'
                     group by fgamefsk,fplatformfsk,fhallfsk
                     union all
                    select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,0 frel_income
                           ,0 fboyaa_sharing
                           ,0 farea_sharing
                           ,0 fmatch_bill
                           ,0 fmatch_goods
                           ,sum(case when ftype = '话费' then fpurchase_cost end) factivity_bill
                           ,sum(case when ftype <> '话费' then fpurchase_cost end) factivity_goods
                           ,cast (0 as decimal(20,2)) ftake_bill
                           ,cast (0 as decimal(20,2)) fbill_to_gold
                      from bud_dm.bud_area_sharing_info_3_relincome
                     where dt = '%(ld_1month_ago_begin)s'
                     group by fgamefsk,fplatformfsk,fhallfsk
                     union all
                    select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,cast (0 as decimal(20,2)) frel_income
                           ,cast (0 as decimal(20,2)) fboyaa_sharing
                           ,cast (0 as decimal(20,2)) farea_sharing
                           ,cast (0 as decimal(20,2)) fmatch_bill
                           ,cast (0 as decimal(20,2)) fmatch_goods
                           ,cast (0 as decimal(20,2)) factivity_bill
                           ,cast (0 as decimal(20,2)) factivity_goods
                           ,sum(ffee) ftake_bill
                           ,cast (0 as decimal(20,2)) fbill_to_gold
                      from bud_dm.bud_area_sharing_info_4_relincome
                     where dt = '%(ld_1month_ago_begin)s'
                     group by fgamefsk,fplatformfsk,fhallfsk
                     union all
                    select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,cast (0 as decimal(20,2)) frel_income
                           ,cast (0 as decimal(20,2)) fboyaa_sharing
                           ,cast (0 as decimal(20,2)) farea_sharing
                           ,cast (0 as decimal(20,2)) fmatch_bill
                           ,cast (0 as decimal(20,2)) fmatch_goods
                           ,cast (0 as decimal(20,2)) factivity_bill
                           ,cast (0 as decimal(20,2)) factivity_goods
                           ,cast (0 as decimal(20,2)) ftake_bill
                           ,sum(fcurren_num)* 0.01 fbill_to_gold
                      from bud_dm.bud_currencies_detail t1
                     where t1.dt <= '%(ld_1month_ago_end)s'
                       and t1.dt >= '%(ld_1month_ago_begin)s'
                       and t1.fcointype = '11'
                       and t1.fact_type = '1'
                       and t1.fact_id = '122'
                       and t1.fhallfsk <> -21379
                       and t1.fversionfsk = -21379
                       and t1.fsubgamefsk = -21379
                       and t1.fgamefsk = 4132314431
                     group by fgamefsk,fplatformfsk,fhallfsk
                   ) t1
             group by fgamefsk,fplatformfsk,fhallfsk

         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 临时表不删以便核查
        # hql = """--删除临时表
        #     drop table if exists bud_dm.bud_area_sharing_info_1_relincome;
        #     drop table if exists work.bud_area_sharing_info_tmp_2_%(statdatenum)s;
        #     drop table if exists bud_dm.bud_area_sharing_info_2_relincome;
        #     drop table if exists bud_dm.bud_area_sharing_info_2_relincome;
        #     drop table if exists bud_dm.bud_area_sharing_info_3_relincome;
        #     drop table if exists bud_dm.bud_area_sharing_info_4_relincome;
        #  """
        # res = self.sql_exe(hql)
        # if res != 0:
        #     return res
        return res

# 生成统计实例
a = agg_bud_area_sharing_info(sys.argv[1:])
a()
