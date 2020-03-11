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


class agg_province_pay_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_付费相关
        create table if not exists dcnew.province_pay_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fversionfsk         bigint,
                fchannelcode        bigint,
                fprovince           varchar(64)     comment '省份',
                forder_cnt          bigint          comment '下单数',
                fpay_unum           bigint          comment '付费用户数',
                fpay_cnt            bigint          comment '付费次数',
                fpay_income         decimal(20,2)   comment '付费金额',
                ff_pay_uunm         bigint          comment '首付用户数',
                ffpay_cnt           bigint          comment '首付次数',
                ffpay_income        decimal(20,2)   comment '首付金额'
                )comment '分省数据_付费相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_pay_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        #四组组合，共14种
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fplatformfsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk) )
                union all """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fprovince),
                               (fgamefsk, fplatformfsk, fgame_id, fprovince) )
                union all """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fprovince),
                               (fgamefsk, fplatformfsk, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fprovince) ) """

        query = {'extend_group_1':extend_group_1,
                 'extend_group_2':extend_group_2,
                 'extend_group_3':extend_group_3,
                 'extend_group_4':extend_group_4,
                 'null_str_group_rule' : sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule' : sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum'         : """%(statdatenum)s""" }

        hql = """
        drop table if exists work.province_pay_tmp_1_%(statdatenum)s;
        create table work.province_pay_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(d) */ distinct c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id,
                 t1.forder_id,
                 t1.fplatform_uid,
                 coalesce(t3.fcoins_num * t3.frate,0) fincome,
                 case when t3.forder_id is not null then 1 else 0 end is_sus,
                 case when t4.fplatform_uid is not null then 1 else 0 end is_first
            from stage.payment_stream_all_stg t1  --所有订单
            left join stage.user_generate_order_stg t2
              on t1.forder_id = t2.forder_id
             and t2.dt = '%(statdate)s'
            left join stage.payment_stream_stg t3  --成功订单
              on t1.forder_id = t3.forder_id
             and t3.dt = '%(statdate)s'
            left join dim.user_pay t4  --首付
              on t3.fbpid = t4.fbpid
             and t3.fplatform_uid = t4.fplatform_uid
             and t4.dt = '%(statdate)s'
            join dim.bpid_map c
              on t1.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        #汇总
        base_hql = """
                select fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       count(distinct forder_id) forder_cnt,
                       count(distinct case when is_sus = 1 then fplatform_uid end) fpay_unum,
                       count(distinct case when is_sus = 1 then forder_id end) fpay_cnt,
                       sum(case when is_sus = 1 then fincome end) fpay_income,
                       count(distinct case when is_first = 1 then fplatform_uid end) ff_pay_uunm,
                       count(distinct case when is_first = 1 then forder_id end) ffpay_cnt,
                       sum(case when is_first = 1 then round(fincome,4) end) ffpay_income
                  from work.province_pay_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        #组合
        hql = ("""
                  drop table if exists work.province_pay_tmp_%(statdatenum)s;
                create table work.province_pay_tmp_%(statdatenum)s as """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """insert overwrite table dcnew.province_pay_info
                    partition(dt='%(statdate)s')
                  select '%(statdate)s' fdate,
                         fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         -1 fversionfsk,
                         -1 fchannel_code,
                         fprovince,
                         nvl(sum(forder_cnt),0) forder_cnt,
                         nvl(sum(fpay_unum),0) fpay_unum,
                         nvl(sum(fpay_cnt),0) fpay_cnt,
                         nvl(sum(fpay_income),0) fpay_income,
                         nvl(sum(ff_pay_uunm),0) ff_pay_uunm,
                         nvl(sum(ffpay_cnt),0) ffpay_cnt,
                         nvl(sum(ffpay_income),0) ffpay_income
                    from work.province_pay_tmp_%(statdatenum)s gs
                   group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.province_pay_tmp_1_%(statdatenum)s;
                 drop table if exists work.province_pay_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res



#生成统计实例
a = agg_province_pay_info(sys.argv[1:])
a()
