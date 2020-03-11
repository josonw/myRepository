#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_pay_cube(BaseStatModel):

    """地方棋牌付费多维表
     """
    def create_tab(self):

        hql = """
        create table if not exists veda.user_pay_cube
        (
          fhallfsk            bigint,
          fuid                bigint,              --用户游戏ID
          fpay_hour           int,                 --时间点
          fpay_scene          varchar(100)   comment '场景',
          fpay_scene_type     varchar(100)   comment '场景类型',
          fpay_scene_extra    varchar(100)   comment '场景扩展',
          fproduct_id         varchar(100)   comment '商品ID',
          fproduct_num        varchar(100)   comment '商品数量',
          fm_id               varchar(100)   comment '支付方式id',
          fpay_cnt            bigint         comment '付费次数',
          fpay_income         decimal(20,2)  comment '付费金额',
          fcoins_num          bigint         comment '原币金额',
          frate               decimal(20,7)  comment '原币汇率'
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


    def stat(self):

        hql = """
        insert overwrite table veda.user_pay_cube
        partition( dt="%(statdate)s" )
        select fhallfsk
               ,t1.fuid
               ,from_unixtime(floor(unix_timestamp(t1.fsucc_time) / 3600)* 3600,'HH') fpay_hour
               ,fpay_scene
               ,fpay_scene_type
               ,fpay_scene_extra
               ,CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END fp_id
               ,sum(t1.fp_num) fproduct_num
               ,t1.fm_id
               ,count(distinct t1.forder_id) fpay_cnt
               ,sum(t1.fpamount_usd) fpay_income
               ,sum(t1.fcoins_num) fcoins_num
               ,t1.frate
        from stage.payment_stream_stg t1
        left join stage.user_generate_order_stg t2
          on t1.forder_id = t2.forder_id
         and t2.dt = '%(statdate)s'
        join dim.bpid_map_bud tt
          on t1.fbpid = tt.fbpid
         and tt.fgamefsk = 4132314431
       where t1.dt = '%(statdate)s'
        group by fhallfsk
                 ,t1.fuid
                 ,fpay_scene
                 ,fpay_scene_type
                 ,fpay_scene_extra
                 ,CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END
                 ,t1.fm_id
                 ,t1.frate
                 ,from_unixtime(floor(unix_timestamp(t1.fsucc_time) / 3600)* 3600,'HH')
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists veda.dfqp_product_dim_tmp_%(statdatenum)s;
        create table veda.dfqp_product_dim_tmp_%(statdatenum)s as
        select fgamefsk,fplatformfsk,fp_id,fp_name,fname,fp_type from (
               SELECT fdate,fgamefsk,
                      fplatformfsk,
                      CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END fp_id,
                      CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_name ELSE fproduct_name END fp_name,
                      CASE WHEN coalesce(fproduct_id,'0')='0' THEN (CASE
                                                                    WHEN fp_type = 2 THEN concat('游戏币(', fp_num, ')')
                                                                    WHEN fp_type = 1 THEN concat('博雅币(', fp_num, ')')
                                                                    ELSE fp_name
                                                                END)
                       ELSE concat(fproduct_id,'_',fproduct_name)
                   END fname,
                   fp_type,
                   row_number() over(partition by fgamefsk,fplatformfsk,CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END order by fdate desc) as flag
            FROM stage.payment_stream_stg m
        join dim.bpid_map_bud n
          on m.fbpid = n.fbpid
         and n.fgamefsk = 4132314431
            WHERE dt = '%(statdate)s'
              AND (fproduct_id IS NOT NULL
                   OR (fp_id IS NOT NULL
                       AND fp_name IS NOT NULL))
            GROUP BY fdate,fgamefsk,
                     fplatformfsk,
                     CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END ,
                     CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_name ELSE fproduct_name END ,
                     CASE
                         WHEN coalesce(fproduct_id,'0')='0' THEN (CASE
                                                                      WHEN fp_type = 2 THEN concat('游戏币(', fp_num, ')')
                                                                      WHEN fp_type = 1 THEN concat('博雅币(', fp_num, ')')
                                                                      ELSE fp_name
                                                                  END)
                         ELSE concat(fproduct_id,'_',fproduct_name)
                     END ,
                     fp_type) aa
        where flag =1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table veda.dfqp_product_dim
        select distinct fgamefsk,fplatformfsk, fp_id, fp_name, fname,
               case when fp_name like '%%银币%%' then 2
                    when fp_name like '%%游戏币%%' then 2
                    when fp_name like '%%金条%%' then 1
                    when fp_name like '%%VIP%%' then 3 else ftype end ftype
          from (--当日新增的产品id
                select a.fgamefsk,a.fplatformfsk, a.fp_id, a.fp_name, a.fname, a.fp_type ftype
                  from veda.dfqp_product_dim_tmp_%(statdatenum)s a
                  left join veda.dfqp_product_dim b
                    on a.fgamefsk=b.fgamefsk
                   and a.fplatformfsk=b.fplatformfsk
                   and a.fp_id = b.fp_id
                 where b.fp_id is null
                 union all
                --更新之前的的产品名称
                select a.fgamefsk,a.fplatformfsk, a.fp_id, coalesce(b.fp_name,a.fp_name) fp_name, coalesce(b.fname,a.fname) fname, a.ftype fp_type
                  from veda.dfqp_product_dim a
                  left join veda.dfqp_product_dim_tmp_%(statdatenum)s b
                    on a.fgamefsk=b.fgamefsk
                   and a.fplatformfsk=b.fplatformfsk
                   and a.fp_id = b.fp_id
                  ) t ;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists veda.dfqp_product_dim_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 生成统计实例
a = load_user_pay_cube(sys.argv[1:])
a()
