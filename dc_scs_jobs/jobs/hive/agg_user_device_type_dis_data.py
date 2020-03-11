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


class agg_user_device_type_dis_data(BaseStatModel):
    def create_tab(self):

        hql = """
        --用户设备类型分布，包含终端型号、分辨率、系统类型、网络类型、运营商

        create table if not exists dcnew.user_device_type_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               ftrmnl_type         varchar(100)    comment '终端型号',
               freg_unum           bigint          comment '注册用户数',
               fact_unum           bigint          comment '活跃用户数',
               fpay_unum           bigint          comment '付费用户数'
               )comment '终端型号'
               partitioned by(dt date)
        location '/dw/dcnew/user_device_type_dis';

        create table if not exists dcnew.user_device_pixel_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               ftrmnl_pixel        varchar(100)    comment '终端分辨率',
               freg_unum           bigint          comment '注册用户数',
               fact_unum           bigint          comment '活跃用户数',
               fpay_unum           bigint          comment '付费用户数'
               )comment '终端分辨率'
               partitioned by(dt date)
        location '/dw/dcnew/user_device_pixel_dis';

        create table if not exists dcnew.user_device_os_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               ftrmnl_os           varchar(100)    comment '终端系统类型',
               freg_unum           bigint          comment '注册用户数',
               fact_unum           bigint          comment '活跃用户数',
               fpay_unum           bigint          comment '付费用户数'
               )comment '终端系统类型'
               partitioned by(dt date)
        location '/dw/dcnew/user_device_os_dis';

        create table if not exists dcnew.user_device_network_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               ftrmnl_network      varchar(100)    comment '终端网络类型',
               freg_unum           bigint          comment '注册用户数',
               fact_unum           bigint          comment '活跃用户数',
               fpay_unum           bigint          comment '付费用户数'
               )comment '终端网络类型'
               partitioned by(dt date)
        location '/dw/dcnew/user_device_network_dis';

        create table if not exists dcnew.user_device_operator_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               ftrmnl_operator     varchar(100)    comment '终端运营商类型',
               freg_unum           bigint          comment '注册用户数',
               fact_unum           bigint          comment '活跃用户数',
               fpay_unum           bigint          comment '付费用户数'
               )comment '终端运营商类型'
               partitioned by(dt date)
        location '/dw/dcnew/user_device_operator_dis';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['ftrmnl_type','ftrmnl_pixel','ftrmnl_os','ftrmnl_network','ftrmnl_operator'],
                        'groups':[[1, 0, 0, 0, 0],
                                  [0, 1, 0, 0, 0],
                                  [0, 0, 1, 0, 0],
                                  [0, 0, 0, 1, 0],
                                  [0, 0, 0, 0, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0: return res

        hql = """--取用户相关的设备类型
        drop table if exists work.device_type_tmp_b_%(statdatenum)s;
        create table work.device_type_tmp_b_%(statdatenum)s as
      select c.fgamefsk,c.fplatformfsk,c.fhallfsk,c.fterminaltypefsk,c.fversionfsk,c.hallmode,
             c.fgame_id,c.fchannel_code,c.fuid,
             case when c.ftrmnl_type = '-13658' then '其他' else c.ftrmnl_type end ftrmnl_type,
             case when c.ftrmnl_pixel = '-13658' then '其他' else c.ftrmnl_pixel end ftrmnl_pixel,
             case when c.ftrmnl_os = '-13658' then '其他' else c.ftrmnl_os end ftrmnl_os,
             case when c.ftrmnl_network = '-13658' then '其他' else c.ftrmnl_network end ftrmnl_network,
             case when c.ftrmnl_operator = '-13658' then '其他' else c.ftrmnl_operator end ftrmnl_operator,
             max(reg_f) reg_f,max(act_f) act_f,max(pay_f) pay_f
        from (select  c.fgamefsk,c.fplatformfsk,c.fhallfsk,c.fterminaltypefsk,c.fversionfsk,c.hallmode,
                     coalesce(t2.fgame_id,%(null_int_report)d) fgame_id,
                     coalesce(t2.fchannel_code,%(null_int_report)d) fchannel_code,
                     t1.fuid,
                     coalesce(t1.fm_dtype, '其他') ftrmnl_type,
                     coalesce(t1.fm_pixel, '其他') ftrmnl_pixel,
                     coalesce(t1.fm_os, '其他') ftrmnl_os,
                     coalesce(t1.fm_network, '其他') ftrmnl_network,
                     coalesce(t1.fm_operator, '其他') ftrmnl_operator,
                     1 reg_f,
                     0 act_f,
                     0 pay_f
                from dim.reg_user_main_additional t1
                join dim.bpid_map c
                  on t1.fbpid=c.fbpid
                left join dim.user_act t2
                  on t1.fbpid = t2.fbpid
                 and t1.fuid = t2.fuid
                 and t2.dt = '%(statdate)s'
               where t1.dt = '%(statdate)s'
               union all
              select /*+ MAPJOIN(c) */ c.fgamefsk,c.fplatformfsk,c.fhallfsk,c.fterminaltypefsk,c.fversionfsk,c.hallmode,
                     coalesce(t2.fgame_id,%(null_int_report)d) fgame_id,
                     coalesce(t2.fchannel_code,%(null_int_report)d) fchannel_code,
                     t3.fuid,
                     coalesce(t3.fm_dtype, '其他') ftrmnl_type,
                     coalesce(t3.fm_pixel, '其他') ftrmnl_pixel,
                     coalesce(t3.fm_os, '其他') ftrmnl_os,
                     coalesce(t3.fm_network, '其他') ftrmnl_network,
                     coalesce(t3.fm_operator, '其他') ftrmnl_operator,
                     0 reg_f,
                     1 act_f,
                     case when t4.fuid is not null then 1 else 0 end pay_f
                from dim.user_login_additional t3
                left join dim.user_pay_day t4
                  on t3.fbpid =t4.fbpid
                 and t3.fuid=t4.fuid
                 and t4.dt = '%(statdate)s'
                join dim.bpid_map c
                  on t3.fbpid=c.fbpid
                left join dim.user_act t2
                  on t3.fbpid = t2.fbpid
                 and t3.fuid = t2.fuid
                 and t3.dt = '%(statdate)s'
               where t3.dt = '%(statdate)s'
            ) c group by c.fgamefsk,c.fplatformfsk,c.fhallfsk,c.fterminaltypefsk,c.fversionfsk,c.hallmode,
                       c.fgame_id,c.fchannel_code,c.fuid,c.ftrmnl_type,c.ftrmnl_pixel,c.ftrmnl_os,c.ftrmnl_network,c.ftrmnl_operator
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(ftrmnl_type,%(null_str_group_rule)s) ftrmnl_type,
                       coalesce(ftrmnl_pixel,%(null_str_group_rule)s) ftrmnl_pixel,
                       coalesce(ftrmnl_os,%(null_str_group_rule)s) ftrmnl_os,
                       coalesce(ftrmnl_network,%(null_str_group_rule)s) ftrmnl_network,
                       coalesce(ftrmnl_operator,%(null_str_group_rule)s) ftrmnl_operator,
                       count(distinct case when reg_f = 1 then gs.fuid end) freg_unum,
                       count(distinct case when act_f = 1 then gs.fuid end) fact_unum,
                       count(distinct case when pay_f = 1 then gs.fuid end) fpay_unum
                  from work.device_type_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       ftrmnl_type,
                       ftrmnl_pixel,
                       ftrmnl_os,
                       ftrmnl_network,
                       ftrmnl_operator
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        drop table if exists work.device_type_tmp_%(statdatenum)s;
        create table work.device_type_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到正式表上
        --用户设备类型
        insert overwrite table dcnew.user_device_type_dis
        partition (dt = '%(statdate)s')
        select "%(statdate)s" fdate,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               fchannelcode,
               case when row_num <=100 then ftrmnl_type else '其他' end ftrmnl_type,
               sum(freg_unum) freg_unum,
               sum(fact_unum) fact_unum,
               sum(fpay_unum) fpay_unum
          from (select fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fsubgamefsk,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannelcode,
                       ftrmnl_type,
                       freg_unum,
                       fact_unum,
                       fpay_unum,
                       dense_rank() over(order by row_num desc) row_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fgame_id fsubgamefsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fchannel_code fchannelcode,
                               ftrmnl_type,
                               freg_unum,
                               fact_unum,
                               fpay_unum,
                               sum(fact_unum) over(partition by ftrmnl_type) row_num
                          from work.device_type_tmp_%(statdatenum)s
                         where ftrmnl_type <> '-21379'
                       ) t
               ) t
         group by fgamefsk,
                  fplatformfsk,
                  fhallfsk,
                  fsubgamefsk,
                  fterminaltypefsk,
                  fversionfsk,
                  fchannelcode,
                  case when row_num <=100 then ftrmnl_type else '其他' end
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到正式表上
        --设备分辨率fm_pixel
        insert overwrite table dcnew.user_device_pixel_dis
        partition (dt = '%(statdate)s')
        select "%(statdate)s" fdate,
               fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               ftrmnl_pixel,
               freg_unum,
               fact_unum,
               fpay_unum
          from work.device_type_tmp_%(statdatenum)s
         where ftrmnl_pixel <> '-21379'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到正式表上
        --设备操作系统m_os
        insert overwrite table dcnew.user_device_os_dis
        partition (dt = '%(statdate)s')
        select "%(statdate)s" fdate,
               fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               ftrmnl_os,
               freg_unum,
               fact_unum,
               fpay_unum
          from work.device_type_tmp_%(statdatenum)s
         where ftrmnl_os <> '-21379'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到正式表上
        --设备接入方式m_network
        insert overwrite table dcnew.user_device_network_dis
        partition (dt = '%(statdate)s')
        select "%(statdate)s" fdate,
               fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               ftrmnl_network,
               freg_unum,
               fact_unum,
               fpay_unum
          from work.device_type_tmp_%(statdatenum)s
         where ftrmnl_network <> '-21379'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到正式表上
        --设备运营商m_operator
        insert overwrite table dcnew.user_device_operator_dis
        partition (dt = '%(statdate)s')
        select "%(statdate)s" fdate,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               fchannelcode,
               case when row_num <=100 then ftrmnl_operator else '其他' end ftrmnl_operator,
               sum(freg_unum) freg_unum,
               sum(fact_unum) fact_unum,
               sum(fpay_unum) fpay_unum
          from (select fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fsubgamefsk,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannelcode,
                       ftrmnl_operator,
                       freg_unum,
                       fact_unum,
                       fpay_unum,
                       dense_rank() over(order by row_num desc) row_num
                  from (select fgamefsk,
                               fplatformfsk,
                               fhallfsk,
                               fgame_id fsubgamefsk,
                               fterminaltypefsk,
                               fversionfsk,
                               fchannel_code fchannelcode,
                               ftrmnl_operator,
                               freg_unum,
                               fact_unum,
                               fpay_unum,
                               sum(fact_unum) over(partition by ftrmnl_operator) row_num
                          from work.device_type_tmp_%(statdatenum)s
                         where ftrmnl_operator <> '-21379'
                       ) t
               ) t
         group by fgamefsk,
                  fplatformfsk,
                  fhallfsk,
                  fsubgamefsk,
                  fterminaltypefsk,
                  fversionfsk,
                  fchannelcode,
                  case when row_num <=100 then ftrmnl_operator else '其他' end
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.device_type_tmp_b_%(statdatenum)s;
                 drop table if exists work.device_type_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_device_type_dis_data(sys.argv[1:])
a()
