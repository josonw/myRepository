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


class agg_user_abnormal_login_info(BaseStatModel):
    def create_tab(self):
        hql = """--
        create table if not exists veda.user_abnormal_login_info (
                fdate               string        comment '时间',
                fuid                bigint        comment 'MID ',
                fhallfsk            bigint        comment '地区',
                fhallname           string        comment '地区',
                fgamecoins          bigint        comment '当前总银币',
                fgold_bars          bigint        comment '当前总金条',
                fabnormal_type      string        comment '异常类型',
                fpay_income         decimal(10,4) comment '当天支付金额',
                fdevice             string        comment '设备号',
                fdtype              string        comment '设备型号',
                fpixel              string        comment '分辨率',
                flast_device        string        comment '上一次登录设备号',
                flast_dtype         string        comment '上一次登录设型号',
                flast_pixel         string        comment '上一次登录设备分辨率',
                fversion_info       string        comment '版本号',
                fos_type            string        comment '系统类型',
                fmax_version_info   string        comment '该系统历史登录最高版本号',
                fip                 string        comment 'IP',
                fip_country         string        comment 'IP所在国家',
                fip_province        string        comment 'IP所在省份',
                fip_city            string        comment 'IP所在城市',
                flast_ip            string        comment '上一次登录IP',
                flast_ip_country    string        comment '上次IP所在国家',
                flast_ip_province   string        comment '上次IP所在省份',
                flast_ip_city       string        comment '上次IP所在城市',
                fcommon_province    string        comment '常用IP省份',
                fcommon_device      string        comment '常用设备号'
               )comment '异常登录信息表'
               partitioned by(dt string)
        location '/dw/veda/user_abnormal_login_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-XX:-UseGCOverheadLimit -Xmx1700m;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--设备
            drop table if exists work.user_abnormal_login_info_tmp_1_%(statdatenum)s;
          create table work.user_abnormal_login_info_tmp_1_%(statdatenum)s as
          select distinct t1.fuid,t1.device,t1.m_dtype,t1.m_pixel,t1.terminal_type,case when tt.device is null then 1 else 0 end is_normal
            from veda.dfqp_user_device_relation t1
            left join veda.dfqp_user_device_relation_all tt
              on t1.fuid = tt.fuid
             and t1.device = tt.device
             and tt.days >= 3
           where dt= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--IP
            drop table if exists work.user_abnormal_login_info_tmp_2_%(statdatenum)s;
          create table work.user_abnormal_login_info_tmp_2_%(statdatenum)s as
          select distinct t1.fuid,t1.ip,t1.ip_country,t1.ip_province,t1.ip_city,case when tt.ip_province is null then 1 else 0 end is_normal
            from veda.dfqp_user_ip_relation t1
            left join veda.dfqp_user_ip_relation_all tt
              on t1.fuid = tt.fuid
             and t1.ip_province = tt.ip_province
             and tt.days >= 3
           where dt= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--版本
            drop table if exists work.user_abnormal_login_info_tmp_3_%(statdatenum)s;
          create table work.user_abnormal_login_info_tmp_3_%(statdatenum)s as
          select distinct t1.fuid,t1.fversion_info,case when tt.fuid is null then 1 else 0 end is_normal
            from stage_dfqp.user_version_info_day t1
            left join stage_dfqp.user_version_info tt
              on t1.fuid = tt.fuid
             and t1.fbpid = tt.fbpid
             and t1.fversion_info = tt.fversion_max
             and tt.dt= '%(statdate)s'
           where t1.dt= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--异常登录
            drop table if exists work.user_abnormal_login_info_tmp_4_%(statdatenum)s;
          create table work.user_abnormal_login_info_tmp_4_%(statdatenum)s as
                select t1.fhallfsk
                       ,t1.fuid
                       ,tt.fhallname
                       ,t1.fterminaltypefsk
                       ,t1.fversion_info
                       ,coalesce(t3.device,t6.device) device
                       ,coalesce(t3.m_dtype,t6.m_dtype) m_dtype
                       ,coalesce(t3.m_pixel,t6.m_pixel) m_pixel
                       ,coalesce(t3.terminal_type,t6.terminal_type) terminal_type
                       ,coalesce(t4.ip,t7.ip) ip
                       ,coalesce(t4.ip_country,t7.ip_country) ip_country
                       ,coalesce(t4.ip_province,t7.ip_province) ip_province
                       ,coalesce(t4.ip_city,t7.ip_city) ip_city
                       ,case when t3.fuid is not null then 1 else 0 end res_1
                       ,case when t5.fuid is not null then 1 else 0 end res_2
                       ,case when t4.fuid is not null then 1 else 0 end res_3
                  from stage_dfqp.user_version_info_day t1
                  join veda.dfqp_user_portrait t2
                    on t1.fuid = t2.mid
                   and t1.fhallfsk = t2.fhallfsk
                   and t2.login_days > 3
                  left join work.user_abnormal_login_info_tmp_1_%(statdatenum)s t3
                    on t1.fuid = t3.fuid
                   and t3.is_normal = 1
                  left join work.user_abnormal_login_info_tmp_2_%(statdatenum)s t4
                    on t1.fuid = t4.fuid
                   and t4.is_normal = 1
                  left join work.user_abnormal_login_info_tmp_3_%(statdatenum)s t5
                    on t1.fuid = t5.fuid
                   and t5.is_normal = 1
                  left join work.user_abnormal_login_info_tmp_1_%(statdatenum)s t6
                    on t1.fuid = t6.fuid
                   and t6.is_normal = 0
                  left join work.user_abnormal_login_info_tmp_2_%(statdatenum)s t7
                    on t1.fuid = t7.fuid
                   and t7.is_normal = 0
                  join dim.bpid_map tt
                    on t1.fbpid = tt.fbpid
                   and tt.fgamefsk = 4132314431
                 where t1.dt= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--常用IP省份
            drop table if exists work.user_abnormal_login_info_tmp_5_%(statdatenum)s;
          create table work.user_abnormal_login_info_tmp_5_%(statdatenum)s as
          select fuid,
                 coalesce(max(case when row_num = 1 then ip_province end),'无') ip_province_1,
                 coalesce(max(case when row_num = 2 then ip_province end),'无') ip_province_2,
                 coalesce(max(case when row_num = 3 then ip_province end),'无') ip_province_3,
                 coalesce(max(case when row_num = 4 then ip_province end),'无') ip_province_4,
                 coalesce(max(case when row_num = 5 then ip_province end),'无') ip_province_5
            from (select fuid,ip_province,days,row_number() over(partition by fuid order by days desc) row_num
                    from (select t1.fuid,ip_province,sum(days) days
                            from veda.dfqp_user_ip_relation_all t1
                           where t1.days >= 3
                           group by t1.fuid,ip_province
                         ) t
                 ) t
           where row_num <= 5
           group by fuid

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--常用设备
            drop table if exists work.user_abnormal_login_info_tmp_6_%(statdatenum)s;
          create table work.user_abnormal_login_info_tmp_6_%(statdatenum)s as
          select fuid,
                 coalesce(max(case when row_num = 1 then device end),'无') ip_device_1,
                 coalesce(max(case when row_num = 2 then device end),'无') ip_device_2,
                 coalesce(max(case when row_num = 3 then device end),'无') ip_device_3,
                 coalesce(max(case when row_num = 4 then device end),'无') ip_device_4,
                 coalesce(max(case when row_num = 5 then device end),'无') ip_device_5
            from (select fuid,device,days,row_number() over(partition by fuid order by days desc) row_num
                    from (select t1.fuid,device,sum(days) days
                            from veda.dfqp_user_device_relation_all t1
                           where t1.days >= 3
                           group by t1.fuid,device
                         ) t
                 ) t
           where row_num <= 5
           group by fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--异常登录
            insert overwrite table veda.user_abnormal_login_info partition(dt="%(statdate)s")
                select distinct '%(statdate)s' fdate
                       ,t1.fuid
                       ,t1.fhallfsk
                       ,t1.fhallname
                       ,t6.total_silver_coin fgamecoins
                       ,t6.total_gold_bar fgold_bars
                       ,case when res_1 = 1 then 1
                             when res_1 = 0 and res_2 = 1 then 2
                             when res_1 = 0 and res_2 = 0 and res_3 = 1 then 3 end fabnormal_type
                       ,t2.pay_sum fpay_income
                       ,t1.device fdevice
                       ,t1.m_dtype fdtype
                       ,t1.m_pixel fpixel
                       ,t3.latest_device_imei flast_device
                       ,t3.latest_device_type flast_dtype
                       ,t3.latest_device_pixel flast_pixel
                       ,t1.fversion_info
                       ,t1.fterminaltypefsk fos_type
                       ,t4.fversion_max fmax_version_info
                       ,t1.ip fip
                       ,t1.ip_country fip_country
                       ,t1.ip_province fip_province
                       ,t1.ip_city fip_city
                       ,t3.latest_login_ip flast_ip
                       ,t3.latest_login_ip_country flast_ip_country
                       ,t3.latest_login_ip_province flast_ip_province
                       ,t3.latest_login_ip_city flast_ip_city
                       ,concat_ws("\\\\",t5.ip_province_1,t5.ip_province_2,t5.ip_province_3,t5.ip_province_4,t5.ip_province_5) fcommon_province
                       ,concat_ws("\\\\",t7.ip_device_1,t7.ip_device_2,t7.ip_device_3,t7.ip_device_4,t7.ip_device_5) fcommon_device
                  from work.user_abnormal_login_info_tmp_4_%(statdatenum)s t1
                 left join veda.dfqp_user_portrait_career t2
                   on t1.fuid = t2.mid
                 left join veda.dfqp_user_portrait_career_history t3
                   on t1.fuid = t3.mid
                  and t3.dt = date_sub('%(statdate)s',1)
                 left join stage_dfqp.user_version_info t4
                   on t1.fhallfsk = t4.fhallfsk
                  and t1.fterminaltypefsk = t4.fterminaltypefsk
                  and t1.fuid = t4.fuid
                  and t4.dt = '%(statdate)s'
                 left join work.user_abnormal_login_info_tmp_5_%(statdatenum)s t5
                   on t1.fuid = t5.fuid
                 left join work.user_abnormal_login_info_tmp_6_%(statdatenum)s t7
                   on t1.fuid = t7.fuid
                 left join veda.dfqp_user_portrait t6
                   on t1.fuid = t6.mid
                  and t1.fhallfsk = t6.fhallfsk
                where t1.res_1 + t1.res_2 + t1.res_3  >= 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_abnormal_login_info(sys.argv[1:])
a()
