#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserPortraitBasicInfo(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.dfqp_user_portrait_basic(
              mid bigint COMMENT 'MID',
              nickname string COMMENT '昵称',
              signup_time string COMMENT '注册时间',
              latest_login_time string COMMENT '最后登录时间',
              lifespan int COMMENT '生命周期(最后登录-注册时间)',
              signup_to_now_days int COMMENT '注册至今天数',
              signup_hallname string COMMENT '注册大厅',
              signup_channel_code int COMMENT '注册渠道代码',
              signup_device_type string COMMENT '注册设备型号',
              signup_device_pixel string COMMENT '注册设备分辨率',
              signup_device_imei string COMMENT '注册设备码',
              signup_os string COMMENT '注册设备操作系统',
              signup_ip string COMMENT '注册IP',
              signup_ip_country string COMMENT '注册IP所属国家',
              signup_ip_province string COMMENT '注册IP所属省份',
              signup_ip_city string COMMENT '注册IP所属城市',
              realname string COMMENT '真实姓名',
              realname_certify_date string COMMENT '实名认证日期',
              gender string COMMENT '性别',
              birthday string COMMENT '生日',
              star_sign string COMMENT '星座',
              age int COMMENT '年龄',
              phone_number string COMMENT '手机号',
              email string COMMENT '电子邮箱',
              id_card_number string COMMENT '身份证号码',
              cid string COMMENT 'CID',
              current_device_imsi string COMMENT '当前设备IMSI',
              current_device_imei string COMMENT '当前设备IMEI',
              total_silver_coin bigint COMMENT '总银币',
              carrying_silver_coin bigint COMMENT '携带银币',
              safebox_silver_coin bigint COMMENT '保险箱银币',
              total_gold_bar bigint COMMENT '总金条',
              carrying_gold_bar bigint COMMENT '携带金条',
              safebox_gold_bar bigint COMMENT '保险箱金条',
              carrying_items_value int COMMENT '携带道具价值',
              max_silver_coin bigint COMMENT '历史最大银币量',
              max_silver_date string COMMENT '历史最大银币日期',
              max_gold_bar bigint COMMENT '历史最大金条量',
              max_gold_date string COMMENT '历史最大金条日期',
              vip_level int COMMENT 'VIP等级',
              vip_type int COMMENT 'VIP类型',
              vip_expire_time string COMMENT 'VIP到期时间',
              signup_bpid string COMMENT 'BPID',
              signup_time_async string COMMENT '注册时间同步(业务侧上报)',
              user_status string COMMENT '用户状态',
              user_type bigint COMMENT '用户类型(登录入口)',
              app_version string COMMENT '当前应用版本',
              latest_located_latitude string COMMENT '最后所在位置纬度',
              latest_located_longitude string COMMENT '最后所在位置经度',
              device_simulator_flag string COMMENT '模拟器标识',
              signup_exception_flag int COMMENT '异常新增标识',
              fluctuometer_flag int COMMENT '波动计标识',
              active_exception_flag int COMMENT '异常活跃标识',
              multiple_account_flag int COMMENT '多账号标识',
              user_identity int COMMENT '用户身份',
              superior_agent string COMMENT '上级代理商',
              superior_promoter string COMMENT '上级推广员')
            COMMENT '地方棋牌用户基本信息'
            STORED AS ORC
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
    	  exec_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    	  if len(sys.argv) == 1 or exec_date == sys.argv[1]:
    	  
            ############  将当前数据备份到历史表  ############
            hql = """
                insert overwrite table veda.dfqp_user_portrait_basic_history partition (dt)
                select t.*, date_sub('%(statdate)s',1) as dt
                from veda.dfqp_user_portrait_basic t
            """
            res = self.sql_exe(hql)
            if res != 0:
                return res
    
            ############  统计当天数据更新画像表  ############
            hql = """
                --注册信息
                with signup_info as
                 (select mid,
                         signup_hallname,
                         signup_bpid,
                         signup_time,
                         signup_channel_code,
                         signup_device_type,
                         signup_device_pixel,
                         signup_device_imei,
                         signup_os,
                         signup_ip,
                         signup_ip_country,
                         signup_ip_province,
                         signup_ip_city
                    from (select fuid as mid,
                                 fhallname as signup_hallname,
                                 fbpid as signup_bpid,
                                 fsignup_at as signup_time,
                                 fchannel_code as signup_channel_code,
                                 fm_dtype as signup_device_type,
                                 fm_pixel as signup_device_pixel,
                                 fm_imei as signup_device_imei,
                                 fm_os as signup_os,
                                 fip as signup_ip,
                                 fip_country as signup_ip_country,
                                 fip_province as signup_ip_province,
                                 fip_city as signup_ip_city,
                                 row_number() over(partition by fuid order by fsignup_at desc) as ranking
                            from stage_dfqp.user_signup_stg
                           where dt = '%(statdate)s') m1
                   where ranking = 1),
    
                --登录信息
                login_info as
                 (select mid,
                         latest_login_time,
                         user_type,
                         device_simulator_flag,
                         signup_exception_flag,
                         fluctuometer_flag,
                         active_exception_flag,
                         multiple_account_flag
                    from (select fuid as mid,
                                 flogin_at as latest_login_time,
                                 fentrance_id as user_type,
                                 fsimulator_flag as device_simulator_flag,
                                 fsign_excption_flag as signup_exception_flag,
                                 fwave_flag as fluctuometer_flag,
                                 factive_excption_flag as active_exception_flag,
                                 fmutil_account_flag as multiple_account_flag,
                                 row_number() over(partition by fuid order by flogin_at desc) as ranking
                            from stage_dfqp.user_login_stg
                           where dt = '%(statdate)s') m2
                   where ranking = 1),
    
                --同步信息
                async_info as
                 (select fuid as mid,
                         fcid as cid,
                         fdisplay_name as nickname,
                         fname as realname,
                         case
                           when fidcard is not null then
                            '%(statdate)s'
                           else
                            null
                         end as realname_certify_date,
                         case
                           when length(fidcard) = 15 then
                            case
                              when substr(fidcard, -1, 1) %%2 = 1 then
                               '男'
                              else
                               '女'
                            end
                           when length(fidcard) = 18 then
                            case
                              when substr(fidcard, -2, 1) %%2 = 1 then
                               '男'
                              else
                               '女'
                            end
                           else
                            case fgender
                              when 0 then
                               '女'
                              when 1 then
                               '男'
                              else
                               '未定义'
                            end
                         end as gender,
                         fphone as phone_number,
                         femail as email,
                         fidcard as id_card_number,
                         to_date(from_unixtime(unix_timestamp(case
                                                                when length(fidcard) = 15 then
                                                                 concat('19', substr(fidcard, 7, 6))
                                                                when length(fidcard) = 18 then
                                                                 substr(fidcard, 7, 8)
                                                                else
                                                                 cast(null as string)
                                                              end,
                                                              'yyyyMMdd'))) as birthday,
                         case
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0120' and '0218' then
                            '水瓶座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0219' and '0320' then
                            '双鱼座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0321' and '0419' then
                            '白羊座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0420' and '0520' then
                            '金牛座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0521' and '0621' then
                            '双子座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0622' and '0722' then
                            '巨蟹座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0723' and '0822' then
                            '狮子座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0823' and '0922' then
                            '处女座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0923' and '1023' then
                            '天秤座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '1024' and '1122' then
                            '天蝎座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '1123' and '1221' then
                            '射手座'
                           when if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '1222' and '1231' or
                                if(length(fidcard) = 15,
                                   substr(fidcard, 9, 4),
                                   substr(fidcard, 11, 4)) between '0101' and '0119' then
                            '摩羯座'
                           else
                            null
                         end as star_sign,
                         floor(months_between(current_date(),
                                              to_date(from_unixtime(unix_timestamp(case
                                                                                     when length(fidcard) = 15 then
                                                                                      concat('19', substr(fidcard, 7, 6))
                                                                                     when length(fidcard) = 18 then
                                                                                      substr(fidcard, 7, 8)
                                                                                     else
                                                                                      cast(null as string)
                                                                                   end,
                                                                                   'yyyyMMdd')))) / 12) as age,
                         fimsi as current_device_imsi,
                         fimei as current_device_imei,
                         fsignup_at as signup_time_async,
                         case fstatus
                           when 0 then
                            '正常'
                           when 1 then
                            '临时封停'
                           when 2 then
                            '永久封停'
                           else
                            null
                         end as user_status,
                         fversion as app_version,
                         flatitude as latest_located_latitude,
                         flongitude as latest_located_longitude,
                         fidentity as user_identity,
                         fsuperior_agent_id as superior_agent,
                         fsuperior_promoter_id as superior_promoter,
                         row_number() over(partition by fuid order by fsync_at desc) as ranking
                    from stage_dfqp.user_async_stg
                   where dt = '%(statdate)s'),
    
                --VIP信息
                vip_info as
                 (select mid, vip_level, vip_type, vip_expire_time
                    from (select fuid as mid,
                                 fvip_level as vip_level,
                                 fvip_type as vip_type,
                                 fdue_at as vip_expire_time,
                                 row_number() over(partition by fuid order by fdue_at desc) as ranking
                            from stage_dfqp.user_vip_stg
                           where dt = '%(statdate)s') m4
                   where ranking = 1),
                
                --资产信息
                coins_info as
                 (select nvl(m5.mid, m6.mid) as mid,
                         carrying_silver_coin,
                         safebox_silver_coin,
                         max_silver_coin,
                         carrying_gold_bar,
                         safebox_gold_bar,
                         max_gold_bar
                    from (select mid,
                                 carrying_silver_coin,
                                 safebox_silver_coin,
                                 max_silver_coin
                            from (select fuid as mid,
                                         fuser_gamecoins_num as carrying_silver_coin,
                                         fbank_gamecoins as safebox_silver_coin,
                                         row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking,
                                         max(fuser_gamecoins_num + nvl(fbank_gamecoins, 0)) over(partition by fuid) as max_silver_coin
                                    from stage_dfqp.pb_gamecoins_stream_stg
                                   where dt = '%(statdate)s') t
                           where ranking = 1) m5
                    full join (select mid, carrying_gold_bar, safebox_gold_bar, max_gold_bar
                                from (select fuid as mid,
                                             fcurrencies_num as carrying_gold_bar,
                                             fbank_currencies as safebox_gold_bar,
                                             row_number() over(partition by fuid order by flts_at desc, fseq_no desc) as ranking,
                                             max(fcurrencies_num + nvl(fbank_currencies, 0)) over(partition by fuid) as max_gold_bar
                                        from stage_dfqp.pb_currencies_stream_stg
                                       where fcurrencies_type = '11'
                                         and dt = '%(statdate)s') t
                               where ranking = 1) m6
                      on m5.mid = m6.mid),
                
                --需更新的目标用户
                target_users as
                 (select distinct mid
                    from veda.dfqp_user_portrait_basic
                  union distinct
                  select distinct mid
                    from signup_info)
    
                --插入更新
                insert overwrite table veda.dfqp_user_portrait_basic
                select nvl(v1.mid, t.mid) as mid,
                       nvl(v4.nickname, t.nickname) as nickname,
                       nvl(v2.signup_time, t.signup_time) as signup_time,
                       nvl(v3.latest_login_time, t.latest_login_time) as latest_login_time,
                       datediff(nvl(v3.latest_login_time, t.latest_login_time),
                                nvl(v2.signup_time, t.signup_time)) + 1 as lifespan,
                       datediff(current_date(), nvl(v2.signup_time, t.signup_time)) as signup_to_now_days,
                       nvl(v2.signup_hallname, t.signup_hallname) as signup_hallname,
                       nvl(v2.signup_channel_code, t.signup_channel_code) as signup_channel_code,
                       nvl(v2.signup_device_type, t.signup_device_type) as signup_device_type,
                       nvl(v2.signup_device_pixel, t.signup_device_pixel) as signup_device_pixel,
                       nvl(v2.signup_device_imei, t.signup_device_imei) as signup_device_imei,
                       nvl(v2.signup_os, t.signup_os) as signup_os,
                       nvl(v2.signup_ip, t.signup_ip) as signup_ip,
                       nvl(v2.signup_ip_country, t.signup_ip_country) as signup_ip_country,
                       nvl(v2.signup_ip_province, t.signup_ip_province) as signup_ip_province,
                       nvl(v2.signup_ip_city, t.signup_ip_city) as signup_ip_city,
                       nvl(v4.realname, t.realname) as realname,
                       nvl(t.realname_certify_date, v4.realname_certify_date) as realname_certify_date,
                       nvl(v4.gender, t.gender) as gender,
                       nvl(v4.birthday, t.birthday) as birthday,
                       nvl(v4.star_sign, t.star_sign) as star_sign,
                       nvl(v4.age, floor(months_between(current_date(), t.birthday)/12)) as age,
                       nvl(v4.phone_number, t.phone_number) as phone_number,
                       nvl(v4.email, t.email) as email,
                       nvl(v4.id_card_number, t.id_card_number) as id_card_number,
                       nvl(v4.cid, t.cid) as cid,
                       nvl(v4.current_device_imsi, t.current_device_imsi) as current_device_imsi,
                       nvl(v4.current_device_imei, t.current_device_imei) as current_device_imei,
                       coalesce(v6.carrying_silver_coin,t.carrying_silver_coin,0) + coalesce(v6.safebox_silver_coin,t.safebox_silver_coin,0) as total_silver_coin,
                       coalesce(v6.carrying_silver_coin,t.carrying_silver_coin,0) as carrying_silver_coin,
                       coalesce(v6.safebox_silver_coin,t.safebox_silver_coin,0) as safebox_silver_coin,
                       coalesce(v6.carrying_gold_bar,t.carrying_gold_bar,0) + coalesce(v6.safebox_gold_bar,t.safebox_gold_bar,0) as total_gold_bar,
                       coalesce(v6.carrying_gold_bar,t.carrying_gold_bar,0) as carrying_gold_bar,
                       coalesce(v6.safebox_gold_bar,t.safebox_gold_bar,0) as safebox_gold_bar,
                       cast(null as int) as carrying_items_value,
                       greatest(nvl(v6.max_silver_coin,0),nvl(t.max_silver_coin,0)) as max_silver_coin,
                       case when nvl(v6.max_silver_coin,0) > nvl(t.max_silver_coin,0) then '%(statdate)s' else t.max_silver_date end as max_silver_date,
                       greatest(nvl(v6.max_gold_bar,0),nvl(t.max_gold_bar,0)) as max_gold_bar,
                       case when nvl(v6.max_gold_bar,0) > nvl(t.max_gold_bar,0) then '%(statdate)s' else t.max_gold_date end as max_gold_date,
                       case
                         when nvl(v5.vip_expire_time, t.vip_expire_time) < current_date() then
                          null
                         else
                          nvl(v5.vip_level, t.vip_level)
                       end as vip_level,
                       case
                         when nvl(v5.vip_expire_time, t.vip_expire_time) < current_date() then
                          null
                         else
                          nvl(v5.vip_type, t.vip_type)
                       end as vip_type,
                       case
                         when nvl(v5.vip_expire_time, t.vip_expire_time) < current_date() then
                          null
                         else
                          nvl(v5.vip_expire_time, t.vip_expire_time)
                       end as vip_expire_time,
                       nvl(v2.signup_bpid, t.signup_bpid) as signup_bpid,
                       nvl(v4.signup_time_async, t.signup_time_async) as signup_time_async,
                       nvl(v4.user_status, t.user_status) as user_status,
                       nvl(v3.user_type, t.user_type) as user_type,
                       nvl(v4.app_version, t.app_version) as app_version,
                       nvl(v4.latest_located_latitude, t.latest_located_latitude) as latest_located_latitude,
                       nvl(v4.latest_located_longitude, t.latest_located_longitude) as latest_located_longitude,
                       nvl(v3.device_simulator_flag, t.device_simulator_flag) as device_simulator_flag,
                       nvl(v3.signup_exception_flag, t.signup_exception_flag) as signup_exception_flag,
                       nvl(v3.fluctuometer_flag, t.fluctuometer_flag) as fluctuometer_flag,
                       nvl(v3.active_exception_flag, t.active_exception_flag) as active_exception_flag,
                       nvl(v3.multiple_account_flag, t.multiple_account_flag) as multiple_account_flag,
                       nvl(v4.user_identity, t.user_identity) as user_identity,
                       nvl(v4.superior_agent, t.superior_agent) as superior_agent,
                       nvl(v4.superior_promoter, t.superior_promoter) as superior_promoter
                  from target_users v1
                  left join signup_info v2
                    on v1.mid = v2.mid
                  left join login_info v3
                    on v1.mid = v3.mid
                  left join async_info v4
                    on (v1.mid = v4.mid and v4.ranking = 1)
                  left join vip_info v5
                    on v1.mid = v5.mid
                  left join coins_info v6
                    on v1.mid = v6.mid
                  left join veda.dfqp_user_portrait_basic t
                    on v1.mid = t.mid
                 order by mid
            """
            res = self.sql_exe(hql)
            if res != 0:
                return res

            return res


# 实例化执行
a = UserPortraitBasicInfo(sys.argv[1:])
a()