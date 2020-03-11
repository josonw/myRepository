#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_chinese_elephant_chess_user_portrait(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.chinese_elephant_chess_user_portrait(
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fuid bigint COMMENT '用户ID',
              nickname string COMMENT '昵称',
              signup_time string COMMENT '注册时间',
              latest_login_time string COMMENT '最后登录时间',
              lifespan int COMMENT '生命周期(最后登录-注册时间)',
              signup_to_now_days int COMMENT '注册至今天数',
              signup_bpid string COMMENT '注册BPID',
              signup_channel_code int COMMENT '注册渠道代码',
              signup_device_type string COMMENT '注册设备型号',
              signup_device_pixel string COMMENT '注册设备分辨率',
              signup_device_imei string COMMENT '注册设备码',
              signup_os string COMMENT '注册设备操作系统',
              signup_ip string COMMENT '注册IP',
              country string COMMENT '国家',
              city string COMMENT '地区',
              realname string COMMENT '真实姓名',
              gender string COMMENT '性别',
              age int COMMENT '年龄',
              phone_number string COMMENT '手机号',
              email string COMMENT '电子邮箱',
              gamecoins bigint COMMENT '游戏币资产',
              vip_level int COMMENT 'VIP等级',
              vip_type int COMMENT 'VIP类型',
              vip_expire_time string COMMENT 'VIP到期时间',
              user_status string COMMENT '用户状态',
              latest_located_latitude string COMMENT '最后所在位置纬度',
              latest_located_longitude string COMMENT '最后所在位置经度',
              login_days int COMMENT '登录天数',
              play_days int COMMENT '玩棋天数',
              bankrupt_days int COMMENT '破产天数',
              relieve_days int COMMENT '领取救济天数',
              login_count int COMMENT '登录次数',
              total_innings int COMMENT '玩棋局数',
              win_innings int COMMENT '胜局数',
              lose_innings int COMMENT '输局数',
              play_duration int COMMENT '玩棋时长(秒)',
              pay_count int COMMENT '付费次数',
              pay_sum decimal(10,2) COMMENT '付费总额(原币)',
              bankrupt_count int COMMENT '破产次数',
              relieve_count int COMMENT '领取救济次数',
              relieve_gamecoin bigint COMMENT '领取救济游戏币量',
              latest_play_time string COMMENT '最后玩棋时间',
              latest_pay_time string COMMENT '最后付费时间',
              latest_pay_money decimal(10,2) COMMENT '最后付费金额(原币)',
              latest_bankrupt_time string COMMENT '最后破产时间',
              latest_relieve_time string COMMENT '最后领取救济时间',
              first_login_time string COMMENT '首次登录时间',
              first_play_time string COMMENT '首次玩棋时间',
              first_pay_time string COMMENT '首次付费时间',
              first_bankrupt_time string COMMENT '首次破产时间',
              first_relieve_time string COMMENT '首次领取救济时间',
              last_2nd_login_time string COMMENT '最后登录上一次时间',
              last_2nd_play_time string COMMENT '最后玩棋上一次时间',
              last_2nd_pay_time string COMMENT '最后付费上一次时间',
              latest_device_imei string COMMENT '最后登录设备IMEI',
              latest_device_type string COMMENT '最后登录设备型号',
              latest_device_pixel string COMMENT '最后登录设备分辨率',
              latest_device_os string COMMENT '最后登录操作系统',
              latest_app_version string COMMENT '最后登录版本',
              latest_network string COMMENT '最后登录网络类型',
              latest_operator string COMMENT '最后登录运营商',
              latest_login_ip string COMMENT '最后登录IP',
              latest_login_ip_country string COMMENT '最后登录IP所属国家',
              latest_login_ip_province string COMMENT '最后登录IP所属省份',
              latest_login_ip_city string COMMENT '最后登录IP所属城市',
              recent_login_series_days int COMMENT '最近连续登录天数',
              max_login_series_days int COMMENT '最大连续登录天数',
              latest_play_gamecoin bigint COMMENT '最后棋局输赢游戏币',
              purchase_gamecoin_money decimal(10,2) COMMENT '付费购买游戏币金额(原币)',
              purchase_vip_money decimal(10,2) COMMENT '付费购买VIP金额(原币)',
              purchase_items_money decimal(10,2) COMMENT '付费购买其它物品金额(原币)',
              max_pay_money decimal(10,2) COMMENT '最大一次付费金额(原币)')
            COMMENT '中国象棋用户画像'
            STORED AS parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        ############  统计当天数据更新画像表  ############
        hql = """
            --注册信息
            with signup_info as
             (select fuid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     signup_bpid,
                     signup_time,
                     signup_channel_code,
                     signup_device_type,
                     signup_device_pixel,
                     signup_device_imei,
                     signup_os,
                     signup_ip,
                     country,
                     city
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fgamename,
                             t1.fplatformfsk,
                             t1.fplatformname,
                             t1.fbpid as signup_bpid,
                             fsignup_at as signup_time,
                             fchannel_code as signup_channel_code,
                             fm_dtype as signup_device_type,
                             fm_pixel as signup_device_pixel,
                             fm_imei as signup_device_imei,
                             fm_os as signup_os,
                             fip as signup_ip,
                             coalesce(fcountry, fip_country) as country,
                             coalesce(fcity, fip_city, fip_province) as city,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fsignup_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_signup_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m1
               where ranking = 1),
            
            --登录信息
            login_info as
             (select fuid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     login_count,
                     latest_login_time,
                     latest_device_imei,
                     latest_device_type,
                     latest_device_pixel,
                     latest_device_os,
                     latest_app_version,
                     latest_network,
                     latest_operator,
                     latest_login_ip,
                     latest_login_ip_country,
                     latest_login_ip_province,
                     latest_login_ip_city
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fgamename,
                             t1.fplatformfsk,
                             t1.fplatformname,
                             count(distinct flogin_at) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as login_count,
                             flogin_at as latest_login_time,
                             fm_imei as latest_device_imei,
                             fm_dtype as latest_device_type,
                             fm_pixel as latest_device_pixel,
                             fm_os as latest_device_os,
                             fversion_info as latest_app_version,
                             fm_network as latest_network,
                             fm_operator as latest_operator,
                             fip as latest_login_ip,
                             fip_country as latest_login_ip_country,
                             fip_province as latest_login_ip_province,
                             fip_city as latest_login_ip_city,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by flogin_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_login_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m2
               where ranking = 1),
            
            --同步信息
            async_info as
             (select fuid,
                     fgamefsk,
                     fplatformfsk,
                     fcid,
                     nickname,
                     realname,
                     gender,
                     age,
                     phone_number,
                     email,
                     user_status,
                     latest_located_latitude,
                     latest_located_longitude
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             fcid,
                             fdisplay_name as nickname,
                             fname as realname,
                             case fgender
                               when 0 then
                                '女'
                               when 1 then
                                '男'
                               else
                                null
                             end as gender,
                             fage as age,
                             fphone as phone_number,
                             femail as email,
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
                             flatitude as latest_located_latitude,
                             flongitude as latest_located_longitude,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fsync_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_async_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m3
               where ranking = 1),
            
            --游戏币信息
            gamecoin_info as
             (select fuid, fgamefsk, fplatformfsk, gamecoins
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             user_gamecoins_num as gamecoins,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by lts_at desc, fseq_no desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.pb_gamecoins_stream_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m4
               where ranking = 1),
            
            --VIP信息
            vip_info as
             (select fuid, fgamefsk, fplatformfsk, vip_level, vip_type, vip_expire_time
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             fvip_level as vip_level,
                             fvip_type as vip_type,
                             fdue_at as vip_expire_time,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fdue_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_vip_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m5
               where ranking = 1),
            
            --棋局信息
            gameparty_info_part_1 as
             (select t2.fuid,
                     t1.fgamefsk,
                     t1.fplatformfsk,
                     count(distinct finning_id) as total_innings,
                     count(distinct(case
                                      when fgamecoins > 0 then
                                       finning_id
                                      else
                                       null
                                    end)) as win_innings,
                     count(distinct(case
                                      when fgamecoins < 0 then
                                       finning_id
                                      else
                                       null
                                    end)) as lose_innings,
                     sum(case
                           when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as play_duration
                from dim.bpid_map t1
               inner join stage.user_gameparty_stg t2
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
               where dt = '%(statdate)s'
               group by t2.fuid, t1.fgamefsk, t1.fplatformfsk),
            gameparty_info_part_2 as
             (select fuid,
                     fgamefsk,
                     fplatformfsk,
                     latest_play_time,
                     latest_play_gamecoin
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             fe_timer as latest_play_time,
                             fgamecoins as latest_play_gamecoin,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by flts_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_gameparty_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m6
               where ranking = 1),
            gameparty_info as
             (select g1.fuid,
                     g1.fgamefsk,
                     g1.fplatformfsk,
                     total_innings,
                     win_innings,
                     lose_innings,
                     play_duration,
                     latest_play_time,
                     latest_play_gamecoin
                from gameparty_info_part_1 g1
               inner join gameparty_info_part_2 g2
                  on (g1.fgamefsk = g2.fgamefsk and g1.fplatformfsk = g2.fplatformfsk and
                     g1.fuid = g2.fuid)),
            
            --破产信息
            bankrupt_info as
             (select t2.fuid,
                     t1.fgamefsk,
                     t1.fplatformfsk,
                     count(distinct frupt_at) as bankrupt_count,
                     max(frupt_at) as latest_bankrupt_time
                from dim.bpid_map t1
               inner join stage.user_bankrupt_stg t2
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
               where dt = '%(statdate)s'
               group by t2.fuid, t1.fgamefsk, t1.fplatformfsk),
            
            --领取救济信息
            relieve_info as
             (select t2.fuid,
                     t1.fgamefsk,
                     t1.fplatformfsk,
                     count(distinct flts_at) as relieve_count,
                     sum(fgamecoins) as relieve_gamecoin,
                     max(flts_at) as latest_relieve_time
                from dim.bpid_map t1
               inner join stage.user_bankrupt_relieve_stg t2
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
               where dt = '%(statdate)s'
               group by t2.fuid, t1.fgamefsk, t1.fplatformfsk),
            
            --付费信息
            pay_info as
             (select fuid,
                     fgamefsk,
                     fplatformfsk,
                     pay_count,
                     pay_sum,
                     latest_pay_time,
                     latest_pay_money,
                     purchase_gamecoin_money,
                     purchase_vip_money,
                     purchase_items_money,
                     max_pay_money
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             count(fdate) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as pay_count,
                             sum(fcoins_num) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as pay_sum,
                             fdate as latest_pay_time,
                             fcoins_num as latest_pay_money,
                             sum(case
                                   when fproduct_name rlike '(金币|银币|金幣|銀幣|游戏币|遊戲幣)' then
                                    fcoins_num
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as purchase_gamecoin_money,
                             sum(case
                                   when fproduct_name rlike 'VIP' then
                                    fcoins_num
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as purchase_vip_money,
                             sum(case
                                   when fproduct_name not rlike '(金币|银币|金幣|銀幣|游戏币|遊戲幣|VIP)' then
                                    fcoins_num
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as purchase_items_money,
                             max(fcoins_num) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as max_pay_money,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fdate desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.payment_stream_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '中国象棋')
                       where dt = '%(statdate)s') m7
               where ranking = 1),
            
            --目标用户汇总
            target_users as
             (select distinct fgamefsk, fplatformfsk, fuid
                from veda.chinese_elephant_chess_user_portrait_history
               where dt = date_sub('%(statdate)s',1)
              union distinct
              select distinct fgamefsk, fplatformfsk, fuid
                from signup_info
              union distinct
              select distinct fgamefsk, fplatformfsk, fuid
                from login_info)
            
            --数据写入到表
            insert overwrite table veda.chinese_elephant_chess_user_portrait
            select v1.fgamefsk,
                   coalesce(v2.fgamename, v3.fgamename, t.fgamename) as fgamename,
                   v1.fplatformfsk,
                   coalesce(v2.fplatformname, v3.fplatformname, t.fplatformname) as fplatformname,
                   v1.fuid,
                   coalesce(v4.nickname, t.nickname) as nickname,
                   coalesce(v2.signup_time, t.signup_time) as signup_time,
                   coalesce(v3.latest_login_time, t.latest_login_time) as latest_login_time,
                   datediff(coalesce(v3.latest_login_time, t.latest_login_time),
                            coalesce(v2.signup_time, t.signup_time)) + 1 as lifespan,
                   datediff(current_date(), to_date(coalesce(v2.signup_time, t.signup_time))) as signup_to_now_days,
                   coalesce(v2.signup_bpid, t.signup_bpid) as signup_bpid,
                   coalesce(v2.signup_channel_code, t.signup_channel_code) as signup_channel_code,
                   coalesce(v2.signup_device_type, t.signup_device_type) as signup_device_type,
                   coalesce(v2.signup_device_pixel, t.signup_device_pixel) as signup_device_pixel,
                   coalesce(v2.signup_device_imei, t.signup_device_imei) as signup_device_imei,
                   coalesce(v2.signup_os, t.signup_os) as signup_os,
                   coalesce(v2.signup_ip, t.signup_ip) as signup_ip,
                   coalesce(v2.country, t.country) as country,
                   coalesce(v2.city, t.city) as city,
                   coalesce(v4.realname, t.realname) as realname,
                   coalesce(v4.gender, t.gender) as gender,
                   coalesce(v4.age,t.age) as age,
                   coalesce(v4.phone_number, t.phone_number) as phone_number,
                   coalesce(v4.email, t.email) as email,
                   coalesce(v5.gamecoins, t.gamecoins) as gamecoins,
                   case
                     when coalesce(v6.vip_expire_time, t.vip_expire_time) <
                          current_date() then
                      null
                     else
                      coalesce(v6.vip_level, t.vip_level)
                   end as vip_level,
                   case
                     when coalesce(v6.vip_expire_time, t.vip_expire_time) <
                          current_date() then
                      null
                     else
                      coalesce(v6.vip_type, t.vip_type)
                   end as vip_type,
                   case
                     when coalesce(v6.vip_expire_time, t.vip_expire_time) <
                          current_date() then
                      null
                     else
                      coalesce(v6.vip_expire_time, t.vip_expire_time)
                   end as vip_expire_time,
                   coalesce(v4.user_status, t.user_status) as user_status,
                   coalesce(v4.latest_located_latitude, t.latest_located_latitude) as latest_located_latitude,
                   coalesce(v4.latest_located_longitude, t.latest_located_longitude) as latest_located_longitude,
                   case
                     when v3.fuid is null then
                      coalesce(t.login_days, 0)
                     else
                      coalesce(t.login_days, 0) + 1
                   end as login_days,
                   case
                     when v7.fuid is null then
                      coalesce(t.play_days, 0)
                     else
                      coalesce(t.play_days, 0) + 1
                   end as play_days,
                   case
                     when v8.fuid is null then
                      coalesce(t.bankrupt_days, 0)
                     else
                      coalesce(t.bankrupt_days, 0) + 1
                   end as bankrupt_days,
                   case
                     when v9.fuid is null then
                      coalesce(t.relieve_days, 0)
                     else
                      coalesce(t.relieve_days, 0) + 1
                   end as relieve_days,
                   coalesce(v3.login_count, 0) + coalesce(t.login_count, 0) as login_count,
                   coalesce(v7.total_innings, 0) + coalesce(t.total_innings, 0) as total_innings,
                   coalesce(v7.win_innings, 0) + coalesce(t.win_innings, 0) as win_innings,
                   coalesce(v7.lose_innings, 0) + coalesce(t.lose_innings, 0) as lose_innings,
                   coalesce(v7.play_duration, 0) + coalesce(t.play_duration, 0) as play_duration,
                   coalesce(v10.pay_count, 0) + coalesce(t.pay_count, 0) as pay_count,
                   coalesce(v10.pay_sum, 0) + coalesce(t.pay_sum, 0) as pay_sum,
                   coalesce(v8.bankrupt_count, 0) + coalesce(t.bankrupt_count, 0) as bankrupt_count,
                   coalesce(v9.relieve_count, 0) + coalesce(t.relieve_count, 0) as relieve_count,
                   coalesce(v9.relieve_gamecoin, 0) + coalesce(t.relieve_gamecoin, 0) as relieve_gamecoin,
                   coalesce(v7.latest_play_time, t.latest_play_time) as latest_play_time,
                   coalesce(v10.latest_pay_time, t.latest_pay_time) latest_pay_time,
                   coalesce(v10.latest_pay_money, t.latest_pay_money) as latest_pay_money,
                   coalesce(v8.latest_bankrupt_time, t.latest_bankrupt_time) as latest_bankrupt_time,
                   coalesce(v9.latest_relieve_time, t.latest_relieve_time) as latest_relieve_time,
                   coalesce(t.first_login_time,
                            t.latest_login_time,
                            v3.latest_login_time) as first_login_time,
                   coalesce(t.first_play_time, t.latest_play_time, v7.latest_play_time) as first_play_time,
                   coalesce(t.first_pay_time, t.latest_pay_time, v10.latest_pay_time) as first_pay_time,
                   coalesce(t.first_bankrupt_time,
                            t.latest_bankrupt_time,
                            v8.latest_bankrupt_time) as first_bankrupt_time,
                   coalesce(t.first_relieve_time,
                            t.latest_relieve_time,
                            v9.latest_relieve_time) as first_relieve_time,
                   if(v3.latest_login_time > t.latest_login_time,
                      t.latest_login_time,
                      t.last_2nd_login_time) as last_2nd_login_time,
                   if(v7.latest_play_time > t.latest_play_time,
                      t.latest_play_time,
                      t.last_2nd_play_time) as last_2nd_play_time,
                   if(v10.latest_pay_time > t.latest_pay_time,
                      t.latest_pay_time,
                      t.last_2nd_pay_time) as last_2nd_pay_time,
                   coalesce(v3.latest_device_imei, t.latest_device_imei) as latest_device_imei,
                   coalesce(v3.latest_device_type, t.latest_device_type) as latest_device_type,
                   coalesce(v3.latest_device_pixel, t.latest_device_pixel) as latest_device_pixel,
                   coalesce(v3.latest_device_os, t.latest_device_os) as latest_device_os,
                   coalesce(v3.latest_app_version, t.latest_app_version) as latest_app_version,
                   coalesce(v3.latest_network, t.latest_network) as latest_network,
                   coalesce(v3.latest_operator, t.latest_operator) as latest_operator,
                   coalesce(v3.latest_login_ip, t.latest_login_ip) as latest_login_ip,
                   coalesce(v3.latest_login_ip_country, t.latest_login_ip_country) as latest_login_ip_country,
                   coalesce(v3.latest_login_ip_province, t.latest_login_ip_province) as latest_login_ip_province,
                   coalesce(v3.latest_login_ip_city, t.latest_login_ip_city) as latest_login_ip_city,
                   case
                     when v3.fuid is null then
                      0
                     else
                      coalesce(t.recent_login_series_days, 0) + 1
                   end as recent_login_series_days,
                   greatest(case
                              when v3.fuid is null then
                               0
                              else
                               coalesce(t.recent_login_series_days, 0) + 1
                            end,
                            coalesce(t.max_login_series_days, 0)) as max_login_series_days,
                   coalesce(v7.latest_play_gamecoin, t.latest_play_gamecoin) as latest_play_gamecoin,
                   coalesce(v10.purchase_gamecoin_money, 0) + coalesce(t.purchase_gamecoin_money, 0) as purchase_gamecoin_money,
                   coalesce(v10.purchase_vip_money, 0) + coalesce(t.purchase_vip_money, 0) as purchase_vip_money,
                   coalesce(v10.purchase_items_money, 0) + coalesce(t.purchase_items_money, 0) as purchase_items_money,
                   greatest(coalesce(v10.max_pay_money, 0),
                            coalesce(t.max_pay_money, 0)) as max_pay_money
              from target_users v1
              left join signup_info v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fplatformfsk = v2.fplatformfsk and v1.fuid = v2.fuid)
              left join login_info v3
                on (v1.fgamefsk = v3.fgamefsk and v1.fplatformfsk = v3.fplatformfsk and v1.fuid = v3.fuid)
              left join async_info v4
                on (v1.fgamefsk = v4.fgamefsk and v1.fplatformfsk = v4.fplatformfsk and v1.fuid = v4.fuid)
              left join gamecoin_info v5
                on (v1.fgamefsk = v5.fgamefsk and v1.fplatformfsk = v5.fplatformfsk and v1.fuid = v5.fuid)
              left join vip_info v6
                on (v1.fgamefsk = v6.fgamefsk and v1.fplatformfsk = v6.fplatformfsk and v1.fuid = v6.fuid)
              left join gameparty_info v7
                on (v1.fgamefsk = v7.fgamefsk and v1.fplatformfsk = v7.fplatformfsk and v1.fuid = v7.fuid)
              left join bankrupt_info v8
                on (v1.fgamefsk = v8.fgamefsk and v1.fplatformfsk = v8.fplatformfsk and v1.fuid = v8.fuid)
              left join relieve_info v9
                on (v1.fgamefsk = v9.fgamefsk and v1.fplatformfsk = v9.fplatformfsk and v1.fuid = v9.fuid)
              left join pay_info v10
                on (v1.fgamefsk = v10.fgamefsk and v1.fplatformfsk = v10.fplatformfsk and v1.fuid = v10.fuid)
              left join (select * from veda.chinese_elephant_chess_user_portrait_history where dt=date_sub('%(statdate)s',1)) t
                on (v1.fgamefsk = t.fgamefsk and v1.fplatformfsk = t.fplatformfsk and v1.fuid = t.fuid)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        
        ############  将当前数据备份到历史表  ############
        hql = """
            insert overwrite table veda.chinese_elephant_chess_user_portrait_history partition (dt='%(statdate)s')
            select * from veda.chinese_elephant_chess_user_portrait
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
exec_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
if len(sys.argv) == 1 or exec_date == sys.argv[1]:
    a = agg_chinese_elephant_chess_user_portrait(sys.argv[1:])
    a()