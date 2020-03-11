#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class TexasUserPortrait(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.texas_user_portrait(
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称',
              fuid bigint COMMENT '用户ID',
              nickname string COMMENT '昵称',
              signup_time string COMMENT '注册时间',
              fbpid string COMMENT '注册BPID',
              latest_login_time string COMMENT '最后登录时间',
              lifespan int COMMENT '生命周期(最后登录-注册时间)',
              signup_to_now_days int COMMENT '注册至今天数',
              signup_channel_code int COMMENT '注册渠道代码',
              signup_device_type string COMMENT '注册设备型号',
              signup_device_pixel string COMMENT '注册设备分辨率',
              signup_device_imei string COMMENT '注册设备码',
              signup_os string COMMENT '注册设备操作系统',
              country string COMMENT '国家',
              city string COMMENT '地区',
              realname string COMMENT '真实姓名',
              gender string COMMENT '性别',
              age int COMMENT '年龄',
              phone_number string COMMENT '手机号',
              email string COMMENT '电子邮箱',
              total_gamecoin bigint COMMENT '总游戏币',
              carrying_gamecoin bigint COMMENT '携带游戏币',
              safebox_gamecoin bigint COMMENT '保险箱游戏币',
              max_gamecoin bigint COMMENT '历史最大游戏币量',
              max_gamecoin_date string COMMENT '历史最多游戏币日期',
              vip_level int COMMENT 'VIP等级',
              vip_type int COMMENT 'VIP类型',
              vip_expire_time string COMMENT 'VIP到期时间',
              user_status string COMMENT '用户状态',
              user_type bigint COMMENT '用户类型(登录入口)',
              latest_located_latitude string COMMENT '最后所在位置纬度',
              latest_located_longitude string COMMENT '最后所在位置经度',
              login_days int COMMENT '登录天数',
              play_days int COMMENT '玩牌天数',
              bankrupt_days int COMMENT '破产天数',
              relieve_days int COMMENT '领取救济天数',
              match_days int COMMENT '比赛天数',
              login_count int COMMENT '登录次数',
              total_innings int COMMENT '玩牌局数',
              win_innings int COMMENT '胜局数',
              lose_innings int COMMENT '输局数',
              play_duration int COMMENT '玩牌时长(秒)',
              pay_count int COMMENT '付费次数',
              pay_sum decimal(10,2) COMMENT '付费总额(美元)',
              match_enroll_count int COMMENT '比赛报名次数',
              match_innings int COMMENT '比赛局数',
              match_duration int COMMENT '比赛时长(秒)',
              match_win_innings int COMMENT '比赛获胜局数',
              bankrupt_count int COMMENT '破产次数',
              relieve_count int COMMENT '领取救济次数',
              relieve_gamecoin bigint COMMENT '领取救济游戏币量',
              latest_active_time string COMMENT '最后活跃时间',
              latest_play_time string COMMENT '最后玩牌时间',
              latest_pay_time string COMMENT '最后付费时间',
              latest_pay_money decimal(10,2) COMMENT '最后付费金额(美元)',
              latest_match_time string COMMENT '最后比赛时间',
              latest_bankrupt_time string COMMENT '最后破产时间',
              latest_relieve_time string COMMENT '最后领取救济时间',
              first_login_time string COMMENT '首次登录时间',
              first_play_time string COMMENT '首次玩牌时间',
              first_pay_time string COMMENT '首次付费时间',
              first_match_time string COMMENT '首次比赛时间',
              first_bankrupt_time string COMMENT '首次破产时间',
              first_relieve_time string COMMENT '首次领取救济时间',
              last_2nd_login_time string COMMENT '最后登录上一次时间',
              last_2nd_active_time string COMMENT '最后活跃上一次时间',
              last_2nd_play_time string COMMENT '最后玩牌上一次时间',
              last_2nd_pay_time string COMMENT '最后付费上一次时间',
              last_2nd_match_time string COMMENT '最后比赛上一次时间',
              latest_device_imei string COMMENT '最后登录设备IMEI',
              latest_device_imsi string COMMENT '最后登录设备IMSI',
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
              latest_pname string COMMENT '最后玩牌一次场次',
              latest_subname string COMMENT '最后玩牌二级场次',
              latest_gsubname string COMMENT '最后玩牌三级场次',
              latest_play_gamecoin bigint COMMENT '最后玩牌输赢游戏币',
              purchase_gamecoin_money decimal(10,2) COMMENT '付费购买游戏币金额(美元)',
              purchase_vip_money decimal(10,2) COMMENT '付费购买VIP金额(美元)',
              purchase_items_money decimal(10,2) COMMENT '付费购买其它物品金额(美元)',
              max_pay_money decimal(10,2) COMMENT '最大付费金额(美元)',
              match_entry_fee bigint COMMENT '比赛总报名费(游戏币)',
              max_match_entry_fee bigint COMMENT '比赛最大报名费(游戏币)',
              latest_match_pname string COMMENT '最后比赛一次场次',
              latest_match_subname string COMMENT '最后比赛二级场次',
              latest_match_gsubname string COMMENT '最后比赛三级场次',
              match_reward decimal(10,2) COMMENT '比赛获奖总金额(人民币元)',
              max_match_reward decimal(10,2) COMMENT '比赛最大获奖金额(人民币元)',
              if_signup_2days char(1) COMMENT '是否已注册2天',
              if_signup_3days char(1) COMMENT '是否已注册3天',
              if_signup_7days char(1) COMMENT '是否已注册7天',
              if_signup_15days char(1) COMMENT '是否已注册15天',
              if_signup_30days char(1) COMMENT '是否已注册30天',
              if_signup_60days char(1) COMMENT '是否已注册60天',
              if_signup_90days char(1) COMMENT '是否已注册90天',
              if_away_7days char(1) COMMENT '是否已流失7天',
              if_away_15days char(1) COMMENT '是否已流失15天',
              if_away_30days char(1) COMMENT '是否已流失30天',
              if_away_60days char(1) COMMENT '是否已流失60天',
              if_away_90days char(1) COMMENT '是否已流失90天')
            COMMENT '德州扑克用户画像'
            STORED AS parquet
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
                insert overwrite table veda.texas_user_portrait_history partition (dt)
                select t.*, date_sub('%(statdate)s',1) as dt
                from veda.texas_user_portrait t
            """
            res = self.sql_exe(hql)
            if res != 0:
                return res
    
            ############  统计当天数据更新画像表  ############
            hql = """
            --注册信息
            with signup_info as
             (select fuid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname,
                     fbpid,
                     signup_time,
                     signup_channel_code,
                     signup_device_type,
                     signup_device_pixel,
                     signup_device_imei,
                     signup_os,
                     country,
                     city
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fgamename,
                             t1.fplatformfsk,
                             t1.fplatformname,
                             t1.fhallfsk,
                             t1.fhallname,
                             t1.fterminaltypefsk,
                             t1.fterminaltypename,
                             t1.fversionfsk,
                             t1.fversionname,
                             t1.fbpid,
                             fsignup_at as signup_time,
                             fchannel_code as signup_channel_code,
                             fm_dtype as signup_device_type,
                             fm_pixel as signup_device_pixel,
                             fm_imei as signup_device_imei,
                             fm_os as signup_os,
                             coalesce(fcountry, fip_country) as country,
                             coalesce(fcity, fip_city, fip_province) as city,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fsignup_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_signup_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m1
               where ranking = 1),
            
            --登录信息
            login_info as
             (select fuid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname,
                     login_count,
                     latest_login_time,
                     latest_device_imei,
                     latest_device_imsi,
                     latest_device_type,
                     latest_device_pixel,
                     latest_device_os,
                     latest_app_version,
                     latest_network,
                     latest_operator,
                     latest_login_ip,
                     latest_login_ip_country,
                     latest_login_ip_province,
                     latest_login_ip_city,
                     user_type
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fgamename,
                             t1.fplatformfsk,
                             t1.fplatformname,
                             t1.fhallfsk,
                             t1.fhallname,
                             t1.fterminaltypefsk,
                             t1.fterminaltypename,
                             t1.fversionfsk,
                             t1.fversionname,
                             count(distinct flogin_at) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as login_count,
                             flogin_at as latest_login_time,
                             fm_imei as latest_device_imei,
                             fm_imsi as latest_device_imsi,
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
                             fentrance_id as user_type,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by flogin_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_login_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
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
                                '未定义'
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
                                '未上报'
                             end as user_status,
                             flatitude as latest_located_latitude,
                             flongitude as latest_located_longitude,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fsync_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_async_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m3
               where ranking = 1),
            
            --携带游戏币信息
            carrying_gamecoin_info as
             (select fuid, fgamefsk, fplatformfsk, carrying_gamecoin
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             user_gamecoins_num as carrying_gamecoin,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by lts_at desc, fseq_no desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.pb_gamecoins_stream_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m4
               where ranking = 1),
            
            --保险箱游戏币信息
            safebox_gamecoin_info as
             (select fuid, fgamefsk, fplatformfsk, safebox_gamecoin
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             fbank_gamecoins_num as safebox_gamecoin,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by flts_at desc, fseq_no desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_bank_stage t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m5
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
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m6
               where ranking = 1),
            
            --牌局信息
            gameparty_info_1 as
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
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
               where dt = '%(statdate)s'
               group by t2.fuid, t1.fgamefsk, t1.fplatformfsk),
            gameparty_info_2 as
             (select fuid,
                     fgamefsk,
                     fplatformfsk,
                     latest_play_time,
                     latest_pname,
                     latest_subname,
                     latest_gsubname,
                     latest_play_gamecoin
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             fe_timer as latest_play_time,
                             fpname as latest_pname,
                             fsubname as latest_subname,
                             fgsubname as latest_gsubname,
                             fgamecoins as latest_play_gamecoin,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by flts_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_gameparty_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m7
               where ranking = 1),
            gameparty_info_all as
             (select g1.fuid,
                     g1.fgamefsk,
                     g1.fplatformfsk,
                     total_innings,
                     win_innings,
                     lose_innings,
                     play_duration,
                     latest_play_time,
                     latest_pname,
                     latest_subname,
                     latest_gsubname,
                     latest_play_gamecoin
                from gameparty_info_1 g1
               inner join gameparty_info_2 g2
                  on (g1.fgamefsk = g2.fgamefsk and g1.fplatformfsk = g2.fplatformfsk and
                     g1.fuid = g2.fuid)),
            
            --比赛信息
            match_info as
             (select fuid,
                     fgamefsk,
                     fplatformfsk,
                     match_innings,
                     match_duration,
                     match_win_innings,
                     latest_match_time,
                     latest_match_pname,
                     latest_match_subname,
                     latest_match_gsubname
                from (select t2.fuid,
                             t1.fgamefsk,
                             t1.fplatformfsk,
                             count(distinct finning_id) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as match_innings,
                             sum(case
                                   when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                        10800 then
                                    unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as match_duration,
                             count(distinct(case
                                              when fgamecoins > 0 then
                                               finning_id
                                              else
                                               null
                                            end)) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as match_win_innings,
                             flts_at as latest_match_time,
                             fpname as latest_match_pname,
                             fsubname as latest_match_subname,
                             fgsubname as latest_match_gsubname,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by flts_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_gameparty_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where (fmatch_id != '0' or coalesce(fmatch_cfg_id, fmatch_log_id) != 0)
                         and dt = '%(statdate)s') m8
               where ranking = 1),
            
            --破产信息
            bankrupt_info as
             (select t2.fuid,
                     t1.fgamefsk,
                     t1.fplatformfsk,
                     count(distinct frupt_at) as bankrupt_count,
                     max(frupt_at) as latest_bankrupt_time
                from dim.bpid_map t1
               inner join stage.user_bankrupt_stg t2
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
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
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
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
                             sum(fpamount_usd) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as pay_sum,
                             fdate as latest_pay_time,
                             fpamount_usd as latest_pay_money,
                             sum(case
                                   when fproduct_name rlike '(金币|银币|金幣|銀幣|游戏币|遊戲幣)' then
                                    fpamount_usd
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as purchase_gamecoin_money,
                             sum(case
                                   when fproduct_name rlike 'VIP' then
                                    fpamount_usd
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as purchase_vip_money,
                             sum(case
                                   when fproduct_name not rlike '(金币|银币|金幣|銀幣|游戏币|遊戲幣|VIP)' then
                                    fpamount_usd
                                   else
                                    0
                                 end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as purchase_items_money,
                             max(fpamount_usd) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid) as max_pay_money,
                             row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fuid order by fdate desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.payment_stream_stg t2
                          on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
                       where dt = '%(statdate)s') m9
               where ranking = 1),
            
            --比赛报名信息
            join_match_info as
             (select t2.fuid,
                     t1.fgamefsk,
                     t1.fplatformfsk,
                     count(distinct flts_at) as match_enroll_count,
                     sum(fentry_fee) as match_entry_fee,
                     max(fentry_fee) as max_match_entry_fee
                from dim.bpid_map t1
               inner join stage.join_gameparty_stg t2
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
               where dt = '%(statdate)s'
               group by t2.fuid, t1.fgamefsk, t1.fplatformfsk),
            
            --比赛发放信息
            match_economy as
             (select t2.fuid,
                     t1.fgamefsk,
                     t1.fplatformfsk,
                     sum(fcost) as match_reward,
                     max(fcost) as max_match_reward
                from dim.bpid_map t1
               inner join stage.match_economy_stg t2
                  on (t1.fbpid = t2.fbpid and t1.fgamename = '德州扑克')
               where dt = '%(statdate)s'
               group by t2.fuid, t1.fgamefsk, t1.fplatformfsk),
            
            --目标用户列表
            target_users as
             (select distinct fgamefsk, fplatformfsk, fuid
                from veda.texas_user_portrait
              union distinct
              select distinct fgamefsk, fplatformfsk, fuid
                from signup_info
              union distinct
              select distinct fgamefsk, fplatformfsk, fuid
                from login_info)
            
            --插入更新
            insert overwrite table veda.texas_user_portrait
            select v1.fgamefsk,
                   coalesce(v2.fgamename, v3.fgamename, t.fgamename) as fgamename,
                   v1.fplatformfsk,
                   coalesce(v2.fplatformname, v3.fplatformname, t.fplatformname) as fplatformname,
                   coalesce(v2.fhallfsk, v3.fhallfsk, t.fhallfsk) as fhallfsk,
                   coalesce(v2.fhallname, v3.fhallname, t.fhallname) as fhallname,
                   coalesce(v2.fterminaltypefsk, v3.fterminaltypefsk, t.fterminaltypefsk) as fterminaltypefsk,
                   coalesce(v2.fterminaltypename, v3.fterminaltypename, t.fterminaltypename) as fterminaltypename,
                   coalesce(v2.fversionfsk, v3.fversionfsk, t.fversionfsk) as fversionfsk,
                   coalesce(v2.fversionname, v3.fversionname, t.fversionname) as fversionname,
                   v1.fuid,
                   coalesce(v4.nickname, t.nickname) as nickname,
                   coalesce(v2.signup_time, t.signup_time) as signup_time,
                   coalesce(v2.fbpid, t.fbpid) as fbpid,
                   coalesce(v3.latest_login_time, t.latest_login_time) as latest_login_time,
                   datediff(coalesce(v3.latest_login_time, t.latest_login_time),
                            coalesce(v2.signup_time, t.signup_time)) + 1 as lifespan,
                   datediff(current_date(), coalesce(v2.signup_time, t.signup_time)) as signup_to_now_days,
                   coalesce(v2.signup_channel_code, t.signup_channel_code) as signup_channel_code,
                   coalesce(v2.signup_device_type, t.signup_device_type) as signup_device_type,
                   coalesce(v2.signup_device_pixel, t.signup_device_pixel) as signup_device_pixel,
                   coalesce(v2.signup_device_imei, t.signup_device_imei) as signup_device_imei,
                   coalesce(v2.signup_os, t.signup_os) as signup_os,
                   coalesce(v2.country, t.country) as country,
                   coalesce(v2.city, t.city) as city,
                   coalesce(v4.realname, t.realname) as realname,
                   coalesce(v4.gender, t.gender) as gender,
                   coalesce(v4.age,t.age) as age,
                   coalesce(v4.phone_number, t.phone_number) as phone_number,
                   coalesce(v4.email, t.email) as email,
                   coalesce(v5.carrying_gamecoin, t.carrying_gamecoin, 0) +
                   coalesce(v6.safebox_gamecoin, t.safebox_gamecoin, 0) as total_gamecoin,
                   coalesce(v5.carrying_gamecoin, t.carrying_gamecoin) as carrying_gamecoin,
                   coalesce(v6.safebox_gamecoin, t.safebox_gamecoin) as safebox_gamecoin,
                   greatest(coalesce(v5.carrying_gamecoin, t.carrying_gamecoin, 0) +
                            coalesce(v6.safebox_gamecoin, t.safebox_gamecoin, 0),
                            coalesce(t.carrying_gamecoin, 0) +
                            coalesce(t.safebox_gamecoin, 0)) as max_gamecoin,
                   if(coalesce(v5.carrying_gamecoin, t.carrying_gamecoin, 0) +
                      coalesce(v6.safebox_gamecoin, t.safebox_gamecoin, 0) >
                      coalesce(t.carrying_gamecoin, 0) +
                      coalesce(t.safebox_gamecoin, 0),
                      '%(statdate)s',
                      t.max_gamecoin_date) as max_gamecoin_date,
                   case
                     when coalesce(v7.vip_expire_time, t.vip_expire_time) <
                          current_date() then
                      null
                     else
                      coalesce(v7.vip_level, t.vip_level)
                   end as vip_level,
                   case
                     when coalesce(v7.vip_expire_time, t.vip_expire_time) <
                          current_date() then
                      null
                     else
                      coalesce(v7.vip_type, t.vip_type)
                   end as vip_type,
                   case
                     when coalesce(v7.vip_expire_time, t.vip_expire_time) <
                          current_date() then
                      null
                     else
                      coalesce(v7.vip_expire_time, t.vip_expire_time)
                   end as vip_expire_time,
                   coalesce(v4.user_status, t.user_status) as user_status,
                   coalesce(v3.user_type, t.user_type) as user_type,
                   coalesce(v4.latest_located_latitude, t.latest_located_latitude) as latest_located_latitude,
                   coalesce(v4.latest_located_longitude, t.latest_located_longitude) as latest_located_longitude,
                   case
                     when v3.fuid is null then
                      coalesce(t.login_days, 0)
                     else
                      coalesce(t.login_days, 0) + 1
                   end as login_days,
                   case
                     when v8.fuid is null then
                      coalesce(t.play_days, 0)
                     else
                      coalesce(t.play_days, 0) + 1
                   end as play_days,
                   case
                     when v9.fuid is null then
                      coalesce(t.bankrupt_days, 0)
                     else
                      coalesce(t.bankrupt_days, 0) + 1
                   end as bankrupt_days,
                   case
                     when v10.fuid is null then
                      coalesce(t.relieve_days, 0)
                     else
                      coalesce(t.relieve_days, 0) + 1
                   end as relieve_days,
                   case
                     when v11.fuid is null then
                      coalesce(t.match_days, 0)
                     else
                      coalesce(t.match_days, 0) + 1
                   end as match_days,
                   coalesce(v3.login_count, 0) + coalesce(t.login_count, 0) as login_count,
                   coalesce(v8.total_innings, 0) + coalesce(t.total_innings, 0) as total_innings,
                   coalesce(v8.win_innings, 0) + coalesce(t.win_innings, 0) as win_innings,
                   coalesce(v8.lose_innings, 0) + coalesce(t.lose_innings, 0) as lose_innings,
                   coalesce(v8.play_duration, 0) + coalesce(t.play_duration, 0) as play_duration,
                   coalesce(v12.pay_count, 0) + coalesce(t.pay_count, 0) as pay_count,
                   coalesce(v12.pay_sum, 0) + coalesce(t.pay_sum, 0) as pay_sum,
                   coalesce(v13.match_enroll_count, 0) +
                   coalesce(t.match_enroll_count, 0) as match_enroll_count,
                   coalesce(v11.match_innings, 0) + coalesce(t.match_innings, 0) as match_innings,
                   coalesce(v11.match_duration, 0) + coalesce(t.match_duration, 0) as match_duration,
                   coalesce(v11.match_win_innings, 0) +
                   coalesce(t.match_win_innings, 0) as match_win_innings,
                   coalesce(v9.bankrupt_count, 0) + coalesce(t.bankrupt_count, 0) as bankrupt_count,
                   coalesce(v10.relieve_count, 0) + coalesce(t.relieve_count, 0) as relieve_count,
                   coalesce(v10.relieve_gamecoin, 0) +
                   coalesce(t.relieve_gamecoin, 0) as relieve_gamecoin,
                   nullif(greatest(coalesce(v3.latest_login_time, '0'),
                                   coalesce(v8.latest_play_time, '0'),
                                   coalesce(v9.latest_bankrupt_time, '0'),
                                   coalesce(v10.latest_relieve_time, '0'),
                                   coalesce(v12.latest_pay_time, '0'),
                                   coalesce(t.latest_active_time, '0')),
                          '0') as latest_active_time,
                   coalesce(v8.latest_play_time, t.latest_play_time) as latest_play_time,
                   coalesce(v12.latest_pay_time, t.latest_pay_time) latest_pay_time,
                   coalesce(v12.latest_pay_money, t.latest_pay_money) as latest_pay_money,
                   coalesce(v11.latest_match_time, t.latest_match_time) as latest_match_time,
                   coalesce(v9.latest_bankrupt_time, t.latest_bankrupt_time) as latest_bankrupt_time,
                   coalesce(v10.latest_relieve_time, t.latest_relieve_time) as latest_relieve_time,
                   coalesce(t.first_login_time,
                            t.latest_login_time,
                            v3.latest_login_time) as first_login_time,
                   coalesce(t.first_play_time, t.latest_play_time, v8.latest_play_time) as first_play_time,
                   coalesce(t.first_pay_time, t.latest_pay_time, v12.latest_pay_time) as first_pay_time,
                   coalesce(t.first_match_time,
                            t.latest_match_time,
                            v11.latest_match_time) as first_match_time,
                   coalesce(t.first_bankrupt_time,
                            t.latest_bankrupt_time,
                            v9.latest_bankrupt_time) as first_bankrupt_time,
                   coalesce(t.first_relieve_time,
                            t.latest_relieve_time,
                            v10.latest_relieve_time) as first_relieve_time,
                   if(v3.latest_login_time > t.latest_login_time,
                      t.latest_login_time,
                      t.last_2nd_login_time) as last_2nd_login_time,
                   if(greatest(coalesce(v3.latest_login_time, '0'),
                               coalesce(v8.latest_play_time, '0'),
                               coalesce(v9.latest_bankrupt_time, '0'),
                               coalesce(v10.latest_relieve_time, '0'),
                               coalesce(v12.latest_pay_time, '0')) >
                      coalesce(t.latest_active_time, '0'),
                      t.latest_active_time,
                      t.last_2nd_active_time) as last_2nd_active_time,
                   if(v8.latest_play_time > t.latest_play_time,
                      t.latest_play_time,
                      t.last_2nd_play_time) as last_2nd_play_time,
                   if(v12.latest_pay_time > t.latest_pay_time,
                      t.latest_pay_time,
                      t.last_2nd_pay_time) as last_2nd_pay_time,
                   if(v11.latest_match_time > t.latest_match_time,
                      t.latest_match_time,
                      t.last_2nd_match_time) as last_2nd_match_time,
                   coalesce(v3.latest_device_imei, t.latest_device_imei) as latest_device_imei,
                   coalesce(v3.latest_device_imsi, t.latest_device_imsi) as latest_device_imsi,
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
                   coalesce(v8.latest_pname, t.latest_pname) as latest_pname,
                   coalesce(v8.latest_subname, t.latest_subname) as latest_subname,
                   coalesce(v8.latest_gsubname, t.latest_gsubname) as latest_gsubname,
                   coalesce(v8.latest_play_gamecoin, t.latest_play_gamecoin) as latest_play_gamecoin,
                   coalesce(v12.purchase_gamecoin_money, 0) +
                   coalesce(t.purchase_gamecoin_money, 0) as purchase_gamecoin_money,
                   coalesce(v12.purchase_vip_money, 0) +
                   coalesce(t.purchase_vip_money, 0) as purchase_vip_money,
                   coalesce(v12.purchase_items_money, 0) +
                   coalesce(t.purchase_items_money, 0) as purchase_items_money,
                   greatest(coalesce(v12.max_pay_money, 0),
                            coalesce(t.max_pay_money, 0)) as max_pay_money,
                   coalesce(v13.match_entry_fee, 0) + coalesce(t.match_entry_fee, 0) as match_entry_fee,
                   greatest(coalesce(v13.max_match_entry_fee, 0),
                            coalesce(t.max_match_entry_fee, 0)) as max_match_entry_fee,
                   coalesce(v11.latest_match_pname, t.latest_match_pname) as latest_match_pname,
                   coalesce(v11.latest_match_subname, t.latest_match_subname) as latest_match_subname,
                   coalesce(v11.latest_match_gsubname, t.latest_match_gsubname) as latest_match_gsubname,
                   coalesce(v14.match_reward, 0) + coalesce(t.match_reward, 0) as match_reward,
                   greatest(coalesce(v14.max_match_reward, 0),
                            coalesce(t.max_match_reward, 0)) as max_match_reward,
                   case
                     when datediff(current_date(), v2.signup_time) >= 2 then
                      'Y'
                     else
                      'N'
                   end as if_signup_2days,
                   case
                     when datediff(current_date(), v2.signup_time) >= 3 then
                      'Y'
                     else
                      'N'
                   end as if_signup_3days,
                   case
                     when datediff(current_date(), v2.signup_time) >= 7 then
                      'Y'
                     else
                      'N'
                   end as if_signup_7days,
                   case
                     when datediff(current_date(), v2.signup_time) >= 15 then
                      'Y'
                     else
                      'N'
                   end as if_signup_15days,
                   case
                     when datediff(current_date(), v2.signup_time) >= 30 then
                      'Y'
                     else
                      'N'
                   end as if_signup_30days,
                   case
                     when datediff(current_date(), v2.signup_time) >= 60 then
                      'Y'
                     else
                      'N'
                   end as if_signup_60days,
                   case
                     when datediff(current_date(), v2.signup_time) >= 90 then
                      'Y'
                     else
                      'N'
                   end as if_signup_90days,
                   case
                     when datediff(current_date(),
                                   coalesce(v3.latest_login_time, t.latest_login_time)) >= 7 then
                      'Y'
                     else
                      'N'
                   end as if_away_7days,
                   case
                     when datediff(current_date(),
                                   coalesce(v3.latest_login_time, t.latest_login_time)) >= 15 then
                      'Y'
                     else
                      'N'
                   end as if_away_15days,
                   case
                     when datediff(current_date(),
                                   coalesce(v3.latest_login_time, t.latest_login_time)) >= 30 then
                      'Y'
                     else
                      'N'
                   end as if_away_30days,
                   case
                     when datediff(current_date(),
                                   coalesce(v3.latest_login_time, t.latest_login_time)) >= 60 then
                      'Y'
                     else
                      'N'
                   end as if_away_60days,
                   case
                     when datediff(current_date(),
                                   coalesce(v3.latest_login_time, t.latest_login_time)) >= 90 then
                      'Y'
                     else
                      'N'
                   end as if_away_90days
              from target_users v1
              left join signup_info v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fplatformfsk = v2.fplatformfsk and v1.fuid = v2.fuid)
              left join login_info v3
                on (v1.fgamefsk = v3.fgamefsk and v1.fplatformfsk = v3.fplatformfsk and v1.fuid = v3.fuid)
              left join async_info v4
                on (v1.fgamefsk = v4.fgamefsk and v1.fplatformfsk = v4.fplatformfsk and v1.fuid = v4.fuid)
              left join carrying_gamecoin_info v5
                on (v1.fgamefsk = v5.fgamefsk and v1.fplatformfsk = v5.fplatformfsk and v1.fuid = v5.fuid)
              left join safebox_gamecoin_info v6
                on (v1.fgamefsk = v6.fgamefsk and v1.fplatformfsk = v6.fplatformfsk and v1.fuid = v6.fuid)
              left join vip_info v7
                on (v1.fgamefsk = v7.fgamefsk and v1.fplatformfsk = v7.fplatformfsk and v1.fuid = v7.fuid)
              left join gameparty_info_all v8
                on (v1.fgamefsk = v8.fgamefsk and v1.fplatformfsk = v8.fplatformfsk and v1.fuid = v8.fuid)
              left join bankrupt_info v9
                on (v1.fgamefsk = v9.fgamefsk and v1.fplatformfsk = v9.fplatformfsk and v1.fuid = v9.fuid)
              left join relieve_info v10
                on (v1.fgamefsk = v10.fgamefsk and v1.fplatformfsk = v10.fplatformfsk and v1.fuid = v10.fuid)
              left join match_info v11
                on (v1.fgamefsk = v11.fgamefsk and v1.fplatformfsk = v11.fplatformfsk and v1.fuid = v11.fuid)
              left join pay_info v12
                on (v1.fgamefsk = v12.fgamefsk and v1.fplatformfsk = v12.fplatformfsk and v1.fuid = v12.fuid)
              left join join_match_info v13
                on (v1.fgamefsk = v13.fgamefsk and v1.fplatformfsk = v13.fplatformfsk and v1.fuid = v13.fuid)
              left join match_economy v14
                on (v1.fgamefsk = v14.fgamefsk and v1.fplatformfsk = v14.fplatformfsk and v1.fuid = v14.fuid)
              left join veda.texas_user_portrait t
                on (v1.fgamefsk = t.fgamefsk and v1.fplatformfsk = t.fplatformfsk and v1.fuid = t.fuid)
            """
            res = self.sql_exe(hql)
            if res != 0:
                return res

            return res


# 实例化执行
a = TexasUserPortrait(sys.argv[1:])
a()