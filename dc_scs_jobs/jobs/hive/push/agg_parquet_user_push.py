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

from libs.ImpalaSql import ImpalaSql


class agg_parquet_user_push(BaseStatModel):
    def create_tab(self):
        pass
        # hql = """
        # create table if not exists dim.parquet_user_push (
#                fdate              string   comment '日期',
#                fappid             string   comment 'appid',
#                fuid               string   comment '用户id',
#                ftoken             string   comment 'token',
#                flts_at            string   comment '上报时间',
#                flogin_at          string   comment '最后登录时间',
#                fsignup_at         string   comment '注册时间',
#                fis_open           int      comment '是否开启推送',
#                fversion_info      string   comment '版本号',
#                fuser_gamecoins    bigint   comment '携带游戏币',
#                fm_dtype           string   comment '用户登录设备是否越狱',
#                fentrance_id       int      comment '账户类型',
#                fis_paid           int      comment '是否付费',
#                fvip_level         int      comment 'VIP级别',
#                fgender            int      comment '性别',
#                fage               int      comment '年龄--无数据',
#                fbrith             int      comment '生日--无数据',
#                fchannel_code      string   comment '渠道邀请ID',
#                fparty_num         bigint   comment '上一次玩牌局数',
#                fparty_num_total   bigint   comment '累计玩牌局数',
#                fparty_num_day     bigint   comment '累计玩牌天数',
#                fy_party_num       bigint   comment '上一次约牌房玩牌局数',
#                fy_party_num_total bigint   comment '累计约牌房玩牌局数',
#                fy_party_num_day   bigint   comment '累计约牌房玩牌天数',
#                fpay_num           float    comment '上一次付费额度',
#                fpay_num_total     float    comment '累计付费额度',
#                frupt_num          bigint   comment '上一次破产次数',
#                frupt_num_total    bigint   comment '累计破产次数'
#                )comment '推送用户信息表'
        # partitioned by (fbpid string,fpush_platform string)
        # stored as parquet;
#
         # """
        # impala = ImpalaSql(host='10.30.101.103')
        # res = impala.exe_sql(hql)
        # if res != 0:
            # return res
        # return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        statdate = self.stat_date
        query = {'statdate': statdate}

        # 备份
        hql = ("""invalidate metadata;
        insert overwrite table dim.parquet_user_push_last
               partition(fbpid,fpush_platform)
               select fdate
                      ,fappid
                      ,fuid
                      ,ftoken
                      ,flts_at
                      ,flogin_at
                      ,fsignup_at
                      ,fis_open
                      ,fversion_info
                      ,fuser_gamecoins
                      ,fm_dtype
                      ,fentrance_id
                      ,fis_paid
                      ,fvip_level
                      ,fgender
                      ,fage
                      ,fbrith
                      ,fchannel_code
                      ,fparty_num
                      ,fparty_num_total
                      ,fparty_num_day
                      ,fy_party_num
                      ,fy_party_num_total
                      ,fy_party_num_day
                      ,fpay_num
                      ,fpay_num_total
                      ,frupt_num
                      ,frupt_num_total
                      ,fbpid
                      ,fpush_platform
                 from dim.parquet_user_push t;
                invalidate metadata;
        """) % query
        print hql
        impala = ImpalaSql(host='10.30.101.104')
        res = impala.exe_sql(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = ("""invalidate metadata;
        insert overwrite table dim.parquet_user_push
               partition(fbpid,fpush_platform)
       select "%(statdate)s" fdate
              ,t1.fappid
              ,cast (t1.fuid as string) fuid
              ,t1.ftoken
              ,t1.flts_at
              ,t1.flogin_at
              ,t1.fsignup_at
              ,t1.fis_open
              ,t1.fversion_info
              ,t1.fuser_gamecoins
              ,t1.fm_dtype
              ,t1.fentrance_id
              ,t1.fis_paid
              ,t1.fvip_level
              ,t1.fgender
              ,null fage
              ,null fbrith
              ,t1.fchannel_code
              ,t2.fparty_num
              ,t2.fparty_num_total
              ,t2.fparty_num_day
              ,t2.fy_party_num
              ,t2.fy_party_num_total
              ,t2.fy_party_num_day
              ,t3.fpay_num
              ,t3.fpay_num_total
              ,t4.frupt_num
              ,t4.frupt_num_total
              ,t1.fbpid
              ,t1.fpush_platform
         from (select fbpid
                      ,fappid
                      ,fuid
                      ,ftoken
                      ,flts_at
                      ,flogin_at
                      ,fsignup_at
                      ,fis_open
                      ,fversion_info
                      ,fuser_gamecoins
                      ,fm_dtype
                      ,fentrance_id
                      ,fis_paid
                      ,fvip_level
                      ,fgender
                      ,fchannel_code
                      ,fparty_num
                      ,fpay_num
                      ,fparty_num_total
                      ,fpay_num_total
                      ,fpush_platform
                      ,row_number() over(partition by fbpid, fpush_platform, fuid order by flts_at desc) row_num
                 from (select fbpid
                              ,fappid
                              ,fuid
                              ,ftoken
                              ,flts_at
                              ,flogin_at
                              ,fsignup_at
                              ,fis_open
                              ,fversion_info
                              ,fuser_gamecoins
                              ,fm_dtype
                              ,fentrance_id
                              ,fis_paid
                              ,fvip_level
                              ,fgender
                              ,fchannel_code
                              ,fparty_num
                              ,fpay_num
                              ,fparty_num_total
                              ,fpay_num_total
                              ,fpush_platform
                         from (select fbpid,
                                      fappid,
                                      fuid,
                                      ftoken,
                                      flts_at,
                                      flogin_at,
                                      fsignup_at,
                                      fis_open,
                                      fversion_info,
                                      fuser_gamecoins,
                                      fm_dtype,
                                      fentrance_id,
                                      fis_paid,
                                      fvip_level,
                                      fgender,
                                      fchannel_code,
                                      fparty_num,
                                      fpay_num,
                                      fparty_num_total,
                                      fpay_num_total,
                                      fpush_platform,
                                      row_number() over(partition by fbpid, fpush_platform, fuid order by flts_at desc) row_num
                                 from stage.user_push_stg
                                where dt = '%(statdate)s'
                                  and fpush_platform in ('umeng', 'boyaa', 'goolge', 'ios') --目前只有四种推送2018-1-26
                              ) t
                        where row_num = 1
                         union all
                       select fbpid
                              ,fappid
                              ,cast (fuid as bigint) fuid
                              ,ftoken
                              ,flts_at
                              ,flogin_at
                              ,fsignup_at
                              ,fis_open
                              ,fversion_info
                              ,fuser_gamecoins
                              ,fm_dtype
                              ,fentrance_id
                              ,fis_paid
                              ,fvip_level
                              ,fgender
                              ,fchannel_code
                              ,fparty_num
                              ,fpay_num
                              ,fparty_num_total
                              ,fpay_num_total
                              ,fpush_platform
                         from dim.parquet_user_push_last t
                      ) t1
              ) t1
         left join dim.parquet_user_push_gameparty t2
           on t1.fbpid = t2.fbpid
          and t1.fuid = t2.fuid
          and t2.dt = '%(statdate)s'
         left join dim.parquet_user_push_pay t3
           on t1.fbpid = t3.fbpid
          and t1.fuid = t3.fuid
          and t3.dt = '%(statdate)s'
         left join dim.parquet_user_push_rupt t4
           on t1.fbpid = t4.fbpid
          and t1.fuid = t4.fuid
          and t4.dt = '%(statdate)s'
        where row_num = 1;

                invalidate metadata;
        """) % query
        print hql
        impala = ImpalaSql(host='10.30.101.104')
        res = impala.exe_sql(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_parquet_user_push(sys.argv[1:])
a()
