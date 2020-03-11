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


class load_reg_user_main_additional(BaseStatModel):
    def create_tab(self):

        hql = """--用户城市切换信息表
        create table if not exists dim.reg_user_main_additional
        (
            fbpid           varchar(50)      comment 'BPID',
            fchannel_code   bigint           comment '渠道ID',
            fuid            bigint           comment '用户游戏ID',
            fsignup_at      string           comment '注册时间',
            fgender         tinyint          comment '性别',
            fversion_info   varchar(50)      comment '版本号',
            fentrance_id    bigint           comment '账号类型',
            fad_code        varchar(50)      comment '广告激活ID',
            fsource_path    varchar(100)     comment '来源路径',
            fm_imei         varchar(100)     comment '设备IMEI号',
            fm_dtype        varchar(100)     comment '终端型号',
            fm_pixel        varchar(100)     comment '分辨率',
            fm_os           varchar(100)     comment '系统类型',
            fm_network      varchar(100)     comment '网络类型',
            fm_operator     varchar(100)     comment '运营商',
            fplatform_uid   varchar(50)      comment '付费用户的平台uid',
            fpartner_info   varchar(32)      comment '代理商',
            fpromoter       varchar(100)     comment ' 推广员',
            fshare_key      varchar(100)     comment '分享新增的key',
            fip             varchar(64)      comment 'ip地址',
            flag            int              comment '标识位：1是上报数据，2是处理过的数据'
        )comment '新增用户补充省包切换用户'
               partitioned by(dt date)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.reg_user_main_additional partition (dt='%(statdate)s')
        select fbpid
               ,fchannel_code
               ,fuid
               ,fsignup_at
               ,fgender
               ,fversion_info
               ,fentrance_id
               ,fad_code
               ,fsource_path
               ,fm_imei
               ,fm_dtype
               ,fm_pixel
               ,fm_os
               ,fm_network
               ,fm_operator
               ,fplatform_uid
               ,fpartner_info
               ,fpromoter
               ,fshare_key
               ,fip
               ,1 flag
          from dim.reg_user_main t1
         where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert into table dim.reg_user_main_additional partition (dt='%(statdate)s')
        select t2.fbpid
               ,t1.fchannel_code
               ,t1.fuid
               ,t2.fflts_at fsignup_at
               ,t1.fgender
               ,t1.fversion_info
               ,t1.fentrance_id
               ,t1.fad_code
               ,t1.fsource_path
               ,t1.fm_imei
               ,t1.fm_dtype
               ,t1.fm_pixel
               ,t1.fm_os
               ,t1.fm_network
               ,t1.fm_operator
               ,t1.fplatform_uid
               ,t1.fpartner_info
               ,t1.fpromoter
               ,t1.fshare_key
               ,t1.fip
               ,2 flag
          from dim.reg_user_main_additional t1
          left join dim.city_change t2
            on t1.fbpid = t2.fp_bpid
           and t1.fuid = t2.fuid
           and t2.dt = '%(statdate)s'
          left join (select fbpid, fuid
                       from dim.reg_user_main_additional
                      where dt < '%(statdate)s'
               ) t3
            on t1.fuid = t3.fuid
           and t1.fbpid = t3.fbpid
         where t1.dt = '%(statdate)s'
           and t3.fuid is null
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = load_reg_user_main_additional(sys.argv[1:])
a()
