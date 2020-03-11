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


class agg_push_config(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.push_config (
               fdate                    date,
               fbpid                    varchar(50)          comment 'BPID',
               flts_at                  string               comment '时间戳',
               fpush_id                 int                  comment '推送id',
               fproject_id              int                  comment '项目id',
               ftitle                   varchar(20)          comment '推送标题',
               fcontent                 varchar(1000)        comment '推送内容',
               fbegin_time              string               comment '推送开始时间',
               fend_time                string               comment '推送结束时间',
               fextra_behavior          int                  comment '附加行为的值',
               fpush_package            int                  comment '推送礼包的值',
               freceiver                string               comment '接收用户的值',
               fuser_type               int                  comment '1全部用户  2是指定id  3是条件用户',
               fuids                    string               comment '当user_type=2时使用，逗号分隔',
               fpush_num                int                  comment '目标设备数',
               fsuccess_num             int                  comment '成功接收消息的用户数',
               fappear_num              int                  comment '展现次数',
               fclick_num               int                  comment '点击了消息的用户数',
               fbackup_push_result      string               comment 'boyaa备用推送（个推）结果，json存储',
               fstatus                  int                  comment '1=>待推送,2=>推送中,3=>推送成功,4=>推送失败,5=>推送部分失败,10=>禁用,11=>删除',
               fgt_content_ids          string               comment '个推的content_id',
               fretry_status            int                  comment '重试状态 0没有重试  1待重试  2重试中 3重试成功 4重试失败 5重试部分失败',
               fget_clientid_status     int                  comment '取client_id的状态，1=>待取,2=>正在取,3=>已经获取',
               fget_clientid_type       int                  comment '获取client id的方式,1=>博雅推送,2=>apns,3=>gcm,4=>个推,5=>(博雅+个推),6=>个推+(博雅+个推)',
               ffail_reason             varchar(200)         comment '推送失败原因',
               fget_clientid_time       string               comment '取回client_id的时间',
               fgroup_id                int                  comment '归属于哪个组(用于一次生成多个推送时)',
               fadd_uid                 int                  comment '推送添加者',
               fadd_time                string               comment '推送添加时间',
               fupdate_time             string               comment '修改时间',
               ftoken_id                int                  comment 'fk,对应token表的id',
               fext                     varchar(255)         comment '',
               fbookmark_id             int                  comment 'fk,对应bookmark表的id'
               )comment '推送配置信息'
               partitioned by(dt string)
        location '/dw/bud_dm/push_config';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        # 取基础数据
        hql = """insert overwrite table bud_dm.push_config
            partition(dt='%(statdate)s')
              select '%(statdate)s' fdate
                    ,fbpid
                    ,flts_at
                    ,fpush_id
                    ,fproject_id
                    ,ftitle
                    ,fcontent
                    ,fbegin_time
                    ,fend_time
                    ,fextra_behavior
                    ,fpush_package
                    ,freceiver
                    ,fuser_type
                    ,fuids
                    ,fpush_num
                    ,fsuccess_num
                    ,fappear_num
                    ,fclick_num
                    ,fbackup_push_result
                    ,fstatus
                    ,fgt_content_ids
                    ,fretry_status
                    ,fget_clientid_status
                    ,fget_clientid_type
                    ,ffail_reason
                    ,fget_clientid_time
                    ,fgroup_id
                    ,fadd_uid
                    ,fadd_time
                    ,fupdate_time
                    ,ftoken_id
                    ,fext
                    ,fbookmark_id
                from stage.push_config_stg t
               where t.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_push_config(sys.argv[1:])
a()
