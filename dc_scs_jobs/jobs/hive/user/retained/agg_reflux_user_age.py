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


class agg_reflux_user_age(BaseStatModel):
    def create_tab(self):

        hql = """--流失回流用户年龄分布
        create table if not exists dcnew.reflux_user_age
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                freflux int comment '回流天数，如7表示7日回流',
                fdlq1 bigint comment '注册年龄小于1天的当天活跃用户人数',
                fdmq2lq3 bigint comment '注册年龄大于等于2天且小于等于3天的当天活跃用户人数',
                fdmq4lq7 bigint comment '注册年龄大于等于4天且小于等于7天的当天活跃用户人数',
                fdmq8lq14 bigint comment '注册年龄大于等于8天且小于等于14天的当天活跃用户人数',
                fdmq15lq30 bigint comment '注册年龄大于等于15天且小于等于30天的当天活跃用户人数',
                fdmq31lq90 bigint comment '注册年龄大于等于31天且小于等于90天的当天活跃用户人数',
                fdmq91lq180 bigint comment '注册年龄大于等于91天且小于等于180天的当天活跃用户人数',
                fdmq181lq365 bigint comment '注册年龄大于等于181天且小于等于365天的当天活跃用户人数',
                fdm365 bigint comment '注册年龄大于365天的当天活跃用户人数'
            )
            partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['flossdays','fage'],
                        'groups':[[1, 1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res


        hql = """--组合数据导入到正式表上
        insert overwrite table dcnew.reflux_user_age
        partition( dt="%(statdate)s" )
        select
            '%(statdate)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code,
            freflux,
            coalesce(count(distinct case when fage <= 1 then a.fuid else null end),0) fdlq1,
            coalesce(count(distinct case when fage > 1 and fage <= 3 then a.fuid else null end),0) fdmq2lq3,
            coalesce(count(distinct case when fage > 3 and fage <= 7 then a.fuid else null end),0) fdmq4lq7,
            coalesce(count(distinct case when fage > 7 and fage <= 14 then a.fuid else null end),0) fdmq8lq14,
            coalesce(count(distinct case when fage > 14 and fage <= 30 then a.fuid else null end),0) fdmq15lq30,
            coalesce(count(distinct case when fage > 30 and fage <= 90 then a.fuid else null end),0) fdmq31lq90,
            coalesce(count(distinct case when fage > 90 and fage <= 180 then a.fuid else null end),0) fdmq91lq180,
            coalesce(count(distinct case when fage > 180 and fage <= 365 then a.fuid else null end),0) fdmq181lq365,
            coalesce(count(distinct case when fage > 365 and fage <= 3650 then a.fuid else null end),0) fdm365
        from dim.user_reflux_array a
       where a.dt='%(statdate)s' and a.freflux_type = 'cycle'
        group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               freflux
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        return res


#生成统计实例
a = agg_reflux_user_age(sys.argv[1:])
a()
