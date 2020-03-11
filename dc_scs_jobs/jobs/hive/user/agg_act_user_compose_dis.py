# -*- coding: UTF-8 -*-
#src     :stage.payment_stream_stg,dim.user_act,dim.user_pay,dim.bpid_map
#dst     :dcnew.loss_user_act_pay
#authot  :SimonRen
#date    :2016-09-02


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


class agg_act_user_compose_dis(BaseStatModel):
    def create_tab(self):

        hql = """create table if not exists dcnew.act_user_compose_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fdaucnt bigint comment '每日活跃用户数目',
                f7dsucnt bigint comment '每日活跃中7天之内注册的用户数目，即新用户',
                f7drtnucnt bigint comment '每日活跃中7日回流用户的数目',
                fdapaiducnt bigint comment '每日活跃中的历史付费用户'
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res


        hql = """--取用户数据
        drop table if exists work.act_user_compose_dis_%(statdatenum)s;
        create table work.act_user_compose_dis_%(statdatenum)s  as
                select t.fgamefsk,
                       t.fplatformfsk,
                       t.fhallfsk,
                       t.hallmode,
                       t.fterminaltypefsk,
                       t.fversionfsk,
                       coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                       coalesce(a.fchannel_code,%(null_int_report)d) fchannel_code,
                       a.fuid,
                       e.fuid reflux_uid,
                       c.fuid 7d_reg_uid,
                       d.fuid pay_uid
                  from dim.user_act a
                  left join dim.user_reflux e
                    on a.fbpid=e.fbpid
                   and a.fuid=e.fuid
                   and e.dt = '%(statdate)s' and e.freflux = 7
                  left join dim.reg_user_main_additional c
                    on a.fbpid = c.fbpid
                   and a.fuid = c.fuid
                   and c.dt >= '%(ld_7day_ago)s'and c.dt <= '%(statdate)s'
                  left join dim.user_act_paid d
                    on a.fbpid=d.fbpid
                   and a.fuid=d.fuid
                   and d.dt = '%(statdate)s'
                  join dim.bpid_map t
                    on a.fbpid = t.fbpid
                 where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取组合之后的用户数据
                 select "%(statdate)s" fdate,
                        fgamefsk,
                        coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                        coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                        coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                        coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                        coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                        coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                        count(distinct fuid) fdaucnt,
                        count(distinct 7d_reg_uid) f7dsucnt,
                        count(distinct reflux_uid) f7drtnucnt,
                        count(distinct pay_uid) fdapaiducnt
                   from work.act_user_compose_dis_%(statdatenum)s
                  where hallmode = %(hallmode)s
                  group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.act_user_compose_dis
              partition(dt='%(statdate)s')
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.act_user_compose_dis_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_act_user_compose_dis(sys.argv[1:])
a()
