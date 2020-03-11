# -*- coding: UTF-8 -*-
#src     :dim.user_reflux,dim.reg_user_main_additional,dim.bpid_map
#dst     :dcnew.loss_user_reflux_age_dis
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


class agg_loss_user_reflux_age_dis(BaseStatModel):
    def create_tab(self):

        hql = """--流失回流用户年龄分布
        create table if not exists dcnew.loss_user_reflux_age_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flossdays           int       comment '流失回流天数，如7表示7日回流',
               fage                int       comment '用户年龄',
               floss_unum          bigint    comment '对应年龄单日回流用户人数',
               freflux_unum        bigint    comment '对应年龄回流用户人数'
               )comment '流失回流用户_年龄'
               partitioned by(dt date)
        location '/dw/dcnew/loss_user_reflux_age_dis'
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
        insert overwrite table dcnew.loss_user_reflux_age_dis
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate,
               fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               freflux flossdays,
               fage,
               count(distinct case when freflux_type='day' then fuid end) floss_unum,
               count(distinct case when freflux_type='cycle' then fuid end) freflux_unum
        from dim.user_reflux_array a
       where a.dt='%(statdate)s'
        group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               freflux,
               fage
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        return res


#生成统计实例
a = agg_loss_user_reflux_age_dis(sys.argv[1:])
a()
