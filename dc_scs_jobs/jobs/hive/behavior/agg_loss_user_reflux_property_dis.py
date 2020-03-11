# -*- coding: UTF-8 -*-
#src     :dim.user_reflux,dim.user_gamecoin_stream,dim.bpid_map
#dst     :dcnew.loss_user_reflux_property_dis
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


class agg_loss_user_reflux_property_dis(BaseStatModel):
    def create_tab(self):

        hql = """--流失回流用户资产分布
        create table if not exists dcnew.loss_user_reflux_property_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               flossdays           int       comment '流失回流天数，如7表示7日回流',
               fproperty           string    comment '用户资产',
               floss_unum          bigint    comment '对应资产单日回流用户人数',
               freflux_unum        bigint    comment '对应资产回流用户人数'
               )comment '流失回流用户_资产'
               partitioned by(dt date)
        location '/dw/dcnew/loss_user_reflux_property_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['flossdays','fproperty'],
                        'groups':[[1, 1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.loss_reflux_gc_tmp_b_%(statdatenum)s;
        create table work.loss_reflux_gc_tmp_b_%(statdatenum)s as
                        select a.fgamefsk,
                               a.fplatformfsk,
                               a.fhallfsk,
                               a.fterminaltypefsk,
                               a.fversionfsk,
                               a.fgame_id,
                               a.fchannel_code,
                               a.fuid,
                               a.freflux_type,
                               a.freflux flossdays,
                               case when a.gamecoins <=0 then 0
                                    when a.gamecoins >=1 and a.gamecoins <1000 then 1
                                    when a.gamecoins >=1000 and a.gamecoins <5000 then 1000
                                    when a.gamecoins >=5000 and a.gamecoins <10000 then 5000
                                    when a.gamecoins >=10000 and a.gamecoins <50000 then 10000
                                    when a.gamecoins >=50000 and a.gamecoins <100000 then 50000
                                    when a.gamecoins >=100000 and a.gamecoins <500000 then 100000
                                    when a.gamecoins >=500000 and a.gamecoins <1000000 then 500000
                                    when a.gamecoins >=1000000 and a.gamecoins <5000000 then 1000000
                                    when a.gamecoins >=5000000 and a.gamecoins <10000000 then 5000000
                                    when a.gamecoins >=10000000 and a.gamecoins<50000000 then 10000000
                                    when a.gamecoins >=50000000 and a.gamecoins<100000000 then 50000000
                                    when a.gamecoins >=100000000 and a.gamecoins<1000000000 then 100000000
                                    else 1000000000 end fproperty
                          from dim.user_reflux_array a
                         where a.dt='%(statdate)s'
                           and a.gamecoins is not null
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合数据导入到正式表上
        insert overwrite table dcnew.loss_user_reflux_property_dis
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate,
               fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               flossdays,
               fproperty,
               count(distinct case when freflux_type='day' then fuid end) floss_unum,
               count(distinct case when freflux_type='cycle' then fuid end) freflux_unum
        from work.loss_reflux_gc_tmp_b_%(statdatenum)s
        group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
               flossdays,
               fproperty
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.loss_reflux_gc_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_loss_user_reflux_property_dis(sys.argv[1:])
a()
