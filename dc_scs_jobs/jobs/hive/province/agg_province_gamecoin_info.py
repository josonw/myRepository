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


class agg_province_gamecoin_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_游戏币相关
        create table if not exists dcnew.province_gamecoin_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fprovince           varchar(64),
                fcointype           varchar(64),
                fact_type           int,
                ftype               varchar(64),
                fnum                bigint,
                fusernum            bigint,
                fpayuser            bigint,
                fpaynum             bigint
          )comment '分省数据_游戏币相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_gamecoin_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1


        hql = """--游戏币流水
    insert overwrite table dcnew.province_gamecoin_info partition( dt="%(statdate)s" )
        SELECT distinct a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,a.fhallfsk
               ,a.fsubgamefsk
               ,a.fterminaltypefsk
               ,'-21379' fprovince
               ,a.fcointype
               ,a.fact_type
               ,a.fact_id ftype
               ,a.fcoin_num fnum
               ,a.fcoin_unum fusernum
               ,a.fcoin_pay_unum fpayusernum
               ,a.fcoin_pay_num fpaynum
          FROM dcnew.currencies_detail a
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fcointype = '11'
         union all
        SELECT distinct a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,a.fhallfsk
               ,a.fsubgamefsk
               ,a.fterminaltypefsk
               ,b.fplatformname fprovince
               ,a.fcointype
               ,a.fact_type
               ,a.fact_id ftype
               ,a.fcoin_num fnum
               ,a.fcoin_unum fusernum
               ,a.fcoin_pay_unum fpayusernum
               ,a.fcoin_pay_num fpaynum
          FROM dcnew.currencies_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
           and a.fcointype = '11'
         union all
        SELECT a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,-21379 fhallfsk
               ,a.fsubgamefsk
               ,a.fterminaltypefsk
               ,b.fplatformname fprovince
               ,a.fcointype
               ,a.fact_type
               ,a.fact_id ftype
               ,sum(a.fcoin_num) fnum
               ,sum(a.fcoin_unum) fusernum
               ,sum(a.fcoin_pay_unum) fpayusernum
               ,sum(a.fcoin_pay_num) fpaynum
          FROM dcnew.currencies_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
           and a.fcointype = '11'
         group by a.fdate,a.fgamefsk,a.fplatformfsk,a.fsubgamefsk,a.fterminaltypefsk,b.fplatformname,a.fcointype,a.fact_type,a.fact_id
         union all
        SELECT a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,-21379 fhallfsk
               ,-21379 fsubgamefsk
               ,a.fterminaltypefsk
               ,b.fplatformname fprovince
               ,a.fcointype
               ,a.fact_type
               ,a.fact_id ftype
               ,sum(a.fcoin_num) fnum
               ,sum(a.fcoin_unum) fusernum
               ,sum(a.fcoin_pay_unum) fpayusernum
               ,sum(a.fcoin_pay_num) fpaynum
          FROM dcnew.currencies_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
           and a.fcointype = '11'
         group by a.fdate,a.fgamefsk,a.fplatformfsk,a.fterminaltypefsk,b.fplatformname,a.fcointype,a.fact_type,a.fact_id
         union all
        SELECT a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,-21379 fhallfsk
               ,-21379 fsubgamefsk
               ,-21379 fterminaltypefsk
               ,b.fplatformname fprovince
               ,a.fcointype
               ,a.fact_type
               ,a.fact_id ftype
               ,sum(a.fcoin_num) fnum
               ,sum(a.fcoin_unum) fusernum
               ,sum(a.fcoin_pay_unum) fpayusernum
               ,sum(a.fcoin_pay_num) fpaynum
          FROM dcnew.currencies_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
           and a.fcointype = '11'
         group by a.fdate,a.fgamefsk,a.fplatformfsk,b.fplatformname,a.fcointype,a.fact_type,a.fact_id
        union all

        SELECT distinct a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,a.fhallfsk
               ,a.fsubgamefsk
               ,a.fterminaltypefsk
               ,'-21379' fprovince
               ,'0' fcointype
               ,case a.fdirection when 'IN' then 1 when 'OUT' then 2 end fdirection
               ,a.ftype
               ,a.fnum
               ,a.fusernum
               ,a.fpayusernum
               ,a.fpaynum
          FROM dcnew.gamecoin_detail a
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
         union all
        SELECT distinct a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,a.fhallfsk
               ,a.fsubgamefsk
               ,a.fterminaltypefsk
               ,b.fplatformname fprovince
               ,'0' fcointype
               ,case a.fdirection when 'IN' then 1 when 'OUT' then 2 end fdirection
               ,a.ftype
               ,a.fnum
               ,a.fusernum
               ,a.fpayusernum
               ,a.fpaynum
          FROM dcnew.gamecoin_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
         union all
        SELECT a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,-21379 fhallfsk
               ,a.fsubgamefsk
               ,a.fterminaltypefsk
               ,b.fplatformname fprovince
               ,'0' fcointype
               ,case a.fdirection when 'IN' then 1 when 'OUT' then 2 end fdirection
               ,a.ftype
               ,sum(a.fnum) fnum
               ,sum(a.fusernum) fusernum
               ,sum(a.fpayusernum) fpayusernum
               ,sum(a.fpaynum) fpaynum
          FROM dcnew.gamecoin_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
         group by a.fdate,a.fgamefsk,a.fplatformfsk,a.fsubgamefsk,a.fterminaltypefsk,b.fplatformname,a.fdirection,a.ftype
         union all
        SELECT a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,-21379 fhallfsk
               ,-21379 fsubgamefsk
               ,a.fterminaltypefsk
               ,b.fplatformname fprovince
               ,'0' fcointype
               ,case a.fdirection when 'IN' then 1 when 'OUT' then 2 end fdirection
               ,a.ftype
               ,sum(a.fnum) fnum
               ,sum(a.fusernum) fusernum
               ,sum(a.fpayusernum) fpayusernum
               ,sum(a.fpaynum) fpaynum
          FROM dcnew.gamecoin_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
         group by a.fdate,a.fgamefsk,a.fplatformfsk,a.fterminaltypefsk,b.fplatformname,a.fdirection,a.ftype
         union all
        SELECT a.fdate
               ,a.fgamefsk
               ,a.fplatformfsk
               ,-21379 fhallfsk
               ,-21379 fsubgamefsk
               ,-21379 fterminaltypefsk
               ,b.fplatformname fprovince
               ,'0' fcointype
               ,case a.fdirection when 'IN' then 1 when 'OUT' then 2 end fdirection
               ,a.ftype
               ,sum(a.fnum) fnum
               ,sum(a.fusernum) fusernum
               ,sum(a.fpayusernum) fpayusernum
               ,sum(a.fpaynum) fpaynum
          FROM dcnew.gamecoin_detail a
          join dim.bpid_map b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fhallfsk = b.fhallfsk
         where a.fversionfsk = -21379
           and a.dt = "%(statdate)s"
           and a.fchannelcode = -21379
           and a.fhallfsk <> -21379
         group by a.fdate,a.fgamefsk,a.fplatformfsk,b.fplatformname,a.fdirection,a.ftype
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res



#生成统计实例
a = agg_province_gamecoin_info(sys.argv[1:])
a()
