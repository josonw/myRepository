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


class agg_province_gamecoin_monthly_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_月_游戏币相关
        create table if not exists dcnew.province_gamecoin_monthly_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fversionfsk         bigint,
                fchannelcode        bigint,
                fprovince           varchar(64)     comment '省份',
                fcoin_type          int             comment '货币种类',
                fgold_out           bigint          comment '金币总发放',
                fgold_f_out         bigint          comment '金币免费发放',
                fgold_p_out         bigint          comment '金币付费发放',
                fgold_in            bigint          comment '金币总消耗',
                fgold_f_in          bigint          comment '金币免费消耗',
                fgold_p_in          bigint          comment '金币付费消耗',
                fgold_balance       bigint          comment '金币总结余',
                fgold_f_balance     bigint          comment '金币免费结余',
                fgold_p_balance     bigint          comment '金币付费结余'
                )comment '分省数据_月_游戏币相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_gamecoin_monthly_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        #四组组合，共14种
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fplatformfsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk) )
                union all """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fprovince),
                               (fgamefsk, fplatformfsk, fgame_id, fprovince) )
                union all """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fprovince),
                               (fgamefsk, fplatformfsk, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fprovince) ) """

        query = {'extend_group_1':extend_group_1,
                 'extend_group_2':extend_group_2,
                 'extend_group_3':extend_group_3,
                 'extend_group_4':extend_group_4,
                 'null_str_group_rule' : sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule' : sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum'         : """%(statdatenum)s""" }

        hql = """--游戏币流水
        drop table if exists work.province_gamecoin_m_tmp_1_%(statdatenum)s;
        create table work.province_gamecoin_m_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fprovince,
                 coalesce(p.fgame_id,%(null_int_report)d) fgame_id,
                 p.fact_type,
                 p.fact_id,
                 case when u.fuid is not null then 1 else 0 end fpay_flag, --是否付费:0否1是
                 p.fuid,
                 p.fnum
            from dim.user_gamecoin_stream_day p
            left join dim.user_pay_day u
              on p.fbpid = u.fbpid
             and p.fuid = u.fuid
             and u.dt between '%(ld_month_begin)s' and '%(ld_month_end)s'
            join dim.bpid_map c
              on p.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where p.dt between '%(ld_month_begin)s' and '%(ld_month_end)s'
             and p.fact_type in (1,2) --1:in,2:out
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        #汇总
        base_hql = """
                select fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       0 fcoin_type, --游戏币
                       sum(case when fact_type = 1 then fnum end) fgold_out,
                       sum(case when fact_type = 1 and fpay_flag = 0 then fnum end) fgold_f_out,
                       sum(case when fact_type = 1 and fpay_flag = 1 then fnum end) fgold_p_out,
                       sum(case when fact_type = 2 then fnum end) fgold_in,
                       sum(case when fact_type = 2 and fpay_flag = 0 then fnum end) fgold_f_in,
                       sum(case when fact_type = 2 and fpay_flag = 1 then fnum end) fgold_p_in,
                       sum(case when fact_type = 1 then fnum end)
                         - sum(case when fact_type = 2 then fnum end) fgold_balance,
                       sum(case when fact_type = 1 and fpay_flag = 0 then fnum end)
                         - sum(case when fact_type = 2 and fpay_flag = 0 then fnum end) fgold_f_balance,
                       sum(case when fact_type = 1 and fpay_flag = 1 then fnum end)
                         - sum(case when fact_type = 2 and fpay_flag = 1 then fnum end) fgold_p_balance
                  from work.province_gamecoin_m_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        #组合
        hql = ("""
                  drop table if exists work.province_gamecoin_m_tmp_%(statdatenum)s;
                create table work.province_gamecoin_m_tmp_%(statdatenum)s as """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--货币流水
        drop table if exists work.province_gamecoin_m_tmp_2_%(statdatenum)s;
        create table work.province_gamecoin_m_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fprovince,
                 coalesce(p.fgame_id,cast (0 as bigint)) fgame_id,
                 p.fact_type,
                 p.fact_id,
                 case when u.fuid is not null then 1 else 0 end fpay_flag, --是否付费:0否1是
                 p.fuid,
                 abs(p.fact_num) fnum
            from stage.pb_currencies_stream_stg p
            left join dim.user_pay_day u
              on p.fbpid = u.fbpid
             and p.fuid = u.fuid
             and u.dt between '%(ld_month_begin)s' and '%(ld_month_end)s'
            join dim.bpid_map c
              on p.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where p.dt between '%(ld_month_begin)s' and '%(ld_month_end)s'
             and p.fact_type in (1,2) --1:in,2:out
             and fcurrencies_type = '11'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        #汇总
        base_hql = """
                select fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       11 fcoin_type, --货币类型
                       sum(case when fact_type = 1 then fnum end) fgold_out,
                       sum(case when fact_type = 1 and fpay_flag = 0 then fnum end) fgold_f_out,
                       sum(case when fact_type = 1 and fpay_flag = 1 then fnum end) fgold_p_out,
                       sum(case when fact_type = 2 then fnum end) fgold_in,
                       sum(case when fact_type = 2 and fpay_flag = 0 then fnum end) fgold_f_in,
                       sum(case when fact_type = 2 and fpay_flag = 1 then fnum end) fgold_p_in,
                       sum(case when fact_type = 1 then fnum end)
                         - sum(case when fact_type = 2 then fnum end) fgold_balance,
                       sum(case when fact_type = 1 and fpay_flag = 0 then fnum end)
                         - sum(case when fact_type = 2 and fpay_flag = 0 then fnum end) fgold_f_balance,
                       sum(case when fact_type = 1 and fpay_flag = 1 then fnum end)
                         - sum(case when fact_type = 2 and fpay_flag = 1 then fnum end) fgold_p_balance
                  from work.province_gamecoin_m_tmp_2_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        #组合
        hql = ("""
                insert into table work.province_gamecoin_m_tmp_%(statdatenum)s  """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """insert overwrite table dcnew.province_gamecoin_monthly_info
                    partition(dt='%(ld_month_begin)s')
                  select '%(ld_month_begin)s' fdate,
                         fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         -1 fversionfsk,
                         -1 fchannel_code,
                         fprovince,
                         fcoin_type, --货币类型
                         nvl(sum(fgold_out),0) fgold_out,
                         nvl(sum(fgold_f_out),0) fgold_f_out,
                         nvl(sum(fgold_p_out),0) fgold_p_out,
                         nvl(sum(fgold_in),0) fgold_in,
                         nvl(sum(fgold_f_in),0) fgold_f_in,
                         nvl(sum(fgold_p_in),0) fgold_p_in,
                         nvl(sum(fgold_balance),0) fgold_balance,
                         nvl(sum(fgold_f_balance),0) fgold_f_balance,
                         nvl(sum(fgold_p_balance),0) fgold_p_balance
                    from work.province_gamecoin_m_tmp_%(statdatenum)s gs
                   group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince,fcoin_type

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.province_gamecoin_m_tmp_1_%(statdatenum)s;
                 drop table if exists work.province_gamecoin_m_tmp_2_%(statdatenum)s;
                 drop table if exists work.province_gamecoin_m_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res



#生成统计实例
a = agg_province_gamecoin_monthly_info(sys.argv[1:])
a()
