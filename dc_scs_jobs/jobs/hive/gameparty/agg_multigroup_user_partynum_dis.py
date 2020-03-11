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

class agg_multigroup_user_partynum_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists dcnew.multigroup_user_partynum_dis
        (
          fdate              date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fsubgamefsk        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannelcode       bigint,
          fpartynumfsk       bigint,
          fpartynum          bigint,
          f1daypartynum      bigint,
          f3daypartynum      bigint,
          f7daypartynum      bigint,
          fregpartynum       bigint,
          factpartynum       bigint,
          f7dayactpartynum   bigint,
          fdbuydpartynum     bigint
        )
        partitioned by(dt date)
        location '/dw/dcnew/multigroup_user_partynum_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        extend_group = {'fields': ['fpartynumfsk'],
                        'groups': [[1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.multigroup_user_partynum_dis_tmp_a_%(statdatenum)s;
        create table work.multigroup_user_partynum_dis_tmp_a_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fsubgamefsk fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,coalesce(sum(fparty_num),0) fparty_num
            from dim.user_act_array t1
           where t1.dt='%(statdate)s'
           group by t1.fgamefsk
                    ,t1.fplatformfsk
                    ,t1.fhallfsk
                    ,t1.fsubgamefsk
                    ,t1.fterminaltypefsk
                    ,t1.fversionfsk
                    ,t1.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总act1
        hql = """
        drop table if exists work.multigroup_user_partynum_dis_tmp_%(statdatenum)s;
        create table work.multigroup_user_partynum_dis_tmp_%(statdatenum)s as
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,%(null_int_group_rule)d fchannel_code
                 ,case when coalesce(fparty_num,0) =0 then 1777140815
                       when fparty_num=1 then 1782301487
                       when 2<=fparty_num and fparty_num<=5 then 1782301488
                       when 6<=fparty_num and fparty_num<=10 then 1782301489
                       when 11<=fparty_num and fparty_num<=20 then 1777140817
                       when 21<=fparty_num and fparty_num<=30 then 1777140818
                       when 31<=fparty_num and fparty_num<=40 then 1777140819
                       when 41<=fparty_num and fparty_num<=50 then 1777140820
                       when 51<=fparty_num and fparty_num<=60 then 1777140821
                       when 61<=fparty_num and fparty_num<=70 then 1777140822
                       when 71<=fparty_num and fparty_num<=80 then 1777140823
                       when 81<=fparty_num and fparty_num<=90 then 1777140824
                       when 91<=fparty_num and fparty_num<=100 then 1777140825
                       when 101<=fparty_num and fparty_num<=150 then 1777140826
                       when 151<=fparty_num and fparty_num<=200 then 1777140827
                       when 201<=fparty_num and fparty_num<=300 then 1777140828
                       when 301<=fparty_num and fparty_num<=400 then 1777140829
                       when 401<=fparty_num and fparty_num<=500 then 1777140830
                       when 501<=fparty_num and fparty_num<=1000 then 1777140831
                  else 1777140832 end fpartynumfsk
                 ,count(distinct fuid) fpartynum
                 ,0 f1daypartynum
                 ,0 f3daypartynum
                 ,0 f7daypartynum
                 ,0 fregpartynum
                 ,count(distinct fuid) factpartynum
                 ,0 f7dayactpartynum
                 ,0 fdbuydpartynum
            from work.multigroup_user_partynum_dis_tmp_a_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                 ,case when coalesce(fparty_num,0) =0 then 1777140815
                       when fparty_num=1 then 1782301487
                       when 2<=fparty_num and fparty_num<=5 then 1782301488
                       when 6<=fparty_num and fparty_num<=10 then 1782301489
                       when 11<=fparty_num and fparty_num<=20 then 1777140817
                       when 21<=fparty_num and fparty_num<=30 then 1777140818
                       when 31<=fparty_num and fparty_num<=40 then 1777140819
                       when 41<=fparty_num and fparty_num<=50 then 1777140820
                       when 51<=fparty_num and fparty_num<=60 then 1777140821
                       when 61<=fparty_num and fparty_num<=70 then 1777140822
                       when 71<=fparty_num and fparty_num<=80 then 1777140823
                       when 81<=fparty_num and fparty_num<=90 then 1777140824
                       when 91<=fparty_num and fparty_num<=100 then 1777140825
                       when 101<=fparty_num and fparty_num<=150 then 1777140826
                       when 151<=fparty_num and fparty_num<=200 then 1777140827
                       when 201<=fparty_num and fparty_num<=300 then 1777140828
                       when 301<=fparty_num and fparty_num<=400 then 1777140829
                       when 401<=fparty_num and fparty_num<=500 then 1777140830
                       when 501<=fparty_num and fparty_num<=1000 then 1777140831
                   else 1777140832 end
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.multigroup_user_partynum_dis_tmp_b_%(statdatenum)s;
        create table work.multigroup_user_partynum_dis_tmp_b_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,coalesce(sum(fparty_num),0) fparty_num
                 ,t1.dt reg_day
            from dim.reg_user_array t1
            left join dim.user_gameparty_array t2
              on t1.fuid = t2.fuid
             and t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fhallfsk = t2.fhallfsk
             and t1.fgame_id = t2.fsubgamefsk
             and t1.fterminaltypefsk = t2.fterminaltypefsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.fchannel_code = t2.fchannelcode
             and t2.dt = '%(statdate)s'
           where cast(t1.dt as string) in ("%(statdate)s","%(ld_1day_ago)s", "%(ld_3day_ago)s", "%(ld_7day_ago)s")
           group by t1.fgamefsk
                    ,t1.fplatformfsk
                    ,t1.fhallfsk
                    ,t1.fgame_id
                    ,t1.fterminaltypefsk
                    ,t1.fversionfsk
                    ,t1.fuid
                    ,t1.dt;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总act1
        hql = """
        insert into table work.multigroup_user_partynum_dis_tmp_%(statdatenum)s
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,%(null_int_group_rule)d fchannel_code
                 ,case when coalesce(fparty_num,0) =0 then 1777140815
                       when fparty_num=1 then 1782301487
                       when 2<=fparty_num and fparty_num<=5 then 1782301488
                       when 6<=fparty_num and fparty_num<=10 then 1782301489
                       when 11<=fparty_num and fparty_num<=20 then 1777140817
                       when 21<=fparty_num and fparty_num<=30 then 1777140818
                       when 31<=fparty_num and fparty_num<=40 then 1777140819
                       when 41<=fparty_num and fparty_num<=50 then 1777140820
                       when 51<=fparty_num and fparty_num<=60 then 1777140821
                       when 61<=fparty_num and fparty_num<=70 then 1777140822
                       when 71<=fparty_num and fparty_num<=80 then 1777140823
                       when 81<=fparty_num and fparty_num<=90 then 1777140824
                       when 91<=fparty_num and fparty_num<=100 then 1777140825
                       when 101<=fparty_num and fparty_num<=150 then 1777140826
                       when 151<=fparty_num and fparty_num<=200 then 1777140827
                       when 201<=fparty_num and fparty_num<=300 then 1777140828
                       when 301<=fparty_num and fparty_num<=400 then 1777140829
                       when 401<=fparty_num and fparty_num<=500 then 1777140830
                       when 501<=fparty_num and fparty_num<=1000 then 1777140831
                   else 1777140832 end fpartynumfsk
                  ,0 fpartynum
                  ,count(distinct case when reg_day="%(ld_1day_ago)s" then fuid else null end) f1daypartynum
                  ,count(distinct case when reg_day="%(ld_3day_ago)s" then fuid else null end) f3daypartynum
                  ,count(distinct case when reg_day="%(ld_7day_ago)s" then fuid else null end) f7daypartynum
                  ,count(distinct case when reg_day="%(statdate)s" then fuid else null end) fregpartynum
                  ,0 factpartynum
                  ,0 f7dayactpartynum
                  ,0 fdbuydpartynum
            from work.multigroup_user_partynum_dis_tmp_b_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                 ,case when coalesce(fparty_num,0) =0 then 1777140815
                       when fparty_num=1 then 1782301487
                       when 2<=fparty_num and fparty_num<=5 then 1782301488
                       when 6<=fparty_num and fparty_num<=10 then 1782301489
                       when 11<=fparty_num and fparty_num<=20 then 1777140817
                       when 21<=fparty_num and fparty_num<=30 then 1777140818
                       when 31<=fparty_num and fparty_num<=40 then 1777140819
                       when 41<=fparty_num and fparty_num<=50 then 1777140820
                       when 51<=fparty_num and fparty_num<=60 then 1777140821
                       when 61<=fparty_num and fparty_num<=70 then 1777140822
                       when 71<=fparty_num and fparty_num<=80 then 1777140823
                       when 81<=fparty_num and fparty_num<=90 then 1777140824
                       when 91<=fparty_num and fparty_num<=100 then 1777140825
                       when 101<=fparty_num and fparty_num<=150 then 1777140826
                       when 151<=fparty_num and fparty_num<=200 then 1777140827
                       when 201<=fparty_num and fparty_num<=300 then 1777140828
                       when 301<=fparty_num and fparty_num<=400 then 1777140829
                       when 401<=fparty_num and fparty_num<=500 then 1777140830
                       when 501<=fparty_num and fparty_num<=1000 then 1777140831
                   else 1777140832 end
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.multigroup_user_partynum_dis_tmp_c_%(statdatenum)s;
        create table work.multigroup_user_partynum_dis_tmp_c_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,coalesce(sum(t2.fparty_num),0) fparty_num
            from dim.reg_user_array t1
            left join dim.user_gameparty_array t2
              on t1.fuid = t2.fuid
             and t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fhallfsk = t2.fhallfsk
             and t1.fgame_id = t2.fsubgamefsk
             and t1.fterminaltypefsk = t2.fterminaltypefsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.fchannel_code = t2.fchannelcode
             and t2.dt = '%(ld_1day_ago)s'
            join dim.user_act_array t3
              on t1.fuid = t3.fuid
             and t1.fgamefsk = t3.fgamefsk
             and t1.fplatformfsk = t3.fplatformfsk
             and t1.fhallfsk = t3.fhallfsk
             and t1.fgame_id = t3.fsubgamefsk
             and t1.fterminaltypefsk = t3.fterminaltypefsk
             and t1.fversionfsk = t3.fversionfsk
             and t1.fchannel_code = t3.fchannelcode
             and t3.dt = '%(statdate)s'
           where t1.dt = '%(ld_1day_ago)s'
           group by t1.fgamefsk
                    ,t1.fplatformfsk
                    ,t1.fhallfsk
                    ,t1.fgame_id
                    ,t1.fterminaltypefsk
                    ,t1.fversionfsk
                    ,t1.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总act1
        hql = """
        insert into table work.multigroup_user_partynum_dis_tmp_%(statdatenum)s
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,%(null_int_group_rule)d fchannel_code
                 ,case when coalesce(fparty_num,0) =0 then 1777140815
                       when fparty_num=1 then 1782301487
                       when 2<=fparty_num and fparty_num<=5 then 1782301488
                       when 6<=fparty_num and fparty_num<=10 then 1782301489
                       when 11<=fparty_num and fparty_num<=20 then 1777140817
                       when 21<=fparty_num and fparty_num<=30 then 1777140818
                       when 31<=fparty_num and fparty_num<=40 then 1777140819
                       when 41<=fparty_num and fparty_num<=50 then 1777140820
                       when 51<=fparty_num and fparty_num<=60 then 1777140821
                       when 61<=fparty_num and fparty_num<=70 then 1777140822
                       when 71<=fparty_num and fparty_num<=80 then 1777140823
                       when 81<=fparty_num and fparty_num<=90 then 1777140824
                       when 91<=fparty_num and fparty_num<=100 then 1777140825
                       when 101<=fparty_num and fparty_num<=150 then 1777140826
                       when 151<=fparty_num and fparty_num<=200 then 1777140827
                       when 201<=fparty_num and fparty_num<=300 then 1777140828
                       when 301<=fparty_num and fparty_num<=400 then 1777140829
                       when 401<=fparty_num and fparty_num<=500 then 1777140830
                       when 501<=fparty_num and fparty_num<=1000 then 1777140831
                   else 1777140832 end fpartynumfsk
                 ,0 fpartynum
                 ,0 f1daypartynum
                 ,0 f3daypartynum
                 ,0 f7daypartynum
                 ,0 fregpartynum
                 ,0 factpartynum
                 ,0 f7dayactpartynum
                 ,count(distinct t.fuid) fdbuydpartynum
            from work.multigroup_user_partynum_dis_tmp_c_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                 ,case when coalesce(fparty_num,0) =0 then 1777140815
                       when fparty_num=1 then 1782301487
                       when 2<=fparty_num and fparty_num<=5 then 1782301488
                       when 6<=fparty_num and fparty_num<=10 then 1782301489
                       when 11<=fparty_num and fparty_num<=20 then 1777140817
                       when 21<=fparty_num and fparty_num<=30 then 1777140818
                       when 31<=fparty_num and fparty_num<=40 then 1777140819
                       when 41<=fparty_num and fparty_num<=50 then 1777140820
                       when 51<=fparty_num and fparty_num<=60 then 1777140821
                       when 61<=fparty_num and fparty_num<=70 then 1777140822
                       when 71<=fparty_num and fparty_num<=80 then 1777140823
                       when 81<=fparty_num and fparty_num<=90 then 1777140824
                       when 91<=fparty_num and fparty_num<=100 then 1777140825
                       when 101<=fparty_num and fparty_num<=150 then 1777140826
                       when 151<=fparty_num and fparty_num<=200 then 1777140827
                       when 201<=fparty_num and fparty_num<=300 then 1777140828
                       when 301<=fparty_num and fparty_num<=400 then 1777140829
                       when 401<=fparty_num and fparty_num<=500 then 1777140830
                       when 501<=fparty_num and fparty_num<=1000 then 1777140831
                   else 1777140832 end
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.multigroup_user_partynum_dis
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate,
                   fgamefsk,
                   fplatformfsk,
                   fhallfsk,
                   fgame_id,
                   fterminaltypefsk,
                   fversionfsk,
                   fchannel_code,
                   fpartynumfsk,
                   sum(fpartynum) fpartynum,
                   sum(f1daypartynum) f1daypartynum,
                   sum(f3daypartynum) f3daypartynum,
                   sum(f7daypartynum) f7daypartynum,
                   sum(fregpartynum) fregpartynum,
                   sum(factpartynum) factpartynum,
                   sum(f7dayactpartynum) f7dayactpartynum,
                   sum(fdbuydpartynum) fdbuydpartynum
              from work.multigroup_user_partynum_dis_tmp_%(statdatenum)s a
             group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                      fpartynumfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.multigroup_user_partynum_dis_tmp_a_%(statdatenum)s;
                 drop table if exists work.multigroup_user_partynum_dis_tmp_b_%(statdatenum)s;
                 drop table if exists work.multigroup_user_partynum_dis_tmp_c_%(statdatenum)s;
                 drop table if exists work.multigroup_user_partynum_dis_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res
# 生成统计实例
a = agg_multigroup_user_partynum_dis(sys.argv[1:])
a()
