# -*- coding: UTF-8 -*-
#src     :stage.user_bankrupt_stg,dim.bpid_map,stage.user_bankrupt_stg,dim.user_pay_day,stage.payment_stream_stg,dim.user_login_additional
#dst     :dcnew.user_bankrupt_info,dcnew.partytype_dim
#authot  :SimonRen
#date    :2016-09-05


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


class agg_user_bankrupt_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_bankrupt_info (
               fdate                  string,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fdis_type              int             comment '分布类型:1,场次.2,底注.3,场景.4,资产',
               fdis_list              string          comment '分布数据列表',
               fbankrupt_unum         bigint          comment '破产人数',
               fbankrupt_cnt          bigint          comment '破产次数',
               fbank_30min_pay_unum   bigint          comment '破产三十分钟内付费人数',
               fbank_30min_income     decimal(20,2)   comment '破产三十分钟内付费额度',
               fbank_30min_paycnt     bigint          comment '破产三十分钟内付费次数',
               fpname                 string          comment '子游戏名称'
               )comment '破产用户场景分布'
               partitioned by(dt string)
        location '/dw/dcnew/user_bankrupt_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

#       hql = """--取最新的游戏场名称
#       insert into table dcnew.partytype_dim
#           select c.fname fsk, c.fname fname, c.fgamefsk, null fplatformfsk, null fversionfsk, null fterminalfsk
#           from (
#               select nvl(fplayground_title, '未定义') fname, fgamefsk
#               from stage.user_bankrupt_stg a
#               join dim.bpid_map b
#                   on a.fbpid = b.fbpid
#               where a.dt="%(statdate)s"
#               group by nvl(fplayground_title, '未定义'), fgamefsk
#           ) c
#           left outer join dcnew.partytype_dim d
#               on c.fname = d.fname and c.fgamefsk = d.fgamefsk
#           where d.fname is null
#       """
#       res = self.sql_exe(hql)
#       if res != 0:
#           return res

        hql = """--底注分布、场次分布、场景分布
        drop table if exists work.user_brupt_info_tmp_b_%(statdatenum)s;
        create table work.user_brupt_info_tmp_b_%(statdatenum)s as
                     select /*+ MAPJOIN(d) */ fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk,hallmode,
                            coalesce(a.fgame_id,cast(%(null_int_report)d as bigint)) fgame_id,
                            coalesce(a.fpname,cast(%(null_int_report)d as string)) fpname,
                            coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                            fuphill_pouring pouring_num, --底注分布
                            nvl(fplayground_title, '未定义') partytype, --场次分布
                            nvl(fscene, '未定义') bankruptscene, --场景分布
                            a.fuid,
                            0 fbankrupt_cnt,
                            c.fplatform_uid,
                            c.fcoins_num * c.frate fbank_30min_income,
                            forder_id
                       from stage.user_bankrupt_stg a
                       join dim.user_pay_day b
                         on a.fbpid = b.fbpid
                        and a.fuid = b.fuid
                        and b.dt='%(statdate)s'
                       join stage.payment_stream_stg c
                         on b.fplatform_uid = c.fplatform_uid
                        and b.fbpid = c.fbpid
                        and c.dt='%(statdate)s'
                       join dim.bpid_map  d
                         on a.fbpid=d.fbpid
                      where a.dt='%(statdate)s'
                        and unix_timestamp(c.fdate)-1800 <= unix_timestamp(a.frupt_at)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--底注分布、场次分布、场景分布
            insert into table work.user_brupt_info_tmp_b_%(statdatenum)s
                     select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
                            coalesce(a.fgame_id,cast(%(null_int_report)d as bigint)) fgame_id,
                            coalesce(a.fpname,cast(%(null_int_report)d as string)) fpname,
                            coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                            fuphill_pouring pouring_num, --底注分布
                            nvl(fplayground_title, '未定义') partytype, --场次分布
                            nvl(fscene, '未定义') bankruptscene, --场景分布
                            a.fuid ,
                            count(1) fbankrupt_cnt,
                            null fplatform_uid,
                            null fbank_30min_income,
                            null forder_id
                       from stage.user_bankrupt_stg a
                       join dim.bpid_map  c
                         on a.fbpid=c.fbpid
                      where a.dt="%(statdate)s"
                      group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk,hallmode, a.fgame_id, a.fpname, a.fchannel_code,
                               fuphill_pouring,
                               nvl(fplayground_title, '未定义'),
                               nvl(fscene, '未定义'),
                               a.fuid

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--资产分布
        drop table if exists work.user_brupt_info_tmp_c_%(statdatenum)s;
        create table work.user_brupt_info_tmp_c_%(statdatenum)s as
            select fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
                   coalesce(fgame_id,%(null_int_report)d) fgame_id,
                   coalesce(b.fpname,cast(%(null_int_report)d as string)) fpname,
                   coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                   case when l.user_gamecoins_num <= 0 then '0'
                        when l.user_gamecoins_num >= 1 and l.user_gamecoins_num < 5000 then '1-5000'
                        when l.user_gamecoins_num >= 5000 and l.user_gamecoins_num < 10000 then '5000-1万'
                        when l.user_gamecoins_num >= 10000 and l.user_gamecoins_num < 50000 then '1万-5万'
                        when l.user_gamecoins_num >= 50000 and l.user_gamecoins_num < 100000 then '5万-10万'
                        when l.user_gamecoins_num >= 100000 and l.user_gamecoins_num < 500000 then '10万-50万'
                        when l.user_gamecoins_num >= 500000 and l.user_gamecoins_num < 1000000 then '50万-100万'
                        when l.user_gamecoins_num >= 1000000 and l.user_gamecoins_num < 5000000 then '100万-500万'
                        when l.user_gamecoins_num >= 5000000 and l.user_gamecoins_num < 10000000 then '500万-1000万'
                        when l.user_gamecoins_num >= 10000000 and l.user_gamecoins_num < 50000000 then '1000万-5000万'
                        when l.user_gamecoins_num >= 50000000 and l.user_gamecoins_num < 100000000 then '5000万-1亿'
                        when l.user_gamecoins_num >= 100000000 and l.user_gamecoins_num < 1000000000 then '1亿-10亿'
                        else '10亿+' end fdis_list,
                   l.fuid,
                   count(1) fbankrupt_cnt
              from (select fbpid, fuid, user_gamecoins_num
                      from (select fbpid,
                                   fuid,
                                   user_gamecoins user_gamecoins_num,
                                   row_number() over(partition by fbpid, fuid order by flogin_at, user_gamecoins) rown
                              from dim.user_login_additional u
                             where u.dt = "%(statdate)s"
                           ) t
                     where rown = 1) l
              join (select fbpid, fuid, fgame_id, fpname, fchannel_code
                      from stage.user_bankrupt_stg
                     where dt = "%(statdate)s"
                   ) b
                on l.fbpid = b.fbpid
               and l.fuid = b.fuid
              join dim.bpid_map c
                on l.fbpid = c.fbpid
             group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode, fgame_id, fpname, fchannel_code,
                      case when l.user_gamecoins_num <= 0 then '0'
                           when l.user_gamecoins_num >= 1 and l.user_gamecoins_num < 5000 then '1-5000'
                           when l.user_gamecoins_num >= 5000 and l.user_gamecoins_num < 10000 then '5000-1万'
                           when l.user_gamecoins_num >= 10000 and l.user_gamecoins_num < 50000 then '1万-5万'
                           when l.user_gamecoins_num >= 50000 and l.user_gamecoins_num < 100000 then '5万-10万'
                           when l.user_gamecoins_num >= 100000 and l.user_gamecoins_num < 500000 then '10万-50万'
                           when l.user_gamecoins_num >= 500000 and l.user_gamecoins_num < 1000000 then '50万-100万'
                           when l.user_gamecoins_num >= 1000000 and l.user_gamecoins_num < 5000000 then '100万-500万'
                           when l.user_gamecoins_num >= 5000000 and l.user_gamecoins_num < 10000000 then '500万-1000万'
                           when l.user_gamecoins_num >= 10000000 and l.user_gamecoins_num < 50000000 then '1000万-5000万'
                           when l.user_gamecoins_num >= 50000000 and l.user_gamecoins_num < 100000000 then '5000万-1亿'
                           when l.user_gamecoins_num >= 100000000 and l.user_gamecoins_num < 1000000000 then '1亿-10亿'
                           else '10亿+' end,
                      l.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        extend_group = {'fields':['fpname', 'partytype'],
                        'groups':[[1, 0],
                                  [0, 1]]}

        hql = """--组合数据导入到正式表上--场次
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       1 fdis_type,
                       partytype fdis_list,
                       count(distinct case when fbankrupt_cnt > 0 then fuid end) fbankrupt_unum,
                       sum(fbankrupt_cnt) fbankrupt_cnt,
                       count(distinct fplatform_uid) fbank_30min_pay_unum,
                       sum(coalesce(round(fbank_30min_income, 2), 0)) fbank_30min_income,
                       count(distinct forder_id) fbank_30min_paycnt,
                       coalesce(fpname,%(null_int_group_rule)d) fpname
                  from work.user_brupt_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fpname,fterminaltypefsk,fversionfsk, fchannel_code,
                       partytype
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """--组合数据导入到正式表上
        insert overwrite table dcnew.user_bankrupt_info
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        extend_group = {'fields':['pouring_num'],
                        'groups':[[1]] }

        hql = """--组合数据导入到正式表上--底注
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       2 fdis_type,
                       pouring_num fdis_list,
                       count(distinct case when fbankrupt_cnt > 0 then fuid end) fbankrupt_unum,
                       sum(fbankrupt_cnt) fbankrupt_cnt,
                       count(distinct fplatform_uid) fbank_30min_pay_unum,
                       sum(coalesce(round(fbank_30min_income, 2), 0)) fbank_30min_income,
                       count(distinct forder_id) fbank_30min_paycnt,
                       coalesce(fpname,%(null_int_group_rule)d) fpname
                  from work.user_brupt_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fpname,fterminaltypefsk,fversionfsk, fchannel_code,
                       pouring_num
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """--组合数据导入到正式表上
        insert into table dcnew.user_bankrupt_info
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        extend_group = {'fields':['bankruptscene'],
                        'groups':[[1]] }

        hql = """--组合数据导入到正式表上--场景
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       3 fdis_type,
                       bankruptscene fdis_list,
                       count(distinct case when fbankrupt_cnt > 0 then fuid end) fbankrupt_unum,
                       sum(fbankrupt_cnt) fbankrupt_cnt,
                       count(distinct fplatform_uid) fbank_30min_pay_unum,
                       sum(coalesce(round(fbank_30min_income, 2), 0)) fbank_30min_income,
                       count(distinct forder_id) fbank_30min_paycnt,
                       coalesce(fpname,%(null_int_group_rule)d) fpname
                  from work.user_brupt_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fpname,fterminaltypefsk,fversionfsk, fchannel_code,
                       bankruptscene
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """--组合数据导入到正式表上
        insert into table dcnew.user_bankrupt_info
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        extend_group = {'fields':['fdis_list'],
                        'groups':[[1]] }


        hql = """--组合数据导入到正式表上--资产
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       4 fdis_type,
                       fdis_list,
                       count(distinct case when fbankrupt_cnt > 0 then fuid end) fbankrupt_unum,
                       sum(fbankrupt_cnt) fbankrupt_cnt,
                       0 fbank_30min_pay_unum,
                       0 fbank_30min_income,
                       0 fbank_30min_paycnt,
                       coalesce(fpname,%(null_int_group_rule)d) fpname
                  from work.user_brupt_info_tmp_c_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fpname,fterminaltypefsk,fversionfsk, fchannel_code,
                       fdis_list
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """--组合数据导入到正式表上
        insert into table dcnew.user_bankrupt_info
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        # 统计完清理掉临时表
        hql = """drop table if exists work.user_brupt_info_tmp_b_%(statdatenum)s;
                 drop table if exists work.user_brupt_info_tmp_c_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_bankrupt_info(sys.argv[1:])
a()