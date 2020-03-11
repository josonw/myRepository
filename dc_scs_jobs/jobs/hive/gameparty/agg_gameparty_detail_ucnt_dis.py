#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/service' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import sql_const



class agg_gameparty_detail_ucnt_dis(BaseStatModel):
    """牌局分析-牌局明细-单局人数分布
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_detail_ucnt_dis
        (
          fdate           date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fsubgamefsk        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannelcode       bigint,
          fplayer_cnt     bigint,
          fpname          varchar(200),
          fsubname        varchar(200),
          fusernum        bigint,
          fpartynum       bigint,
          fcharge         decimal(30,2),
          fplayusernum    bigint,
          fregplayusernum bigint,
          factplayusernum bigint,
          falltime        bigint,
          fpartytime      bigint,
          flose           bigint,
          fwin            bigint,
          fparty_type     varchar(200),
          fante           bigint
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_detail_ucnt_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0

        extend_group = {
            'fields':['fpname','fpalyer_cnt','fsubname','fante'],
            'groups':[ [0, 0, 0, 0],
                       [1, 0, 0, 0],
                       [1, 0, 1, 0],
                       [0, 0, 1, 0],
                       [1, 1, 0, 0],
                       [0, 1, 0, 0],
                       [1, 0, 0, 1],
                       [0, 0, 0, 1],
                       ]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;
                              """)
        if res != 0: return res


        hql = """
        drop table if exists work.gp_detail_dis_tmp_b_%(statdatenum)s;
        create table work.gp_detail_dis_tmp_b_%(statdatenum)s
        as
        select c.fgamefsk,
               c.fplatformfsk,
               c.fhallfsk,
               c.fterminaltypefsk,
               c.fversionfsk,
               c.hallmode,
               coalesce(fgame_id,cast (0 as bigint)) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               fuid,
               coalesce(fpname,'%(null_str_report)s') fpname,
               coalesce(fsubname,'%(null_str_report)s') fsubname,
               fpalyer_cnt,
               concat_ws('0', ug.ftbl_id, ug.finning_id) fparty_num,
               case when ug.fs_timer = '1970-01-01 00:00:00' then 0
                    when ug.fe_timer = '1970-01-01 00:00:00' then 0
                    else unix_timestamp(ug.fe_timer)-unix_timestamp(ug.fs_timer)
                    end fplay_time,
               fcharge,
               fgamecoins,
               0 fparty_time,
               coalesce(fparty_type, '%(null_str_report)s')  fparty_type,
               fblind_1 fante
        from stage.user_gameparty_stg ug
        join dim.bpid_map c
          on ug.fbpid=c.fbpid
       where dt = '%(statdate)s'
         and fpalyer_cnt != 0
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """ -- 牌局时长，取桌子最长牌局时间再汇总
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fpalyer_cnt,%(null_int_group_rule)d) fpalyer_cnt,
                       coalesce(fpname,'%(null_str_group_rule)s') fpname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       count(a.fuid) fusernum,
                       count(distinct a.fparty_num) fpartynum,
                       sum(a.fcharge) fcharge,
                       count(distinct a.fuid) fplayusernum,
                       0 fregplayusernum,
                       0 factplayusernum,
                       sum(fplay_time) falltime,
                       0 fpartytime,
                       sum( case when a.fgamecoins < 0 then abs(a.fgamecoins) else 0 end) flose,
                       sum( case when a.fgamecoins > 0 then a.fgamecoins else 0 end) fwin,
                       coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type,
                       coalesce(fante,%(null_int_group_rule)d) fante
                   from work.gp_detail_dis_tmp_b_%(statdatenum)s a
                   where a.hallmode=%(hallmode)s
                   group by fgamefsk,
                            fplatformfsk,
                            fhallfsk,
                            fgame_id,
                            fterminaltypefsk,
                            fversionfsk,
                            fchannel_code,
                            fpalyer_cnt,
                            fpname,
                            fsubname,
                            fparty_type,
                            fante
            """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.gp_detail_dis_tmp_1_%(statdatenum)s;
        create table work.gp_detail_dis_tmp_1_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        drop table if exists work.gp_detail_dis_tmp_b_2_%(statdatenum)s;
        create table work.gp_detail_dis_tmp_b_2_%(statdatenum)s
        as
        select c.fgamefsk,
               c.fplatformfsk,
               c.fhallfsk,
               c.fterminaltypefsk,
               c.fversionfsk,
               c.hallmode,
               coalesce(fgame_id,cast (0 as bigint)) fgame_id,
               coalesce(cast (fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               coalesce(fpname,'%(null_str_report)s') fpname,
               coalesce(fsubname,'%(null_str_report)s') fsubname,
               fpalyer_cnt,
               sum(ts) fplay_time,
               coalesce(fparty_type, '%(null_str_report)s')  fparty_type,
               fante
        from (select fbpid, ftbl_id, finning_id, fpname, fsubname, fpalyer_cnt,fgame_id,fchannel_code,fparty_type,fblind_1 fante,
                    max(case when fs_timer = '1970-01-01 00:00:00' then 0
                        when fe_timer = '1970-01-01 00:00:00' then 0
                        else unix_timestamp(fe_timer)-unix_timestamp(fs_timer)
                        end
                    ) ts
                from stage.user_gameparty_stg
                where fpalyer_cnt!=0
                  and dt = "%(statdate)s"
                group by fbpid, ftbl_id, finning_id, fpname, fsubname, fpalyer_cnt,fgame_id,fchannel_code,fparty_type,fblind_1
             ) ug
        join dim.bpid_map c
          on ug.fbpid=c.fbpid
       group by c.fgamefsk,
                c.fplatformfsk,
                c.fhallfsk,
                c.fterminaltypefsk,
                c.fversionfsk,
                c.hallmode,
                fgame_id,
                fchannel_code,
                fpalyer_cnt,
                fpname,
                fsubname,
                fparty_type,
                fante
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fpalyer_cnt,%(null_int_group_rule)d) fpalyer_cnt,
                       coalesce(fpname,'%(null_str_group_rule)s') fpname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       0 fusernum,
                       0 fpartynum,
                       0 fcharge,
                       0 fplayusernum,
                       0 fregplayusernum,
                       0 factplayusernum,
                       0 falltime,
                       sum(fplay_time) fpartytime,
                       0 flose,
                       0 fwin,
                       coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type,
                       coalesce(fante,%(null_int_group_rule)d) fante
                   from work.gp_detail_dis_tmp_b_2_%(statdatenum)s a
                   where a.hallmode=%(hallmode)s
                   group by fgamefsk,
                            fplatformfsk,
                            fhallfsk,
                            fgame_id,
                            fterminaltypefsk,
                            fversionfsk,
                            fchannel_code,
                            fpalyer_cnt,
                            fpname,
                            fsubname,
                            fparty_type,
                            fante
            """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """insert into table work.gp_detail_dis_tmp_1_%(statdatenum)s
            %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """    -- 把临时表数据处理后弄到正式表上
        insert overwrite table dcnew.gameparty_detail_ucnt_dis
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                fpalyer_cnt,
                fpname,
                fsubname,
                max(fusernum) fusernum,
                max(fpartynum) fpartynum,
                max(fcharge) fcharge,
                max(fplayusernum) fplayusernum,
                max(fregplayusernum) fregplayusernum,
                max(factplayusernum) factplayusernum,
                max(falltime) falltime,
                max(fpartytime) fpartytime,
                max(flose) flose,
                max(fwin) fwin,
                fparty_type,
                fante
            from work.gp_detail_dis_tmp_1_%(statdatenum)s
            group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                     fpalyer_cnt, fpname, fsubname,fparty_type,fante
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists work.gp_detail_dis_tmp_1_%(statdatenum)s;
        drop table if exists work.gp_detail_dis_tmp_b_%(statdatenum)s;
        drop table if exists work.gp_detail_dis_tmp_b_2_%(statdatenum)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


#生成统计实例
a = agg_gameparty_detail_ucnt_dis(sys.argv[1:])
a()
