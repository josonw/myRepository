#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const



class agg_gameparty_detail_ante_dis(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_detail_ante_dis
        (
          fdate           date,
          fgamefsk           bigint,
          fplatformfsk       bigint,
          fhallfsk           bigint,
          fsubgamefsk        bigint,
          fterminaltypefsk   bigint,
          fversionfsk        bigint,
          fchannelcode       bigint,
          fpname          varchar(200),
          fparty_type     varchar(200),
          fsubname        varchar(200),
          fante           bigint,
          fusernum        bigint,
          fpartynum       bigint,
          fcharge         decimal(20,4),
          fplayusernum    bigint,
          fregplayusernum bigint,
          factplayusernum bigint,
          f2partynum      bigint,
          f3partynum      bigint,
          fcommon_usernum bigint,
          fmatch_usernum  bigint,
          falltime        bigint,
          fpartytime      bigint,
          fwin            bigint,
          flose           bigint,
          f2win              bigint
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_detail_ante_dis'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fante', 'fpname', 'fparty_type', 'fsubname'],
                        'groups':[[1, 1, 0, 0],
                                  [1, 0, 0, 0],
                                  [0, 1, 0, 0],
                                  [0, 0, 0, 0]]
                        }


        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;
                              """)
        if res != 0: return res

        hql = """
        select
            fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(fpname, '%(null_str_group_rule)s') fpname,
            coalesce(fparty_type, '%(null_str_group_rule)s') fparty_type,
            coalesce(fsubname, '%(null_str_group_rule)s') fsubname,
            coalesce(fante, '%(null_str_group_rule)s') fante,
            sum(fparty_num) fusernum,
            cast (0 as bigint) fpartynum,
            sum(fcharge) fcharge,
            count(distinct fuid) fplayusernum,
            cast (0 as bigint) fregplayusernum,
            cast (0 as bigint) factplayusernum,
            cast (0 as bigint) f2partynum,
            cast (0 as bigint) f3partynum,
            cast (0 as bigint) fcommon_usernum,
            cast (0 as bigint) fmatch_usernum,
            sum(fplay_time) falltime,
            cast (0 as bigint) fpartytime,
            sum(fwin_amt) fwin,
            sum(flose_amt) flose,
            cast (0 as bigint) f2win
        from dim.user_gameparty
        where hallmode = %(hallmode)s
          and dt = '%(statdate)s'
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname,
                fparty_type,
                fsubname,
                fante
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.gameparty_detail_ante_dis_temp_%(statdatenum)s;
        create table work.gameparty_detail_ante_dis_temp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        select
            fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(fpname, '%(null_str_group_rule)s') fpname,
            coalesce(fparty_type, '%(null_str_group_rule)s') fparty_type,
            coalesce(fsubname, '%(null_str_group_rule)s') fsubname,
            coalesce(fante, '%(null_str_group_rule)s') fante,
            0 fusernum,
            count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
            0 fcharge,
            0 fplayusernum,
            0 fregplayusernum,
            0 factplayusernum,
            count(distinct case when fpalyer_cnt=2 then concat_ws('0', ftbl_id, finning_id) else null end) f2partynum,
            0 f3partynum,
            0 fcommon_usernum,
            0 fmatch_usernum,
            0 falltime,
            sum(fparty_time) fpartytime,
            0 fwin,
            0 flose,
            sum(case when fpalyer_cnt = 2 then fwin_amt else 0 end)  f2win
        from dim.gameparty_stream
        where hallmode = %(hallmode)s
          and dt = '%(statdate)s'
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                fpname,
                fparty_type,
                fsubname,
                fante
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """-- 加入最大牌局时长 从user_gameparty_time中拉来的
            insert into table work.gameparty_detail_ante_dis_temp_%(statdatenum)s
            %(sql_template)s;
            """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据导入到表 dcnew.gameparty_detail_ante_dis
        insert overwrite table dcnew.gameparty_detail_ante_dis
        partition( dt="%(statdate)s" )
        select "%(statdate)s" fdate,
            fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
            fpname,
            fparty_type,
            fsubname,
            fante,
            sum(fusernum) fusernum,
            sum(fpartynum) fpartynum,
            sum(fcharge) fcharge,
            sum(fplayusernum) fplayusernum,
            sum(fregplayusernum) fregplayusernum,
            sum(factplayusernum) factplayusernum,
            sum(f2partynum) f2partynum,
            sum(f3partynum) f3partynum,
            sum(fcommon_usernum) fcommon_usernum,
            sum(fmatch_usernum) fmatch_usernum,
            sum(falltime) falltime,
            sum(fpartytime) fpartytime,
            sum(fwin) fwin,
            sum(flose) flose,
            sum(f2win) f2win
        from work.gameparty_detail_ante_dis_temp_%(statdatenum)s
        group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
            fpname,
            fparty_type,
            fsubname,
            fante
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.gameparty_detail_ante_dis_temp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_gameparty_detail_ante_dis(sys.argv[1:])
a()
