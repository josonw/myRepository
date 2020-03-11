#-*- coding: UTF-8 -*-
#-*- coding: UTF-8 -*-fparty_type

import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class agg_gameparty_detail_total(BaseStatModel):
    """用户牌局-牌局分析-牌局明细
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.gameparty_detail_total
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
          fusernum        bigint,           --人次
          fpartynum       bigint,           --牌局数
          fcharge         decimal(20,4),
          fplayusernum    bigint,           --人数
          fregplayusernum bigint,
          factplayusernum bigint,
          f2partynum      bigint,
          f3partynum      bigint,
          flose           bigint,
          fwin            bigint,
          falltime        bigint,
          fpartytime      bigint,
          ftrustee_num    bigint,
          fweedout_num    bigint,
          fbankrupt_num   bigint,           --破产人次
          fwin_cnt        bigint,
          flose_cnt       bigint,
          fentry_fee      bigint,
          fentry_cnt      bigint,
          fentry_num      bigint,
          fmatch_cnt      bigint,
          fquit_num       bigint,
          fmatch_usercnt  bigint,
          fbankrupt_persons bigint         --破产人数
        )
        partitioned by(dt date)
        location '/dw/dcnew/gameparty_detail_total'
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
            'fields':['fpname', 'fparty_type', 'fsubname'],
            'groups':[ [1, 0, 1],
                       [1, 0, 0],
                       [0, 0, 1],
                       [0, 0, 0]
                       ]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;
                              """)
        if res != 0: return res


        hql = """    -- 加入输赢总数
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
                sum(a.fparty_num) fusernum,
                0 fpartynum,
                sum(a.fcharge) fcharge,
                count(distinct fuid) fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                0 f2partynum,
                0 f3partynum,
                sum(a.flose_amt) flose,
                sum(a.fwin_amt) fwin,
                sum(a.fplay_time) falltime,
                0 fpartytime,
                count(distinct case when ftrustee_num > 0 then fuid else null end) ftrustee_num,
                count(distinct case when fis_weedout=0 then null else fuid end) fweedout_num,
                count(distinct case when coalesce(fis_bankrupt,0)=0 then null else fuid end) as fbankrupt_persons,
                sum(case when coalesce(fis_bankrupt,0)=0 then 0 else 1 end) fbankrupt_num,
                sum(a.fwin_party_num) fwin_cnt,
                sum(a.flose_party_num) flose_cnt,
                0 fentry_fee,
                0 fentry_cnt,
                0 fentry_num,
                0 fmatch_cnt,
                count(distinct case when fis_end=0 then null else fuid end) fquit_num,
                0 fmatch_usercnt
            from dim.user_gameparty a
            where dt="%(statdate)s"
              and hallmode=%(hallmode)s
            group by
                    fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fterminaltypefsk,
                    fversionfsk,
                    fgame_id,
                    fchannel_code,
                    fpname,
                    fsubname,
                    fparty_type
            """


        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        drop table if exists work.gameparty_detail_total_temp_%(statdatenum)s;
        create table work.gameparty_detail_total_temp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

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
                0 fusernum,
                0 fpartynum,
                0 fcharge,
                0 fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                0 f2partynum,
                0 f3partynum,
                0 flose,
                0 fwin,
                0 falltime,
                0 fpartytime,
                0 ftrustee_num,
                0 fweedout_num,
                0 fbankrupt_persons,
                0 fbankrupt_num,
                0 fwin_cnt,
                0 flose_cnt,
                0 fentry_fee,
                0 fentry_cnt,
                0 fentry_num,
                count(distinct fmatch_id ) fmatch_cnt,
                0 fquit_num,
                count(distinct concat(fuid,fmatch_id) ) fmatch_usercnt
            from dim.user_match_party_stream a
            join dim.bpid_map b
              on a.fbpid = b.fbpid
            where a.dt="%(statdate)s"
            and a.fpalyer_cnt!=0
            and b.hallmode=%(hallmode)s
            group by
                    fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fterminaltypefsk,
                    fversionfsk,
                    fgame_id,
                    fchannel_code,
                    fpname,
                    fsubname,
                    fparty_type
            """

        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        insert into table work.gameparty_detail_total_temp_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """    --
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
                0 fusernum,
                count(distinct concat_ws('0', ftbl_id, finning_id) ) fpartynum,
                0 fcharge,
                0 fplayusernum,
                0 fregplayusernum,
                0 factplayusernum,
                count(distinct case when fpalyer_cnt = 2 then concat_ws('0', ftbl_id, finning_id) end) f2partynum,
                0 f3partynum,
                0 flose,
                0 fwin,
                0 falltime,
                sum(fparty_time) fpartytime,
                0 ftrustee_num,
                0 fweedout_num,
                0 fbankrupt_persons,
                0 fbankrupt_num,
                0 fwin_cnt,
                0 flose_cnt,
                0 fentry_fee,
                0 fentry_cnt,
                0 fentry_num,
                0 fmatch_cnt,
                0 fquit_num,
                0 fmatch_usercnt
            from  dim.gameparty_stream a
            where dt="%(statdate)s" and hallmode=%(hallmode)s
            group by
                    fgamefsk,
                    fplatformfsk,
                    fhallfsk,
                    fterminaltypefsk,
                    fversionfsk,
                    fgame_id,
                    fchannel_code,
                    fpname,
                    fsubname,
                    fparty_type
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql = """
        insert into table work.gameparty_detail_total_temp_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """    -- 组合数据导入到表 dcnew.gameparty_detail_total
        insert overwrite table dcnew.gameparty_detail_total
        partition( dt="%(statdate)s" )
            select "%(statdate)s" fdate, fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code,
                fpname, fparty_type, fsubname,
                max(fusernum) fusernum,
                max(fpartynum) fpartynum,
                max(fcharge) fcharge,
                max(fplayusernum) fplayusernum,
                max(fregplayusernum) fregplayusernum,
                max(factplayusernum) factplayusernum,
                max(f2partynum) f2partynum,
                max(f3partynum) f3partynum,
                max(flose) flose,
                max(fwin) fwin,
                max(falltime) falltime,
                max(fpartytime) fpartytime,
                max(ftrustee_num) ftrustee_num,
                max(fweedout_num) fweedout_num,
                max(fbankrupt_num) fbankrupt_num,
                max(fwin_cnt) fwin_cnt,
                max(flose_cnt) flose_cnt,
                max(fentry_fee) fentry_fee,
                max(fentry_cnt) fentry_cnt,
                max(fentry_num) fentry_num,
                max(fmatch_cnt) fmatch_cnt,
                max(fquit_num) fquit_num,
                max(fmatch_usercnt) fmatch_usercnt,
                max(fbankrupt_persons) fbankrupt_persons
            from work.gameparty_detail_total_temp_%(statdatenum)s
            group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fchannel_code, fpname, fparty_type, fsubname
        """

        res = self.sql_exe(hql)
        if res != 0:return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists work.gameparty_detail_total_temp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_gameparty_detail_total(sys.argv[1:])
a()
