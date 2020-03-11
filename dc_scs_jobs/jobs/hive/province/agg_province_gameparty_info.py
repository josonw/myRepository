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


class agg_province_gameparty_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_牌局场次相关
        create table if not exists dcnew.province_gameparty_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fversionfsk         bigint,
                fchannelcode        bigint,
                fprovince           varchar(64)     comment '省份',
                fcoins_type         int             comment '结算货币类型',
                fplay_unum          bigint          comment '玩牌人数',
                fplay_cnt           bigint          comment '玩牌人次',
                fnew_play_unum      bigint          comment '新增用户数',
                fpay_unum           bigint          comment '付费用户数',
                fcharge             bigint          comment '台费',
                fparty_num          bigint          comment '牌局数',
                fplay_time          bigint          comment '玩牌时长',
                favg_party_num      bigint          comment '人均牌局数',
                favg_play_time      bigint          comment '人均玩牌时长',
                frupt_unum          bigint          comment '破产人数',
                frupt_num           bigint          comment '破产牌局数',
                frupt_pay_unum      bigint          comment '破产付费人数',
                frupt_pay_cnt       bigint          comment '破产付费次数',
                frupt_pay_income    decimal(20,2)   comment '破产付费额度'
                )comment '分省数据_牌局场次相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_gameparty_info'
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id,fcoins_type),
                               (fgamefsk, fplatformfsk, fgame_id,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fplatformfsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk,fcoins_type),
                               (fgamefsk, fplatformfsk, fterminaltypefsk,fcoins_type),
                               (fgamefsk, fplatformfsk,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk, fterminaltypefsk),
                               (fgamefsk, fplatformfsk)  )
                union all """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fgame_id, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fprovince),
                               (fgamefsk, fplatformfsk, fgame_id, fprovince) )
                union all """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fterminaltypefsk, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fprovince,fcoins_type),
                               (fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fprovince),
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

        hql = """--玩牌人数、玩牌人次、新增用户数
        drop table if exists work.province_gameparty_tmp_1_%(statdatenum)s;
        create table work.province_gameparty_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                 a.fcoins_type,  --结算货币类型
                 a.fuid,  --玩牌人数
                 a.play_cnt, --玩牌次数
                 case when b.fuid is not null then 1 else 0 end is_new,  --是否新增
                 case when d.fuid is not null then 1 else 0 end is_pay  --是否付费
            from (select fbpid, fuid, fcoins_type, fgame_id, count(1) play_cnt
                    from stage.user_gameparty_stg
                   where dt = "%(statdate)s"
                   group by fbpid, fuid, fcoins_type, fgame_id
                 ) a
            left join dim.reg_user_main_additional b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt = "%(statdate)s"
            left join dim.user_pay_day d
              on a.fbpid = d.fbpid
             and a.fuid = d.fuid
             and d.dt='%(statdate)s'
            join dim.bpid_map c
              on a.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
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
                       coalesce(fcoins_type,'%(null_str_group_rule)s') fcoins_type,
                       count(distinct fuid) fplay_unum,
                       sum(play_cnt) fplay_cnt,
                       count(distinct case when is_new = 1 then fuid end) fnew_play_unum,
                       count(distinct case when is_pay = 1 then fuid end) fpay_unum,
                       cast (0 as bigint) fcharge,
                       cast (0 as bigint) fparty_num,
                       cast (0 as bigint) fplay_time,
                       cast (0 as bigint) favg_party_num,
                       cast (0 as bigint) favg_play_time,
                       cast (0 as bigint) frupt_unum,
                       cast (0 as bigint) frupt_num,
                       cast (0 as bigint) frupt_pay_unum,
                       cast (0 as bigint) frupt_pay_cnt,
                       cast (0 as bigint) frupt_pay_income
                  from work.province_gameparty_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fcoins_type,
                       fprovince """

        #组合
        hql = ("""
                  drop table if exists work.province_gameparty_tmp_%(statdatenum)s;
                create table work.province_gameparty_tmp_%(statdatenum)s as """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--破产牌局数、总牌局数、总玩牌时长
        drop table if exists work.province_gameparty_tmp_2_%(statdatenum)s;
        create table work.province_gameparty_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                 fcoins_type,
                 ftbl_id, --桌子编号
                 finning_id, --牌局编号
                 sum(fcharge) fcharge, --台费
                 sum(case when a.fs_timer = '1970-01-01 00:00:00' then 0
                          when a.fe_timer = '1970-01-01 00:00:00' then 0
                         else unix_timestamp(a.fe_timer)-unix_timestamp(a.fs_timer)
                     end) fplaytime, --玩牌时长
                 max(fpalyer_cnt) fplayer_cnt, --该场牌局参与人数
                 sum( case when a.fgamecoins > 0 then a.fgamecoins else 0 end ) fwin_player_cnt, --赢牌人数
                 sum( case when a.fgamecoins < 0 then a.fgamecoins else 0 end ) flose_player_cnt, --输牌人数
                 max(fis_bankrupt) fis_bankrupt --是否破产
            from dim.user_gameparty_stream a
            join dim.bpid_map c
              on a.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where a.dt = "%(statdate)s"
           group by c.fgamefsk,
                    c.fplatformfsk,
                    c.fhallfsk,
                    c.fterminaltypefsk,
                    c.fplatformname,
                    a.fgame_id,
                    fcoins_type,
                    ftbl_id, --桌子编号
                    finning_id --牌局编号
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
                       coalesce(fcoins_type,'%(null_str_group_rule)s') fcoins_type,
                       0 fplay_unum,
                       0 fplay_cnt,
                       0 fnew_play_unum,
                       0 fpay_unum,
                       sum(fcharge) fcharge,
                       count(1) fparty_num,
                       sum(fplaytime) fplay_time,
                       0 favg_party_num,
                       0 favg_play_time,
                       0 frupt_unum,
                       count(case when fis_bankrupt > 0 then 1 else null end) frupt_num,
                       0 frupt_pay_unum,
                       0 frupt_pay_cnt,
                       0 frupt_pay_income
                  from work.province_gameparty_tmp_2_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fcoins_type,
                       fprovince """

        #组合
        hql = ("""
                 insert into table work.province_gameparty_tmp_%(statdatenum)s """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--破产牌局数、总牌局数、总玩牌时长
        drop table if exists work.province_gameparty_tmp_3_%(statdatenum)s;
        create table work.province_gameparty_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ distinct c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                 nvl(t.fcoins_type,0) fcoins_type,
                 a.fuid,
                 d.fplatform_uid,
                 d.fcoins_num * d.frate fbank_30min_income,
                 forder_id,
                 case when unix_timestamp(d.fdate)-1800 <= unix_timestamp(a.frupt_at) then 1 else 0 end is_30m
            from stage.user_bankrupt_stg a
            left join stage.user_gameparty_stg t
              on a.fbpid = t.fbpid
             and a.fuid = t.fuid
             and a.fuphill_pouring = t.fblind_1
             and a.fpname = t.fpname
             and a.fplayground_title  = t.fsubname
             and t.dt='%(statdate)s'
             and t.fis_bankrupt = 1
            left join dim.user_pay_day b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt='%(statdate)s'
            left join stage.payment_stream_stg d
              on b.fplatform_uid = d.fplatform_uid
             and b.fbpid = d.fbpid
             and d.dt='%(statdate)s'
            join dim.bpid_map c
              on a.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where a.dt='%(statdate)s'
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
                       coalesce(fcoins_type,'%(null_str_group_rule)s') fcoins_type,
                       0 fplay_unum,
                       0 fplay_cnt,
                       0 fnew_play_unum,
                       0 fpay_unum,
                       0 fcharge,
                       0 fparty_num,
                       0 fplay_time,
                       0 favg_party_num,
                       0 favg_play_time,
                       count(distinct fuid) frupt_unum,
                       0 frupt_num,
                       count(distinct case when is_30m = 1 then fplatform_uid end) frupt_pay_unum,
                       count(distinct case when is_30m = 1 then forder_id end) frupt_pay_cnt,
                       sum(case when is_30m = 1 then fbank_30min_income end) frupt_pay_income
                  from work.province_gameparty_tmp_3_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fcoins_type,
                       fprovince """

        #组合
        hql = ("""
                  insert into table work.province_gameparty_tmp_%(statdatenum)s  """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """insert overwrite table dcnew.province_gameparty_info
                    partition(dt='%(statdate)s')
                  select '%(statdate)s' fdate,
                         fgamefsk,
                         fplatformfsk,
                         fhallfsk,
                         fgame_id,
                         fterminaltypefsk,
                         -1 fversionfsk,
                         -1 fchannel_code,
                         fprovince,
                         fcoins_type,
                         nvl(sum(fplay_unum),0) fplay_unum,
                         nvl(sum(fplay_cnt),0) fplay_cnt,
                         nvl(sum(fnew_play_unum),0) fnew_play_unum,
                         nvl(sum(fpay_unum),0) fpay_unum,
                         nvl(sum(fcharge),0) fcharge,
                         nvl(sum(fparty_num),0) fparty_num,
                         nvl(sum(fplay_time),0) fplay_time,
                         case when sum(fplay_unum) = 0 then 0 else sum(fplay_cnt)*1.0/sum(fplay_unum) end favg_party_num,
                         case when sum(fplay_unum) = 0 then 0 else sum(fplay_time)*1.0/sum(fplay_unum) end favg_play_time,
                         nvl(sum(frupt_unum),0) frupt_unum,
                         nvl(sum(frupt_num),0) frupt_num,
                         nvl(sum(frupt_pay_unum),0) frupt_pay_unum,
                         nvl(sum(frupt_pay_cnt),0) frupt_pay_cnt,
                         nvl(sum(frupt_pay_income),0) frupt_pay_income
                    from work.province_gameparty_tmp_%(statdatenum)s gs
                   group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fcoins_type, fprovince

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.province_gameparty_tmp_1_%(statdatenum)s;
                 drop table if exists work.province_gameparty_tmp_2_%(statdatenum)s;
                 drop table if exists work.province_gameparty_tmp_3_%(statdatenum)s;
                 drop table if exists work.province_gameparty_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res



#生成统计实例
a = agg_province_gameparty_info(sys.argv[1:])
a()
