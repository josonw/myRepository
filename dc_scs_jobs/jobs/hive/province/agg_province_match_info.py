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


class agg_province_match_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_赛事相关
        create table if not exists dcnew.province_match_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fversionfsk         bigint,
                fchannelcode        bigint,
                fprovince           varchar(64)     comment '省份',
                fapply_unum         bigint          comment '报名人数',
                fapply_cnt          bigint          comment '报名人次',
                fnew_apply_unum     bigint          comment '新增报名人数',
                fhnew_apply_unum    bigint          comment '大厅新用户报名人数',
                fmatch_unum         bigint          comment '参赛人数',
                fmatch_cnt          bigint          comment '参赛人次',
                fnew_match_unum     bigint          comment '新增参赛人数',
                fhnew_match_unum    bigint          comment '大厅新用户参赛人数',
                fwin_unum           bigint          comment '获奖人数',
                fsysout_unum        bigint          comment '系统退赛人数',
                fbegin_cnt          bigint          comment '开赛次数',
                fbegin_fail_cnt     bigint          comment '开赛失败次数',
                fmatch_back_unum    bigint          comment '比赛用户次日留存',
                fhmatch_back_unum   bigint          comment '比赛用户大厅次日留存',
                faward_out          bigint          comment '比赛奖励发放',
                fcost_in            bigint          comment '比赛回收',
                fmatch_pay_unum     bigint          comment '比赛付费人数',
                fmatch_pay_cnt      bigint          comment '比赛付费次数',
                fmatch_pay_income   decimal(20,2)   comment '比赛付费额度'
                )comment '分省数据_赛事相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_match_info'
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

        hql = """
        drop table if exists work.province_match_tmp_1_%(statdatenum)s;
        create table work.province_match_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(a.fgame_id,%(null_int_report)d) fgame_id,
                 fuid,
                 fmatch_id,
                 fcause,    --退赛原因
                 fio_type,  --发放消耗
                 flag,      --数据类型：1报名2参赛3退赛4发放消耗
                 fapplycnt, --报名
                 fpartycnt, --参赛
                 first_apply, --首次报名
                 new_apply,   --大厅新用户报名
                 first_party, --首次参赛
                 new_party,   --大厅新用户参赛
                 fitem_num,    --发放消耗数额
                 sum(case when fcause >0 then 0 else 1 end) over(partition by fmatch_id) quie_id  --退赛标志，0为退赛，1为为退赛，如果一个match_id全为0则开赛失败
            from (--报名参加牌局比赛
                  select jg.fbpid,
                         coalesce(jg.fgame_id,cast (0 as bigint)) fgame_id,
                         jg.fuid,
                         jg.fmatch_id,
                         0 fcause,
                         0 fio_type,
                         1 flag,
                         1 fapplycnt, --报名
                         0 fpartycnt,
                         ffirst_match first_apply, --首次报名
                         case when ru.fuid is not null then 1 else 0 end new_apply, --大厅新用户报名
                         0 first_party,
                         0 new_party,
                         0 fitem_num
                    from stage.join_gameparty_stg jg
                    left join dim.reg_user_main_additional ru
                      on jg.fbpid = ru.fbpid
                     and jg.fuid = ru.fuid
                     and ru.dt = '%(statdate)s'
                   where jg.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                   union all
                --参赛
                  select ug.fbpid,
                         coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                         ug.fuid,
                         ug.fmatch_id,
                         0 fcause,
                         0 fio_type,
                         2 flag,
                         0 fapplycnt,
                         1 fpartycnt,  --参赛
                         0 first_apply,
                         0 new_apply,
                         ffirst_match first_party,  --首次参赛
                         case when ru.fuid is not null then 1 else 0 end new_party,  --大厅新用户参赛
                         0 fitem_num
                    from stage.user_gameparty_stg ug
                    left join dim.reg_user_main_additional ru
                      on ug.fbpid = ru.fbpid
                     and ug.fuid = ru.fuid
                     and ru.dt = '%(statdate)s'
                   where ug.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                   union all
                --退赛
                  select fbpid,
                         coalesce(qg.fgame_id,cast (0 as bigint)) fgame_id,
                         fuid,
                         qg.fmatch_id,
                         fcause,
                         0 fio_type,
                         3 flag,
                         0 fapplycnt,
                         0 fpartycnt,
                         0 first_apply,
                         0 new_apply,
                         0 first_party,
                         0 new_party,
                         0 fitem_num
                    from stage.quit_gameparty_stg qg
                   where qg.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                   union all
                --比赛场发放消耗
                  select fbpid,
                         coalesce(mg.fgame_id,cast (0 as bigint)) fgame_id,
                         fuid,
                         mg.fmatch_id,
                         0 fcause,
                         fio_type,
                         4 flag,
                         0 fapplycnt,
                         0 fpartycnt,
                         0 first_apply,
                         0 new_apply,
                         0 first_party,
                         0 new_party,
                         fitem_num
                    from stage.match_economy_stg mg
                   where mg.dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                 ) a
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
                       count(distinct case when fapplycnt = 1 then fuid end) fapply_unum,     --报名人数
                       count(case when fapplycnt = 1 then fuid end) fapply_cnt,     --报名人次
                       count(distinct case when fapplycnt = 1 and first_apply = 1 then fuid end) fnew_apply_unum,     --新增报名人数
                       count(distinct case when fapplycnt = 1 and new_apply = 1 then fuid end) fhnew_apply_unum,     --大厅新用户报名人数
                       count(distinct case when fpartycnt = 1 then fuid end) fmatch_unum,     --参赛人数
                       count(distinct case when fpartycnt = 1 then concat(cast (fuid as string),cast (fmatch_id as string)) end) fmatch_cnt,     --参赛人次
                       count(distinct case when fpartycnt = 1 and first_party = 1 then fuid end) fnew_match_unum,     --新增参赛人数
                       count(distinct case when fpartycnt = 1 and new_party = 1 then fuid end) fhnew_match_unum,     --大厅新用户参赛人数
                       count(distinct case when fio_type = 1 then fuid end) fwin_unum,     --获奖人数
                       count(distinct case when fcause = 2 then fuid end) fsysout_unum,     --系统退赛人数
                       count(distinct fmatch_id) fbegin_cnt,     --开赛次数
                       count(distinct case when quie_id = 0 then fmatch_id end) fbegin_fail_cnt,     --开赛失败次数
                       0 fmatch_back_unum,     --比赛用户次日留存
                       0 fhmatch_back_unum,     --比赛用户大厅次日留存
                       sum(case when fio_type = 1 then fitem_num end) faward_out,     --比赛奖励发放
                       sum(case when fio_type = 2 then fitem_num end) fcost_in,     --比赛回收
                       0 fmatch_pay_unum,     --比赛付费人数
                       0 fmatch_pay_cnt,     --比赛付费次数
                       cast (0 as decimal(20,2)) fmatch_pay_income     --比赛付费额度
                  from work.province_match_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        #组合
        hql = ("""
                  drop table if exists work.province_match_tmp_%(statdatenum)s;
                create table work.province_match_tmp_%(statdatenum)s as """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        drop table if exists work.province_match_tmp_2_%(statdatenum)s;
        create table work.province_match_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(yug.fgame_id,cast (0 as bigint)) fgame_id,
                 yug.fuid,
                 case when ug.fuid is not null then 1 else 0 end is_mat_ret,
                 case when ua.fuid is not null then 1 else 0 end is_acy_ret
            from (select distinct fbpid, fuid, fgame_id
                    from stage.user_gameparty_stg
                   where dt='%(ld_1day_ago)s'
                     and coalesce(fmatch_id,'0')<>'0'
                 ) yug
            left join (select distinct fbpid, fuid, fgame_id
                         from stage.user_gameparty_stg
                        where dt='%(statdate)s'
                          and coalesce(fmatch_id,'0')<>'0'
                      ) ug
              on yug.fbpid = ug.fbpid
             and yug.fuid = ug.fuid
            left join dim.user_act ua
              on yug.fbpid = ua.fbpid
             and yug.fuid = ua.fuid
             and ua.dt='%(statdate)s'
            join dim.bpid_map c
              on yug.fbpid=c.fbpid
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
                       0 fapply_unum,     --报名人数
                       0 fapply_cnt,     --报名人次
                       0 fnew_apply_unum,     --新增报名人数
                       0 fhnew_apply_unum,     --大厅新用户报名人数
                       0 fmatch_unum,     --参赛人数
                       0 fmatch_cnt,     --参赛人次
                       0 fnew_match_unum,     --新增参赛人数
                       0 fhnew_match_unum,     --大厅新用户参赛人数
                       0 fwin_unum,     --获奖人数
                       0 fsysout_unum,     --系统退赛人数
                       0 fbegin_cnt,     --开赛次数
                       0 fbegin_fail_cnt,     --开赛失败次数
                       count(distinct case when is_mat_ret = 1 then fuid end) fmatch_back_unum,     --比赛用户次日留存
                       count(distinct case when is_acy_ret = 1 then fuid end)  fhmatch_back_unum,     --比赛用户大厅次日留存
                       0 faward_out,     --比赛奖励发放
                       0 fcost_in,     --比赛回收
                       0 fmatch_pay_unum,     --比赛付费人数
                       0 fmatch_pay_cnt,     --比赛付费次数
                       0 fmatch_pay_income     --比赛付费额度
                  from work.province_match_tmp_2_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        #组合
        hql = ("""
                  insert into table work.province_match_tmp_%(statdatenum)s """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        drop table if exists work.province_match_tmp_3_%(statdatenum)s;
        create table work.province_match_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(c) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(ps.fgame_id,cast (0 as bigint)) fgame_id,
                 ug.fuid,
                 ps.fplatform_uid,
                 ps.fpaycnt,
                 ps.dip
            from (select a.fbpid,
                         a.fplatform_uid,
                         e.fuid,
                         t2.fgame_id,
                         sum(round(fcoins_num * frate, 2)) dip,
                         count(1) fpaycnt
                    from stage.payment_stream_stg a
                    left join stage.user_generate_order_stg t2
                      on a.forder_id = t2.forder_id
                     and t2.dt = '%(statdate)s'
                    left join dim.user_pay e
                      on e.fbpid = a.fbpid
                     and e.fplatform_uid = a.fplatform_uid
                   where a.dt = "%(statdate)s"
                   group by a.fbpid, a.fplatform_uid,t2.fgame_id,e.fuid
                 ) ps
            join (select distinct fbpid, fuid
                    from stage.user_gameparty_stg
                   where dt='%(statdate)s'
                     and coalesce(fmatch_id,'0')<>'0'
                 ) ug
              on ug.fbpid = ps.fbpid
             and ug.fuid = ps.fuid
            join dim.bpid_map c
              on ps.fbpid=c.fbpid
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
                       0 fapply_unum,     --报名人数
                       0 fapply_cnt,     --报名人次
                       0 fnew_apply_unum,     --新增报名人数
                       0 fhnew_apply_unum,     --大厅新用户报名人数
                       0 fmatch_unum,     --参赛人数
                       0 fmatch_cnt,     --参赛人次
                       0 fnew_match_unum,     --新增参赛人数
                       0 fhnew_match_unum,     --大厅新用户参赛人数
                       0 fwin_unum,     --获奖人数
                       0 fsysout_unum,     --系统退赛人数
                       0 fbegin_cnt,     --开赛次数
                       0 fbegin_fail_cnt,     --开赛失败次数
                       0 fmatch_back_unum,     --比赛用户次日留存
                       0 fhmatch_back_unum,     --比赛用户大厅次日留存
                       0 faward_out,     --比赛奖励发放
                       0 fcost_in,     --比赛回收
                       count(distinct fplatform_uid) fmatch_pay_unum,     --比赛付费人数
                       sum(fpaycnt) fmatch_pay_cnt,     --比赛付费次数
                       sum(dip) fmatch_pay_income     --比赛付费额度
                  from work.province_match_tmp_3_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        #组合
        hql = ("""
                  insert into table work.province_match_tmp_%(statdatenum)s """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """insert overwrite table dcnew.province_match_info
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
                         nvl(sum(fapply_unum),0) fapply_unum,
                         nvl(sum(fapply_cnt),0) fapply_cnt,
                         nvl(sum(fnew_apply_unum),0) fnew_apply_unum,
                         nvl(sum(fhnew_apply_unum),0) fhnew_apply_unum,
                         nvl(sum(fmatch_unum),0) fmatch_unum,
                         nvl(sum(fmatch_cnt),0) fmatch_cnt,
                         nvl(sum(fnew_match_unum),0) fnew_match_unum,
                         nvl(sum(fhnew_match_unum),0) fhnew_match_unum,
                         nvl(sum(fwin_unum),0) fwin_unum,
                         nvl(sum(fsysout_unum),0) fsysout_unum,
                         nvl(sum(fbegin_cnt),0) fbegin_cnt,
                         nvl(sum(fbegin_fail_cnt),0) fbegin_fail_cnt,
                         nvl(sum(fmatch_back_unum),0) fmatch_back_unum,
                         nvl(sum(fhmatch_back_unum),0) fhmatch_back_unum,
                         nvl(sum(faward_out),0) faward_out,
                         nvl(sum(fcost_in),0) fcost_in,
                         nvl(sum(fmatch_pay_unum),0) fmatch_pay_unum,
                         nvl(sum(fmatch_pay_cnt),0) fmatch_pay_cnt,
                         nvl(sum(fmatch_pay_income),0) fmatch_pay_income
                    from work.province_match_tmp_%(statdatenum)s gs
                   group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.province_match_tmp_1_%(statdatenum)s;
                 drop table if exists work.province_match_tmp_2_%(statdatenum)s;
                 drop table if exists work.province_match_tmp_3_%(statdatenum)s;
                 drop table if exists work.province_match_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_province_match_info(sys.argv[1:])
a()
