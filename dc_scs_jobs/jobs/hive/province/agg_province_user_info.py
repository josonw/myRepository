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


class agg_province_user_info(BaseStatModel):
    def create_tab(self):
        hql = """--分省数据_用户相关
        create table if not exists dcnew.province_user_info (
                fdate               date,
                fgamefsk            bigint,
                fplatformfsk        bigint,
                fhallfsk            bigint,
                fsubgamefsk         bigint,
                fterminaltypefsk    bigint,
                fversionfsk         bigint,
                fchannelcode        bigint,
                fprovince           varchar(64)     comment '省份',
                fact_unum           bigint          comment '活跃人数',
                freg_unum           bigint          comment '新增人数',
                fmax_online         bigint          comment '在线峰值',
                fmax_play           bigint          comment '在玩峰值',
                fldr_back_unum      bigint          comment '昨注回头',
                fgame_back_unum     bigint          comment '游戏内次日留存',
                fhall_back_unum     bigint          comment '大厅内次日留存',
                fpart_reg_unum      bigint          comment '代理直接发展新增',
                fpart_back_unum     bigint          comment '代理直接新增回头',
                fpart_ind_reg_unum  bigint          comment '代理间接发展新增',
                fpart_ind_back_unum bigint          comment '代理间接发展新增回头',
                fshare_unum         bigint          comment '分享人数',
                fshare_reg_unum     bigint          comment '分享拉新人数',
                fgame_act_unum      bigint          comment '游戏内活跃',
                f30act_unum         bigint          comment '最近30天活跃用户数',
                favg_online         bigint          comment '在线均值',
                favg_play           bigint          comment '在玩均值'
                )comment '分省数据_用户相关'
                partitioned by(dt date)
          location '/dw/dcnew/province_user_info'
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

        hql = """--取今日活跃相关指标
        drop table if exists work.province_user_tmp_1_%(statdatenum)s;
        create table work.province_user_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(d) */ c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fplatformname fprovince,
                 coalesce(t1.fgame_id,%(null_int_report)d) fgame_id,
                 t1.fuid act_uid,
                 t2.fuid reg_back_uid,
                 case when t1.fgame_id is not null then 1 else 0 end game_act,
                 case when t2.fpartner_info in ('1','2') then 1 else 0 end part_reg_back,
                 case when t3.fuid is not null then 1 else 0 end act_ret,
                 case when t3.fuid is not null and t1.fgame_id = t3.fgame_id then 1 else 0 end game_ret,
                 case when t4.fuid is not null then 1 else 0 end is_share
            from dim.user_act t1
            left join dim.reg_user_main_additional t2 --昨注回头
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(ld_1day_ago)s'
            left join dim.user_act t3 --次日留存
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(ld_1day_ago)s'
            left join stage.feed_send_stg t4
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
             and t4.dt = '%(statdate)s'
            join dim.bpid_map c
              on t1.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        base_hql = """
                select fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       count(distinct act_uid) fact_unum,
                       0 freg_unum,
                       0 fmax_online,
                       0 fmax_play,
                       count(distinct reg_back_uid) fldr_back_unum,
                       count(distinct case when game_ret = 1 then act_uid end) fgame_back_unum,
                       count(distinct case when act_ret = 1 then act_uid end) fhall_back_unum,
                       0 fpart_reg_unum,
                       count(distinct case when part_reg_back = 1 then act_uid end) fpart_back_unum,
                       0 fpart_ind_reg_unum,
                       0 fpart_ind_back_unum,
                       count(distinct case when is_share = 1 then act_uid end) fshare_unum,
                       0 fshare_reg_unum,
                       count(distinct case when game_act = 1 then act_uid end) fgame_act_unum,
                       0 f30act_unum,
                       0 favg_online,
                       0 favg_play
                  from work.province_user_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        hql = ("""
                  drop table if exists work.province_user_tmp_%(statdatenum)s;
                create table work.province_user_tmp_%(statdatenum)s as """ +
                base_hql + """%(extend_group_1)s """ +
                base_hql + """%(extend_group_2)s """ +
                base_hql + """%(extend_group_3)s """ +
                base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--取今日注册相关指标
        drop table if exists work.province_user_tmp_2_%(statdatenum)s;
        create table work.province_user_tmp_2_%(statdatenum)s as
          select c.fgamefsk,
                 c.fplatformfsk,
                 c.fhallfsk,
                 c.fterminaltypefsk,
                 c.fversionfsk,
                 c.hallmode,
                 c.fplatformname fprovince,
                 coalesce(t2.fgame_id,%(null_int_report)d) fgame_id,
                 t1.fuid,
                 case when nvl(t3.ffeed_as,0) = 2 then 1 else 0 end ffeed_reg,
                 case when t1.fpartner_info in ('1','2') then 1 else 0 end part_reg
            from dim.reg_user_main_additional t1
            left join dim.user_act t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            left join stage.feed_clicked_stg t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            join dim.bpid_map c
              on t1.fbpid=c.fbpid
             and c.fgamefsk = 4132314431  --地方棋牌
           where t1.dt = '%(statdate)s'

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        base_hql = """
                select fgamefsk,
                       fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       0 fact_unum,
                       count(distinct fuid) freg_unum,
                       0 fmax_online,
                       0 fmax_play,
                       0 fldr_back_unum,
                       0 fgame_back_unum,
                       0 fhall_back_unum,
                       count(distinct case when part_reg = 1 then fuid end) fpart_reg_unum,
                       0 fpart_back_unum,
                       0 fpart_ind_reg_unum,
                       0 fpart_ind_back_unum,
                       0 fshare_unum,
                       count(distinct case when ffeed_reg = 1 then fuid end) fshare_reg_unum,
                       0 fgame_act_unum,
                       0 f30act_unum,
                       0 favg_online,
                       0 favg_play
                  from work.province_user_tmp_2_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       fprovince """

        hql = (""" insert into table work.province_user_tmp_%(statdatenum)s """ +
               base_hql + """%(extend_group_1)s """ +
               base_hql + """%(extend_group_2)s """ +
               base_hql + """%(extend_group_3)s """ +
               base_hql + """%(extend_group_4)s """ )% query

        res = self.sql_exe(hql)
        if res != 0:
            return res


        # hql = """--取最近30天活跃用户数
        # drop table if exists work.province_user_tmp_3_%(statdatenum)s;
        # create table work.province_user_tmp_3_%(statdatenum)s as
          # select /*+ MAPJOIN(d) */ distinct c.fgamefsk,
                 # c.fplatformfsk,
                 # c.fhallfsk,
                 # c.fterminaltypefsk,
                 # c.fplatformname fprovince,
                 # coalesce(t1.fgame_id,%(null_int_report)d) fgame_id,
                 # t1.fuid
            # from dim.user_act t1
            # join dim.bpid_map c
              # on t1.fbpid=c.fbpid
             # and c.fgamefsk = 4132314431  --地方棋牌
           # where t1.dt between '%(ld_29day_ago)s' and '%(statdate)s'
        # """
        # res = self.sql_exe(hql)
        # if res != 0:
            # return res
#
        # base_hql = """
                # select fgamefsk,
                       # fplatformfsk,
                       # coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       # coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       # coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       # coalesce(fprovince,'%(null_str_group_rule)s') fprovince,
                       # 0 fact_unum,
                       # 0 freg_unum,
                       # 0 fmax_online,
                       # 0 fmax_play,
                       # 0 fldr_back_unum,
                       # 0 fgame_back_unum,
                       # 0 fhall_back_unum,
                       # 0 fpart_reg_unum,
                       # 0 fpart_back_unum,
                       # 0 fpart_ind_reg_unum,
                       # 0 fpart_ind_back_unum,
                       # 0 fshare_unum,
                       # 0 fshare_reg_unum,
                       # 0 fgame_act_unum,
                       # count(distinct fuid) f30act_unum,
                       # 0 favg_online,
                       # 0 favg_play
                  # from work.province_user_tmp_3_%(statdatenum)s gs
              # group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,
                       # fprovince """
#
        # hql = (""" insert into table work.province_user_tmp_%(statdatenum)s """ +
               # base_hql + """%(extend_group_1)s """ +
               # base_hql + """%(extend_group_2)s """ +
               # base_hql + """%(extend_group_3)s """ +
               # base_hql + """%(extend_group_4)s """ )% query
#
        # res = self.sql_exe(hql)
        # if res != 0:
            # return res
#
#
        hql = """insert overwrite table dcnew.province_user_info
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
                         nvl(sum(fact_unum),0) fact_unum,
                         nvl(sum(freg_unum),0) freg_unum,
                         nvl(sum(fmax_online),0) fmax_online,
                         nvl(sum(fmax_play),0) fmax_play,
                         nvl(sum(fldr_back_unum),0) fldr_back_unum,
                         nvl(sum(fgame_back_unum),0) fgame_back_unum,
                         nvl(sum(fhall_back_unum),0) fhall_back_unum,
                         nvl(sum(fpart_reg_unum),0) fpart_reg_unum,
                         nvl(sum(fpart_back_unum),0) fpart_back_unum,
                         nvl(sum(fpart_ind_reg_unum),0) fpart_ind_reg_unum,
                         nvl(sum(fpart_ind_back_unum),0) fpart_ind_back_unum,
                         nvl(sum(fshare_unum),0) fshare_unum,
                         nvl(sum(fshare_reg_unum),0) fshare_reg_unum,
                         nvl(sum(fgame_act_unum),0) fgame_act_unum,
                         nvl(sum(f30act_unum), 0) f30act_unum,
                         nvl(sum(favg_online), 0) favg_online,
                         nvl(sum(favg_play), 0) favg_play
                    from work.province_user_tmp_%(statdatenum)s gs
                   group by fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fprovince

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.province_user_tmp_1_%(statdatenum)s;
                 drop table if exists work.province_user_tmp_2_%(statdatenum)s;
                 drop table if exists work.province_user_tmp_3_%(statdatenum)s;
                 drop table if exists work.province_user_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res



#生成统计实例
a = agg_province_user_info(sys.argv[1:])
a()
