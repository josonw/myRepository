#! /usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class agg_reg_user_actret_property(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reg_user_actret_level_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                flevel int comment '注册当天用户最后等级',
                f1ducnt bigint comment '统计日的1日注册留存中对应等级的用户人数',
                f7dusernum bigint comment '统计日的7日注册留存中对应等级的用户人数',
                f30dusernum bigint comment '统计日的30日注册留存中对应等级的用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)

        hql = """create table if not exists dcnew.reg_user_actret_ptynum_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                frtnday int comment '新增留存天数',
                feq0 bigint comment '玩牌局数为0的对应留存的用户人数',
                feq1 bigint comment '玩牌局数为1的对应留存的用户人数',
                feq2 bigint comment '玩牌局数为2的对应留存的用户人数',
                feq3 bigint comment '玩牌局数为3的对应留存的用户人数',
                feq4 bigint comment '玩牌局数为4的对应留存的用户人数',
                feq5 bigint comment '玩牌局数为5的对应留存的用户人数',
                feq6 bigint comment '玩牌局数为6的对应留存的用户人数',
                feq7 bigint comment '玩牌局数为7的对应留存的用户人数',
                feq8 bigint comment '玩牌局数为8的对应留存的用户人数',
                feq9 bigint comment '玩牌局数为9的对应留存的用户人数',
                feq10 bigint comment '玩牌局数为10的对应留存的用户人数',
                feq11 bigint comment '玩牌局数为11的对应留存的用户人数',
                feq12 bigint comment '玩牌局数为12的对应留存的用户人数',
                feq13 bigint comment '玩牌局数为13的对应留存的用户人数',
                feq14 bigint comment '玩牌局数为14的对应留存的用户人数',
                feq15 bigint comment '玩牌局数为15的对应留存的用户人数',
                feq16 bigint comment '玩牌局数为16的对应留存的用户人数',
                feq17 bigint comment '玩牌局数为17的对应留存的用户人数',
                feq18 bigint comment '玩牌局数为18的对应留存的用户人数',
                feq19 bigint comment '玩牌局数为19的对应留存的用户人数',
                feq20 bigint comment '玩牌局数为20的对应留存的用户人数',
                fmq21lq30 bigint comment '玩牌局数大于等于21小于等于30的对应留存的用户人数',
                fmq31lq50 bigint comment '玩牌局数大于等于31小于等于50的对应留存的用户人数',
                fmq51lq100 bigint comment '玩牌局数大于等于51小于等于100的对应留存的用户人数',
                fm100 bigint comment '玩牌局数大于100的对应留存的用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)

        hql = """create table if not exists dcnew.reg_user_actret_logincnt_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                flgncnt int comment '注册当天用户登录次数',
                f1ducnt bigint comment '统计日的1日注册留存中对应登录次数的用户人数',
                f7dusernum bigint comment '统计日的7日注册留存中对应登录次数的用户人数',
                f30dusernum bigint comment '统计日的30日注册留存中对应登录次数的用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)

        hql = """create table if not exists dcnew.reg_user_actret_brpcnt_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fbrpcnt int comment '注册当天用户破产次数',
                f1ducnt bigint comment '统计日的1日注册留存中对应登录次数的用户人数',
                f7dusernum bigint comment '统计日的7日注册留存中对应登录次数的用户人数',
                f30dusernum bigint comment '统计日的30日注册留存中对应登录次数的用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)

        hql = """create table if not exists dcnew.reg_user_actret_pay_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                fname varchar(32) comment '付费标识，共两类，付费及非付费',
                f1ducnt bigint comment '统计日的1日注册留存中对应登录次数的用户人数',
                f7dusernum bigint comment '统计日的7日注册留存中对应登录次数的用户人数',
                f30dusernum bigint comment '统计日的30日注册留存中对应登录次数的用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        query = {
            'statdate':self.stat_date,
            'num_date': self.stat_date.replace("-", ""),
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)

        hql = """
        drop table if exists work.user_retained_regproperty_%(num_date)s;
        create table work.user_retained_regproperty_%(num_date)s
        as
        select
            distinct un.dt dt,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fgame_id,
            un.fchannel_code,
            un.fuid,
            datediff('%(statdate)s', un.dt) fretday
        from
            dim.user_act_array uau
        join dim.reg_user_array un
        on uau.fuid = un.fuid
            and uau.fgamefsk = un.fgamefsk
            and uau.fplatformfsk = un.fplatformfsk
            and uau.fhallfsk = un.fhallfsk
            and uau.fsubgamefsk = un.fgame_id
            and uau.fterminaltypefsk = un.fterminaltypefsk
            and uau.fversionfsk = un.fversionfsk
            and uau.fchannelcode = un.fchannel_code
            and uau.dt='%(ld_daybegin)s'
        where un.dt = '%(ld_30dayago)s'
        or un.dt = '%(ld_7dayago)s'
        or un.dt = '%(ld_1dayago)s';

        insert overwrite table dcnew.reg_user_actret_level_dis
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            urt.fgamefsk,
            urt.fplatformfsk,
            urt.fhallfsk,
            urt.fgame_id fsubgamefsk,
            urt.fterminaltypefsk,
            urt.fversionfsk,
            urt.fchannel_code fchannelcode,
            coalesce(uamu.flast_level,0) flevel,
            count(if(urt.dt = '%(ld_1dayago)s', urt.fuid, null)) f1dusernum,
            count(if(urt.dt = '%(ld_7dayago)s', urt.fuid, null)) f7dusernum,
            count(if(urt.dt = '%(ld_30dayago)s', urt.fuid, null)) f30dusernum
        from
            work.user_retained_regproperty_%(num_date)s urt
        left join
            (select
                dt,
                bm.fgamefsk,
                bm.fplatformfsk,
                bm.fversionfsk,
                uam.fuid,
                max(uam.flevel) flast_level
            from stage.user_grade_stg uam
            join dim.bpid_map bm
            on uam.fbpid = bm.fbpid
            where dt='%(ld_1dayago)s'
                or dt='%(ld_7dayago)s'
                or dt='%(ld_30dayago)s'
            group by dt,bm.fgamefsk,bm.fplatformfsk,bm.fversionfsk,uam.fuid
            ) uamu
        on urt.fuid = uamu.fuid and urt.dt = uamu.dt and urt.fgamefsk = uamu.fgamefsk and urt.fplatformfsk = uamu.fplatformfsk and urt.fversionfsk = uamu.fversionfsk
        group by urt.fgamefsk,urt.fplatformfsk,urt.fhallfsk,urt.fgame_id,urt.fversionfsk,urt.fterminaltypefsk,urt.fchannel_code,uamu.flast_level;


        insert overwrite table dcnew.reg_user_actret_ptynum_dis
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            urt.fgamefsk,
            urt.fplatformfsk,
            urt.fhallfsk,
            urt.fgame_id fsubgamefsk,
            urt.fterminaltypefsk,
            urt.fversionfsk,
            urt.fchannel_code fchannelcode,
            fretday frtnday,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 0 then urt.fuid else null end),0) feq0,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 1 then urt.fuid else null end),0) feq1,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 2 then urt.fuid else null end),0) feq2,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 3 then urt.fuid else null end),0) feq3,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 4 then urt.fuid else null end),0) feq4,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 5 then urt.fuid else null end),0) feq5,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 6 then urt.fuid else null end),0) feq6,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 7 then urt.fuid else null end),0) feq7,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 8 then urt.fuid else null end),0) feq8,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 9 then urt.fuid else null end),0) feq9,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 10 then urt.fuid else null end),0) feq10,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 11 then urt.fuid else null end),0) feq11,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 12 then urt.fuid else null end),0) feq12,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 13 then urt.fuid else null end),0) feq13,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 14 then urt.fuid else null end),0) feq14,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 15 then urt.fuid else null end),0) feq15,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 16 then urt.fuid else null end),0) feq16,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 17 then urt.fuid else null end),0) feq17,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 18 then urt.fuid else null end),0) feq18,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 19 then urt.fuid else null end),0) feq19,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) = 20 then urt.fuid else null end),0) feq20,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) >= 21 and coalesce(uamu.fparty_num,0) <= 30 then urt.fuid else null end),0) fmq21lq30,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) >= 31 and coalesce(uamu.fparty_num,0) <= 50 then urt.fuid else null end),0) fmq31lq50,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) >= 51 and coalesce(uamu.fparty_num,0) <= 100 then urt.fuid else null end),0) fmq51lq100,
            coalesce(count(distinct case when coalesce(uamu.fparty_num,0) > 100 then urt.fuid else null end),0) fm100
        from
            work.user_retained_regproperty_%(num_date)s urt
        left join
            (select fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fuid, fparty_num, dt
             from dim.user_act_array
             where dt='%(ld_1dayago)s'
                or dt='%(ld_7dayago)s'
                or dt='%(ld_30dayago)s'
            ) uamu
        on urt.fuid = uamu.fuid
        and urt.dt = uamu.dt
        and urt.fgamefsk = uamu.fgamefsk
        and urt.fplatformfsk = uamu.fplatformfsk
        and urt.fhallfsk = uamu.fhallfsk
        and urt.fgame_id = uamu.fsubgamefsk
        and urt.fterminaltypefsk = uamu.fterminaltypefsk
        and urt.fversionfsk = uamu.fversionfsk
        and urt.fchannel_code = uamu.fchannelcode
        group by urt.fgamefsk,urt.fplatformfsk,urt.fhallfsk,urt.fgame_id,urt.fversionfsk,urt.fterminaltypefsk,urt.fchannel_code,urt.fretday;

        insert overwrite table dcnew.reg_user_actret_logincnt_dis
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            urt.fgamefsk,
            urt.fplatformfsk,
            urt.fhallfsk,
            urt.fgame_id fsubgamefsk,
            urt.fterminaltypefsk,
            urt.fversionfsk,
            urt.fchannel_code fchannelcode,
            coalesce(uamu.flogin_cnt,0) flgncnt,
            count(if(urt.dt = '%(ld_1dayago)s', urt.fuid, null)) f1dusernum,
            count(if(urt.dt = '%(ld_7dayago)s', urt.fuid, null)) f7dusernum,
            count(if(urt.dt = '%(ld_30dayago)s', urt.fuid, null)) f30dusernum
        from
            work.user_retained_regproperty_%(num_date)s urt
        left join
            (select fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fuid, flogin_cnt, dt
             from dim.user_act_array
             where dt='%(ld_1dayago)s'
                or dt='%(ld_7dayago)s'
                or dt='%(ld_30dayago)s'
            ) uamu
        on urt.fuid = uamu.fuid
        and urt.dt = uamu.dt
        and urt.fgamefsk = uamu.fgamefsk
        and urt.fplatformfsk = uamu.fplatformfsk
        and urt.fhallfsk = uamu.fhallfsk
        and urt.fgame_id = uamu.fsubgamefsk
        and urt.fterminaltypefsk = uamu.fterminaltypefsk
        and urt.fversionfsk = uamu.fversionfsk
        and urt.fchannel_code = uamu.fchannelcode
        group by urt.fgamefsk,urt.fplatformfsk,urt.fhallfsk,urt.fgame_id,urt.fversionfsk,urt.fterminaltypefsk,urt.fchannel_code,coalesce(uamu.flogin_cnt,0);

        insert overwrite table dcnew.reg_user_actret_brpcnt_dis
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            urt.fgamefsk,
            urt.fplatformfsk,
            urt.fhallfsk,
            urt.fgame_id fsubgamefsk,
            urt.fterminaltypefsk,
            urt.fversionfsk,
            urt.fchannel_code fchannelcode,
            coalesce(uamu.frupt_cnt,0) fbrpcnt,
            count(if(urt.dt = '%(ld_1dayago)s', urt.fuid, null)) f1dusernum,
            count(if(urt.dt = '%(ld_7dayago)s', urt.fuid, null)) f7dusernum,
            count(if(urt.dt = '%(ld_30dayago)s', urt.fuid, null)) f30dusernum
        from
            work.user_retained_regproperty_%(num_date)s urt
        left join
            (select fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fuid, frupt_cnt, dt
             from dim.user_bankrupt_array
             where (dt='%(ld_1dayago)s'
                or dt='%(ld_7dayago)s'
                or dt='%(ld_30dayago)s') and frupt_cnt>0
            ) uamu
        on urt.fuid = uamu.fuid
        and urt.dt = uamu.dt
        and urt.fgamefsk = uamu.fgamefsk
        and urt.fplatformfsk = uamu.fplatformfsk
        and urt.fhallfsk = uamu.fhallfsk
        and urt.fgame_id = uamu.fsubgamefsk
        and urt.fterminaltypefsk = uamu.fterminaltypefsk
        and urt.fversionfsk = uamu.fversionfsk
        and urt.fchannel_code = uamu.fchannelcode
        group by urt.fgamefsk,urt.fplatformfsk,urt.fhallfsk,urt.fgame_id,urt.fversionfsk,urt.fterminaltypefsk,urt.fchannel_code,uamu.frupt_cnt;

        insert overwrite table dcnew.reg_user_actret_pay_dis
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            urt.fgamefsk,
            urt.fplatformfsk,
            urt.fhallfsk,
            urt.fgame_id fsubgamefsk,
            urt.fterminaltypefsk,
            urt.fversionfsk,
            urt.fchannel_code fchannelcode,
            case when coalesce(uamu.fpay_cnt,0) > 0 then '付费' else '非付费' end fname,
            count(if(urt.dt = '%(ld_1dayago)s', urt.fuid, null)) f1dusernum,
            count(if(urt.dt = '%(ld_7dayago)s', urt.fuid, null)) f7dusernum,
            count(if(urt.dt = '%(ld_30dayago)s', urt.fuid, null)) f30dusernum
        from
            work.user_retained_regproperty_%(num_date)s urt
        left join
            (select fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fuid, fpay_cnt, dt
             from dim.user_pay_array
             where dt='%(ld_1dayago)s'
                or dt='%(ld_7dayago)s'
                or dt='%(ld_30dayago)s'
            ) uamu
        on urt.fuid = uamu.fuid
        and urt.dt = uamu.dt
        and urt.fgamefsk = uamu.fgamefsk
        and urt.fplatformfsk = uamu.fplatformfsk
        and urt.fhallfsk = uamu.fhallfsk
        and urt.fgame_id = uamu.fsubgamefsk
        and urt.fterminaltypefsk = uamu.fterminaltypefsk
        and urt.fversionfsk = uamu.fversionfsk
        and urt.fchannel_code = uamu.fchannelcode
        group by urt.fgamefsk,urt.fplatformfsk,urt.fhallfsk,urt.fgame_id,urt.fversionfsk,urt.fterminaltypefsk,urt.fchannel_code,
                 case when coalesce(uamu.fpay_cnt,0) > 0 then '付费' else '非付费' end;
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        res = self.hq.exe_sql("""drop table if exists work.user_retained_regproperty_%(num_date)s;"""% query)
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_reg_user_actret_property(statDate)
a()
