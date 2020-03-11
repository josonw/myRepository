#! /usr/local/python272/bin/python
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

class agg_user_pay_income_dis(BaseStatModel):

    def create_tab(self):

        hql = """create table if not exists dcnew.user_pay_income_dis
            (
            fdate date comment '数据日期',
            fgamefsk bigint comment '游戏ID',
            fplatformfsk bigint comment '平台ID',
            fhallfsk bigint comment '大厅ID',
            fsubgamefsk bigint comment '子游戏ID',
            fterminaltypefsk bigint comment '终端类型ID',
            fversionfsk bigint comment '应用包版本ID',
            fchannelcode bigint comment '渠道ID',
            fdimension varchar(32) comment '数据指标类型，register表示新增用户指标，active表示活跃用户指标',
            ftmseq1 bigint comment '当天付费次数为1的用户人数',
            ftmseq2 bigint comment '当天付费次数为2的用户人数',
            ftmsm2lq5 bigint comment '当天付费次数大于2小于等于5的用户人数',
            ftmsm5lq10 bigint comment '当天付费次数大于5小于等于10的用户人数',
            ftmsm10lq15 bigint comment '当天付费次数大于10小于等于15的用户人数',
            ftmsm15 bigint comment '当天付费次数大于15的用户人数',
            famtm0l2 bigint comment '当天付费总额度大于0小于2的用户人数',
            famtmq2l4 bigint comment '当天付费总额度大于等于0小于4的用户人数',
            famtmq4l7 bigint comment '当天付费总额度大于等于4小于7的用户人数',
            famtmq7l11 bigint comment '当天付费总额度大于等于7小于11的用户人数',
            famtmq11l16 bigint comment '当天付费总额度大于等于11小于16的用户人数',
            famtmq16 bigint comment '当天付费总额度大于16的用户人数'
            )
            partitioned by (dt date);
        """

        res = self.sql_exe(hql)
        if res != 0:return res


    def stat(self):
        extend_group = {'fields':['fuid'],
                        'groups':[[1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """select
                fgamefsk,
                fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                sum(pi.fpay_cnt) fpay_cnt,
                sum(pi.ftotal_usd_amt) ftotal_usd_amt
            from
                dim.user_pay_day pi
            join
                dim.bpid_map bm
            on
                pi.fbpid = bm.fbpid
            where dt = '%(statdate)s'
              and hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                fuid
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)
        hql ="""
        drop table if exists work.user_pay_income_dis_%(statdatenum)s;
        create table work.user_pay_income_dis_%(statdatenum)s as
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.user_pay_income_dis
        partition(dt = '%(statdate)s' )
        select
            '%(statdate)s' fdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            'register' fdimension,
            coalesce(count(distinct case when coalesce(piu.fpay_cnt,0) = 1 then un.fuid else null end),0) ftmseq1,
            coalesce(count(distinct case when coalesce(piu.fpay_cnt,0) = 2 then un.fuid else null end),0) ftmseq2,
            coalesce(count(distinct case when coalesce(piu.fpay_cnt,0) > 2 and coalesce(piu.fpay_cnt,0) <= 5 then un.fuid else null end),0) ftmsm2lq5,
            coalesce(count(distinct case when coalesce(piu.fpay_cnt,0) > 5 and coalesce(piu.fpay_cnt,0) <= 10 then un.fuid else null end),0) ftmsm5lq10,
            coalesce(count(distinct case when coalesce(piu.fpay_cnt,0) > 10 and coalesce(piu.fpay_cnt,0) <= 15 then un.fuid else null end),0) ftmsm10lq15,
            coalesce(count(distinct case when coalesce(piu.fpay_cnt,0) > 15 then un.fuid else null end),0) ftmsm15,
            coalesce(count(distinct case when coalesce(piu.ftotal_usd_amt,0) > 0 and coalesce(piu.ftotal_usd_amt,0) < 2 then un.fuid else null end),0) famtm0l2,
            coalesce(count(distinct case when coalesce(piu.ftotal_usd_amt,0) >= 2 and coalesce(piu.ftotal_usd_amt,0) < 4 then un.fuid else null end),0) famtmq2l4,
            coalesce(count(distinct case when coalesce(piu.ftotal_usd_amt,0) >= 4 and coalesce(piu.ftotal_usd_amt,0) < 7 then un.fuid else null end),0) famtmq4l7,
            coalesce(count(distinct case when coalesce(piu.ftotal_usd_amt,0) >= 7 and coalesce(piu.ftotal_usd_amt,0) < 11 then un.fuid else null end),0) famtmq7l11,
            coalesce(count(distinct case when coalesce(piu.ftotal_usd_amt,0) >= 11 and coalesce(piu.ftotal_usd_amt,0) < 16 then un.fuid else null end),0) famtmq11l16,
            coalesce(count(distinct case when coalesce(piu.ftotal_usd_amt,0) >= 16 then un.fuid else null end),0) famtmq16
        from
            dim.reg_user_array un
        left join work.user_pay_income_dis_%(statdatenum)s piu
        on un.fuid = piu.fuid
        and un.fgamefsk = piu.fgamefsk
        and un.fplatformfsk = piu.fplatformfsk
        and un.fhallfsk = piu.fhallfsk
        and un.fgame_id = piu.fgame_id
        and un.fterminaltypefsk = piu.fterminaltypefsk
        and un.fversionfsk = piu.fversionfsk
        and un.fchannel_code = piu.fchannel_code
        where
            un.dt = '%(statdate)s'
        group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,un.fversionfsk,un.fchannel_code

        union all
        select
                '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                'active' fdimension,
                coalesce(count(distinct case when coalesce(fpay_cnt,0) = 1 then fuid else null end),0) ftmseq1,
                coalesce(count(distinct case when coalesce(fpay_cnt,0) = 2 then fuid else null end),0) ftmseq2,
                coalesce(count(distinct case when coalesce(fpay_cnt,0) > 2 and coalesce(fpay_cnt,0) <= 5 then fuid else null end),0) ftmsm2lq5,
                coalesce(count(distinct case when coalesce(fpay_cnt,0) > 5 and coalesce(fpay_cnt,0) <= 10 then fuid else null end),0) ftmsm5lq10,
                coalesce(count(distinct case when coalesce(fpay_cnt,0) > 10 and coalesce(fpay_cnt,0) <= 15 then fuid else null end),0) ftmsm10lq15,
                coalesce(count(distinct case when coalesce(fpay_cnt,0) > 15 then fuid else null end),0) ftmsm15,
                coalesce(count(distinct case when coalesce(ftotal_usd_amt,0) > 0 and coalesce(ftotal_usd_amt,0) < 2 then fuid else null end),0) famtm0l2,
                coalesce(count(distinct case when coalesce(ftotal_usd_amt,0) >= 2 and coalesce(ftotal_usd_amt,0) < 4 then fuid else null end),0) famtmq2l4,
                coalesce(count(distinct case when coalesce(ftotal_usd_amt,0) >= 4 and coalesce(ftotal_usd_amt,0) < 7 then fuid else null end),0) famtmq4l7,
                coalesce(count(distinct case when coalesce(ftotal_usd_amt,0) >= 7 and coalesce(ftotal_usd_amt,0) < 11 then fuid else null end),0) famtmq7l11,
                coalesce(count(distinct case when coalesce(ftotal_usd_amt,0) >= 11 and coalesce(ftotal_usd_amt,0) < 16 then fuid else null end),0) famtmq11l16,
                coalesce(count(distinct case when coalesce(ftotal_usd_amt,0) >= 16 then fuid else null end),0) famtmq16
            from work.user_pay_income_dis_%(statdatenum)s
            group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code

        """

        res = self.sql_exe(hql)
        if res != 0:return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_pay_income_dis_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

#生成统计实例
a = agg_user_pay_income_dis(sys.argv[1:])
a()
