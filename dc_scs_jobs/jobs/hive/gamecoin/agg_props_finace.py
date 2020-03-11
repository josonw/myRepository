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


class agg_props_finace(BaseStatModel):
    def create_tab(self):

        hql = """
        --道具分析，包括道具变化时的场次、原因、道具类型等等
        create table if not exists dcnew.props_finace (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fpname              varchar(256)   comment '牌局场次一级分类',
               fsubname            varchar(256)   comment '牌局场次二级分类',
               fdirection          varchar(50)    comment '操作类型:IN\OUT',
               act_id              varchar(32)    comment '操作编号(变化原因)',
               ftype               varchar(50)    comment '道具编号',
               ctype               int            comment '币种id',
               fnum                bigint         comment '道具变化数量',
               funum               bigint         comment '道具变化用户数'
               )comment '道具分析'
               partitioned by(dt date)
        location '/dw/dcnew/props_finace';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fpname','fsubname','fdirection','act_id','ftype','ctype'],
                        'groups':[[1, 1, 1, 1, 1, 1]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--取道具变化时的场次、原因、道具类型以及相应的数量与人数
        drop table if exists work.props_finace_tmp_b_%(statdatenum)s;
        create table work.props_finace_tmp_b_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     coalesce(p.fgame_id,cast (0 as bigint)) fgame_id,
                     coalesce(cast (p.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                     p.fpname,
                     p.fsubname,
                     p.act_type fdirection,
                     p.act_id,
                     p.prop_id ftype,
                     p.c_type ctype,
                     abs(p.act_num) fnum,
                     p.fuid
                from stage.pb_props_stream_stg p
                join dim.bpid_map c
                  on p.fbpid=c.fbpid
               where p.dt = "%(statdate)s"
                 and p.act_type in (1, 2)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合数据 并导入到正式表上
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fpname,
                       fsubname,
                       fdirection,
                       act_id,
                       ftype,
                       ctype,
                       sum(fnum) fnum,
                       count(distinct fuid) funum
                  from work.props_finace_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fpname,
                       fsubname,
                       fdirection,
                       act_id,
                       ftype,
                       ctype
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)


        hql = """
        insert overwrite table dcnew.props_finace
        partition (dt = '%(statdate)s')
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        # 统计完清理掉临时表
        hql = """drop table if exists work.props_finace_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_props_finace(sys.argv[1:])
a()
