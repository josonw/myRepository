#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class product_dim_update(BaseStat):

    def create_tab(self):
        hql = """


        -- 特殊同步要求,全表
        create table if not exists analysis.payment_product_dim
        (
            fp_fsk          varchar(50),
            fp_id           varchar(256),
            fp_name         varchar(256),
            fname           varchar(256),
            ftype           int
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []



        # hql = """
        # insert into table analysis.payment_product_dim
        # select b.fp_id fp_fsk, b.fp_id, b.fp_name, b.fname, b.fp_type ftype
        # from analysis.payment_product_dim a
        # right join (
        #     select case when coalesce(fproduct_id,'0')='0' then fp_id else fproduct_id end fp_id,
        #           case when coalesce(fproduct_id,'0')='0' then fp_name else fproduct_name end fp_name,
        #           case when coalesce(fproduct_id,'0')='0' then (case when fp_type = 2 then concat('游戏币(', fp_num, ')' )
        #                                                     when fp_type = 1 then concat('博雅币(', fp_num, ')' )
        #                                                else fp_name end)
        #           else  concat(fproduct_id,'_',fproduct_name) end fname,
        #           fp_type
        #      from stage.payment_stream_stg
        #     where dt = '%(stat_date)s'
        #       and (fproduct_id is not null or (fp_id is not null and fp_name is not null) )
        #     group by case when coalesce(fproduct_id,'0')='0' then fp_id else fproduct_id end ,
        #              case when coalesce(fproduct_id,'0')='0' then fp_name else fproduct_name end ,
        #              case when coalesce(fproduct_id,'0')='0' then (case when fp_type = 2 then concat('游戏币(', fp_num, ')' )
        #                                                     when fp_type = 1 then concat('博雅币(', fp_num, ')' )
        #                                                else fp_name end)
        #              else  concat(fproduct_id,'_',fproduct_name) end ,
        #              fp_type

        # ) b
        # on a.fp_id = b.fp_id and a.fp_name = b.fp_name
        # where a.fp_id is null
        # """ % self.hql_dict
        # hql_list.append( hql )




        #payment_product_dim的更新停掉，改为更新new_product_dim
        hql = """
        drop table if exists analysis.tmp_product_day_map;
        create table analysis.tmp_product_day_map as

        select fgamefsk,fplatformfsk,fp_id,fp_name,fname,fp_type from (
            SELECT fdate,fgamefsk,
                   fplatformfsk,
                   CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END fp_id,
                   CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_name ELSE fproduct_name END fp_name,
                   CASE
                       WHEN coalesce(fproduct_id,'0')='0' THEN (CASE
                                                                    WHEN fp_type = 2 THEN concat('游戏币(', fp_num, ')')
                                                                    WHEN fp_type = 1 THEN concat('博雅币(', fp_num, ')')
                                                                    ELSE fp_name
                                                                END)
                       ELSE concat(fproduct_id,'_',fproduct_name)
                   END fname,
                   fp_type,
                   row_number() over(partition by fgamefsk,fplatformfsk,CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END order by fdate desc) as flag
            FROM stage.payment_stream_stg m
            JOIN analysis.bpid_platform_game_ver_map n ON m.fbpid= n.fbpid
            WHERE dt = '%(stat_date)s'
              AND (fproduct_id IS NOT NULL
                   OR (fp_id IS NOT NULL
                       AND fp_name IS NOT NULL))
            GROUP BY fdate,fgamefsk,
                     fplatformfsk,
                     CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_id ELSE fproduct_id END ,
                     CASE WHEN coalesce(fproduct_id,'0')='0' THEN fp_name ELSE fproduct_name END ,
                     CASE
                         WHEN coalesce(fproduct_id,'0')='0' THEN (CASE
                                                                      WHEN fp_type = 2 THEN concat('游戏币(', fp_num, ')')
                                                                      WHEN fp_type = 1 THEN concat('博雅币(', fp_num, ')')
                                                                      ELSE fp_name
                                                                  END)
                         ELSE concat(fproduct_id,'_',fproduct_name)
                     END ,
                     fp_type) aa
        where flag =1
                 """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.new_product_dim
        select distinct fgamefsk,fplatformfsk, fp_id, fp_name, fname, fp_type
          from (--当日新增的产品id
                select a.fgamefsk,a.fplatformfsk, a.fp_id, a.fp_name, a.fname, a.fp_type
                  from analysis.tmp_product_day_map a
                  left join analysis.new_product_dim b
                    on a.fgamefsk=b.fgamefsk
                   and a.fplatformfsk=b.fplatformfsk
                   and a.fp_id = b.fp_id
                 where b.fp_id is null
                 union all
                --更新之前的的产品名称
                select a.fgamefsk,a.fplatformfsk, a.fp_id, coalesce(b.fp_name,a.fp_name) fp_name, coalesce(b.fname,a.fname) fname, a.ftype fp_type
                  from analysis.new_product_dim a
                  left join analysis.tmp_product_day_map b
                    on a.fgamefsk=b.fgamefsk
                   and a.fplatformfsk=b.fplatformfsk
                   and a.fp_id = b.fp_id
                  ) t ;

        """ % self.hql_dict
        hql_list.append( hql )


        res = 0
        res = self.exe_hql_list(hql_list)
        return res


if __name__ == "__main__":
    stat_date = ''

    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = product_dim_update(stat_date)
    a()
