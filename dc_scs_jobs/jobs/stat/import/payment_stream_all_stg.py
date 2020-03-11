#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class payment_stream_all_stg(BaseStat):

    def create_tab(self):
        hql = """
            create table if not exists stage.payadmin_order_all(
              id bigint,
              pid bigint,
              mid bigint,
              sitemid varchar(128),
              buyer varchar(128),
              sid bigint,
              appid bigint,
              pmode smallint,
              pamount double,
              pcoins bigint,
              pchips bigint,
              pcard bigint,
              pnum bigint,
              payconfid bigint,
              pcoinsnow bigint,
              pdealno varchar(256),
              pbankno varchar(256),
              desc varchar(256),
              pstarttime int,
              pendtime int,
              pstatus smallint,
              pamount_rate double,
              pamount_unit varchar(64),
              pamount_usd double,
              ext_1 bigint,
              ext_2 bigint,
              ext_3 bigint,
              ext_4 varchar(64),
              ext_5 varchar(256),
              ext_6 varchar(64),
              ext_7 varchar(64),
              ext_8 varchar(256),
              ext_9 varchar(256),
              ext_10 varchar(256),
              synctime bigint,
              ip varchar(50),
			  cid string,
			  imei string
            )
            partitioned by (dt date)
            location '/dw/stage/payadmin_order_all/';

            alter table stage.payadmin_order_all set serdeproperties('serialization.null.format'='');
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        hql = """
            CREATE EXTERNAL TABLE if not exists stage.payment_stream_all_stg(
               fbpid varchar(50),
               fdate string,
               fplatform_uid varchar(50),
               fis_platform_uid tinyint,
               forder_id varchar(255),
               fcoins_num decimal(20,2),
               frate decimal(20,7),
               fm_id varchar(256),
               fm_name varchar(256),
               fp_id varchar(256),
               fp_name varchar(256),
               fchannel_id varchar(64),
               fimei varchar(64),
               fsucc_time string,
               fcallback_time string,
               fp_type int,
               fp_num bigint,
               fsid int,
               fappid int,
               fpmode int,
               fuid bigint,
               fpamount_usd decimal(20,2),
               pstatus smallint,
               fprodcut_id varchar(64),
               fproduct_name varchar(64),
               fip varchar(64),
			   fcid string)
            PARTITIONED BY (
              dt date)
            ROW FORMAT DELIMITED
              NULL DEFINED AS ''
            STORED AS INPUTFORMAT
              'org.apache.hadoop.mapred.TextInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            LOCATION
              '/dw/stage/pay_stream_all'
        """
        res = self.hq.exe_sql(hql)

        return res


    def stat(self):
        self.hq.debug = 0

        args_dic = {
            "ld_begin": statDate
        }

        hql = """
        alter table stage.payment_stream_all_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pay_stream_all/%(ld_begin)s';
        alter table stage.payadmin_order_all add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/payadmin_order_all/%(ld_begin)s';
        """ % args_dic

        hql += """
            insert overwrite table stage.payment_stream_all_stg partition(dt='%(ld_begin)s')
            select po.bpid as fbpid,
                po.pstarttime as fdate,
                po.sitemid as fplatform_uid,
                po.fis_platform_uid,
                po.pid as forder_id,
                po.pamount as fcoins_num,
                po.rate as frate,
                po.statid as fm_id,
                po.statname as fm_name,
                po.payconfid as fp_id,
                case when pp.getname is null then '' else pp.getname end as fp_name,
                po.ext_6 as fchannel_id,
                po.imei as fimei,
                po.pendtime as fsucc_time,
                po.fcallback_time, po.fp_type, po.fp_num,
                po.sid as fsid, po.appid as fappid, po.pmode as fpmode,
                case when po.is_gat = 1 then po.mid + 1000000000 else po.mid end as fuid,
                po.pamount_usd as fpamount_usd,
                po.pstatus as pstatus,
                po.pcoinsnow as fproduct_id,
                po.desc as fproduct_name,
                po.ip as fip,
				po.cid as fcid
            from
            (
                select id, max(getname) as getname
                from stage.paycenter_payconf
               where dt="%(ld_begin)s"
                group by id
            ) pp
            right join
            (
                select pc.statid, pc.statname, po3.*
                from
                (
                    select sid, pmode, max(statid) as statid, max(statname) as statname
                    from stage.paycenter_chanel
                    where dt="%(ld_begin)s"
                    group by sid, pmode
                ) pc
                right join
                (select case when ur.rate is not null then ur.rate else pr.rate end rate, po2.*
                    from
                    (
                        select pa.bpid
                              ,case when bpidmap.fbpid is not null then 1 else 0 end is_gat
                              ,po1.*
                        from
                        (
                            select sid, appid, max(bpid) as bpid
                            from stage.paycenter_apps
                            where dt="%(ld_begin)s"
                            group by sid, appid
                        ) pa
                        left join (select distinct fbpid
                                    from dim.bpid_map
                                   where fgamefsk = 4132314431
                                     and fplatformfsk in (77000221,77000222,77000223) --2017-11-3，香港台湾澳门的fuid加10亿
                        ) bpidmap
                          on pa.bpid = bpidmap.fbpid
                        join
                        (
                            select from_unixtime(pstarttime) as pstarttime,
                                sitemid, 1 as fis_platform_uid,
                                pid, pamount,
                                sid, appid, pmode, mid,
                                sid sid_tmp,
                                pmode pmode_tmp,
                                pamount_unit unit_tmp,
                                payconfid,
                                ext_6, imei,
                                from_unixtime(pendtime) as pendtime,
                                case when ext_1 = 0 or ext_1 = '' then '' else from_unixtime(ext_1) end as fcallback_time,
                                case when pcoins != 0 then '1'
                                     when pchips != 0 then '2'
                                     when pcard  != 0 then '3' end as fp_type,
                                case when pcoins != 0 then pcoins
                                     when pchips != 0 then pchips
                                     when pcard  != 0 then pcard end as fp_num,
                                pamount_usd,
                                pcoinsnow,
                                desc,
                                pstatus,
                                ip,
								cid
                            from stage.payadmin_order_all
                            where dt='%(ld_begin)s'
                        ) po1
                        on pa.sid = po1.sid and pa.appid = po1.appid
                    ) po2
                    left join ( select unit, rate from stage.paycenter_rate
                                where dt="%(ld_begin)s" group by unit, rate ) ur
                    on po2.unit_tmp = ur.unit
                    left join ( select sid, pmode, rate from stage.paycenter_chanel
                                where dt="%(ld_begin)s" group by sid, pmode, rate
                    ) pr
                    on pr.sid = po2.sid_tmp and pr.pmode = po2.pmode_tmp
                ) po3
                on pc.sid = po3.sid and pc.pmode = po3.pmode
            ) po
            on pp.id = po.payconfid;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
import_job = payment_stream_all_stg(statDate)
import_job()
