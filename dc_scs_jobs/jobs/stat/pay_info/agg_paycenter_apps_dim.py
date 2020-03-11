#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class agg_paycenter_apps_dim(BaseStat):
    """appid sid 与 bpid映射表
    LigaLi 2016-07-15

    支付配置表修改指定目录从payment_stream_stg.py转移到该脚本
    tommyjiang 2017-04-25
    """
    def create_tab(self):
        hql = """
            create table if not exists stage.paycenter_payconf(
              id          bigint,
              sid         bigint,
              appid       bigint,
              pmode       bigint,
              pamount     double,
              discount    double,
              pcoins      bigint,
              pchips      bigint,
              pcard       bigint,
              item_id     bigint,
              ptype       bigint,
              pnum        bigint,
              getname     varchar(50),
              desc        string,
              stag        int,
              currency    varchar(50),
              prid        bigint,
              expire      bigint,
              state       varchar(50),
              device      int,
              sortid      bigint,
              etime       bigint,
              status      int,
              use_status  int
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_payconf';

            create table if not exists stage.paycenter_chanel(
              sid bigint,
              pmode bigint,
              pmodename varchar(256),
              unit varchar(20),
              rate double,
              companyid bigint,
              tag smallint,
              status int,
              etime bigint,
              statid bigint,
              statname varchar(100),
              use_status int
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_chanel';

            create table if not exists stage.paycenter_rate(
              id bigint,
              rate double,
              unit varchar(10),
              ext varchar(256),
              status int,
              etime bigint
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_rate';

            create table if not exists stage.paycenter_apps(
              appid bigint,
              sappid varchar(256),
              sappsecret varchar(256),
              sid bigint,
              appname varchar(256),
              apppaykey varchar(256),
              apppaycgi varchar(256),
              platjson varchar(256),
              gameid bigint,
              langid bigint,
              bpid varchar(32),
              childid varchar(500),
              sortid varchar(10),
              tag smallint,
              status int,
              etime bigint,
              user_status int
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_apps';

            create table if not exists stage.payadmin_order(
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
              ip varchar(50)
            )
            partitioned by (dt date)
            location '/dw/stage/payadmin_order/';

            create table if not exists stage.paycenter_apps_attr(
               id              bigint      ,
               sid             bigint      ,
               appid           bigint      ,
               th_appname      varchar(256) ,
               is_lianyun      varchar(128) ,
               device_type_id  int      ,
               etime           bigint      ,
               ext_1           varchar(128)
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_apps_attr';

            create table if not exists stage.paycenter_site(
               sid          bigint  ,
               sitename     varchar(256)    ,
               status       tinyint      ,
               etime        bigint           ,
               use_status   tinyint       ,
               th_sitename  varchar(256)
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_site';

            create table if not exists stage.paycenter_game(
               gameid   bigint     ,
               name     varchar(128),
               th_name  varchar(128)
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_game';

            create table if not exists stage.paycenter_lang(
               langid  bigint     ,
               name    varchar(128)
            )
            partitioned by (dt date)
            location '/dw/stage/paycenter_lang';

            alter table stage.paycenter_payconf set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_chanel set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_rate set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_apps set serdeproperties('serialization.null.format'='');
            alter table stage.payadmin_order set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_apps_attr set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_site set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_game set serdeproperties('serialization.null.format'='');
            alter table stage.paycenter_lang set serdeproperties('serialization.null.format'='');
        alter table stage.payment_stream_stg add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/pay_stream/%(ld_begin)s';
        alter table stage.paycenter_payconf add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_payconf/%(ld_begin)s';
        alter table stage.paycenter_chanel add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_chanel/%(ld_begin)s';
        alter table stage.paycenter_rate add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_rate/%(ld_begin)s';
        alter table stage.paycenter_apps add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_apps/%(ld_begin)s';
        alter table stage.payadmin_order add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/payadmin_order/%(ld_begin)s';
        alter table stage.paycenter_apps_attr add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_apps_attr/%(ld_begin)s';
        alter table stage.paycenter_site add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_site/%(ld_begin)s';
        alter table stage.paycenter_game add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_game/%(ld_begin)s';
        alter table stage.paycenter_lang add if not exists partition(dt='%(ld_begin)s') location '/dw/stage/paycenter_lang/%(ld_begin)s';
        """% self.hql_dict
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        create table if not exists analysis.paycenter_apps_dim
        (
            appid   BIGINT,
            sappid  VARCHAR(256),
            sappsecret  VARCHAR(256),
            sid BIGINT,
            appname VARCHAR(256),
            apppaykey   VARCHAR(256),
            apppaycgi   VARCHAR(256),
            platjson    VARCHAR(256),
            game_id  BIGINT,
            lang_id  BIGINT,
            bpid    VARCHAR(256),
            childid VARCHAR(256),
            sortid  VARCHAR(256),
            tag SMALLINT,
            status  INT,
            etime   BIGINT,
            user_status INT,
            is_lianyun varchar(128),
            sitename varchar(256),
            game_name varchar(128),
            lang_name varchar(128)
        );
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        self.hq.debug = 0

        # app配置
        hql = """
        insert overwrite table analysis.paycenter_apps_dim
        select
            pa.appid,
            max(pa.sappid) sappid,
            max(pa.sappsecret) sappsecret,
            pa.sid,
            max(pa.appname) appname,
            max(pa.apppaykey) apppaykey,
            max(pa.apppaycgi) apppaycgi,
            max(pa.platjson) platjson,
            max(pa.gameid) game_id,
            max(pa.langid) lang_id,
            max(pa.bpid) bpid,
            max(pa.childid) childid,
            max(pa.sortid) sortid,
            max(pa.tag) tag,
            max(pa.status) status,
            max(pa.etime) etime,
            max(pa.user_status) user_status,
            max(paa.is_lianyun) is_lianyun,
            max(ps.sitename) sitename,
            max(pg.name) game_name,
            max(pl.name) lang_name
            from stage.paycenter_apps pa

            left join stage.paycenter_apps_attr paa
            on pa.sid=paa.sid and pa.appid=paa.appid and paa.dt = '%(ld_begin)s'

            left join stage.paycenter_site ps
            on pa.sid=ps.sid  and ps.dt = '%(ld_begin)s'

            left join stage.paycenter_game pg
            on pa.gameid=pg.gameid and pg.dt = '%(ld_begin)s'

            left join stage.paycenter_lang pl
            on pa.langid=pl.langid and pl.dt = '%(ld_begin)s'

            where pa.dt = '%(ld_begin)s'
            group by pa.appid ,pa.sid
        """ % self.hql_dict
        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)
        return result



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
    a = agg_paycenter_apps_dim(stat_date)
    a()
