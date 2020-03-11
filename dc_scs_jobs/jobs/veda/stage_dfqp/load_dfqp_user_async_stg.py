#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserAsyncStg(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS stage_dfqp.user_async_stg(
             fbpid varchar(50) COMMENT 'BPID',
             fuid bigint COMMENT '用户ID',
             fsync_at string COMMENT '同步时间戳',
             fsync_action int COMMENT '触发同步的动作：1登入，2注册(创建用户)，3退出游戏，4牌局，5刷新用户信息，-1表示未定义',
             fname varchar(200) COMMENT '用户真实姓名',
             fdisplay_name varchar(400) COMMENT '用户昵称',
             fgender int COMMENT '性别：0女，1男，-1表示未定义',
             fage int COMMENT '用户年龄',
             flanguage varchar(200) COMMENT '语种/语言',
             fcountry varchar(200) COMMENT '国家',
             fcity varchar(200) COMMENT '城市',
             ffriends_num bigint COMMENT '平台好友数量',
             fappfriends_num bigint COMMENT '加入游戏的好友数量',
             fprofession int COMMENT '职业',
             fbycoin_num bigint COMMENT '此时用户身上博雅币数量',
             fgamecoin_num bigint COMMENT '此时用户身上游戏币数量',
             fgoods_num bigint COMMENT '用户当前道具数量',
             fpoint_num bigint COMMENT '用户当前积分',
             finterest varchar(100) COMMENT '兴趣',
             feducation varchar(100) COMMENT '学历',
             fbloodtype varchar(100) COMMENT '血型A/B/AB/O',
             fplatform_uid varchar(50) COMMENT '用户平台ID',
             fip varchar(128) COMMENT '最后一次登录IP',
             fphone varchar(64) COMMENT '用户手机号',
             fimsi varchar(64) COMMENT '用户IMSI号',
             fimei varchar(64) COMMENT '设备IMEI号',
             ficcid varchar(64) COMMENT '用户对应的ICCID号',
             fmac varchar(64) COMMENT '用户设备MAC地址',
             flevel int COMMENT '游戏等级',
             fsignup_at string COMMENT '注册时间',
             femail varchar(128) COMMENT '电子邮箱',
             flatitude varchar(32) COMMENT 'IP所在地纬度',
             flongitude varchar(32) COMMENT 'IP所在地经度',
             fstatus int COMMENT '账号状态：0正常，1临时封停，2永久封停',
             fapp_id string COMMENT '应用ID',
             flogin_time string COMMENT '最后登录时间',
             fvip_time string COMMENT 'VIP过期时间',
             fvip_level int COMMENT 'VIP级别',
             fplatform_type int COMMENT '平台类型',
             ficon string COMMENT '头像(缩略图)',
             ficon_big string COMMENT '头像(大)',
             fhometown string COMMENT '家乡',
             fidcard string COMMENT '身份证号码',
             forg_app string COMMENT '原始应用ID',
             forg_channel string COMMENT '原始渠道ID',
             fchannel_id string COMMENT '当前渠道ID',
             ftimeout int COMMENT 'server需求字段',
             fbagvol int COMMENT '背包容量',
             fispay int COMMENT '是否充值',
             ffirst_match int COMMENT '是否首次参加比赛',
             ffast_match int COMMENT '是否首次参加比赛',
             fversion string COMMENT '最后版本',
             fis_set int COMMENT '是否设置过用户资料',
             fpartner_info string COMMENT '合作属性',
             fidentity int COMMENT '用户身份',
             fcid string COMMENT '用户CID',
             fsuperior_agent_id string COMMENT '上级代理商ID',
             fsuperior_promoter_id string COMMENT '上级推广员ID',
             fgamefsk bigint,
             fgamename string,
             fplatformfsk bigint,
             fplatformname string,
             fhallfsk bigint,
             fhallname string,
             fterminaltypefsk bigint,
             fterminaltypename string,
             fversionfsk bigint,
             fversionname string)
            COMMENT '地方棋牌用户信息同步/更新'
            PARTITIONED BY (dt string COMMENT '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table stage_dfqp.user_async_stg partition (dt='%(statdate)s')
            select distinct t2.fbpid,
                   case
                     when t1.fplatformfsk = 77000221 and t2.fuid < 1000000000 then
                      t2.fuid + 1000000000
                     else
                      t2.fuid
                   end as fuid,
                   t2.fsync_at,
                   t2.fsync_action,
                   t2.fname,
                   t2.fdisplay_name,
                   t2.fgender,
                   t2.fage,
                   t2.flanguage,
                   t2.fcountry,
                   t2.fcity,
                   t2.ffriends_num,
                   t2.fappfriends_num,
                   t2.fprofession,
                   t2.fbycoin_num,
                   t2.fgamecoin_num,
                   t2.fgoods_num,
                   t2.fpoint_num,
                   t2.finterest,
                   t2.feducation,
                   t2.fbloodtype,
                   t2.fplatform_uid,
                   t2.fip,
                   t2.fphone,
                   t2.fimsi,
                   t2.fimei,
                   t2.ficcid,
                   t2.fmac,
                   t2.flevel,
                   t2.fsignup_at,
                   t2.femail,
                   t2.flatitude,
                   t2.flongitude,
                   t2.fstatus,
                   t2.fapp_id,
                   t2.flogin_time,
                   t2.fvip_time,
                   t2.fvip_level,
                   t2.fplatform_type,
                   t2.ficon,
                   t2.ficon_big,
                   t2.fhometown,
                   t2.fidcard,
                   t2.forg_app,
                   t2.forg_channel,
                   t2.fchannel_id,
                   t2.ftimeout,
                   t2.fbagvol,
                   t2.fispay,
                   t2.ffirst_match,
                   t2.ffast_match,
                   t2.fversion,
                   t2.fis_set,
                   t2.fpartner_info,
                   t2.fidentity,
                   t2.fcid,
                   t2.fsuperior_agent_id,
                   t2.fsuperior_promoter_id,
                   t1.fgamefsk,
                   t1.fgamename,
                   t1.fplatformfsk,
                   t1.fplatformname,
                   t1.fhallfsk,
                   t1.fhallname,
                   t1.fterminaltypefsk,
                   t1.fterminaltypename,
                   t1.fversionfsk,
                   t1.fversionname
              from dim.bpid_map_bud t1
             inner join stage.user_async_stg t2
                on (t1.fbpid = t2.fbpid and t1.fgamefsk = 4132314431)
             where t2.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            with nickname_info as --昵称信息
             (select fcid, fdisplay_name
                from (select fcid,
                             fdisplay_name,
                             row_number() over(partition by fcid order by fsync_at desc) as ranking
                        from stage_dfqp.user_async_stg
                       where fdisplay_name is not null
                         and dt = '%(statdate)s') m1
               where ranking = 1),
            realname_info as --实名信息
             (select fcid, fidcard, fname
                from (select fcid,
                             fidcard,
                             fname,
                             row_number() over(partition by fcid order by fsync_at desc) as ranking
                        from stage_dfqp.user_async_stg
                       where length(fidcard) in (15, 18)
                         and dt = '%(statdate)s') m2
               where ranking = 1),
            longitude_info as --经纬度信息
             (select fuid, flatitude, flongitude
                from (select fuid,
                             flatitude,
                             flongitude,
                             row_number() over(partition by fuid order by fsync_at desc) as ranking
                        from stage_dfqp.user_async_stg
                       where flongitude is not null
                         and dt = '%(statdate)s') m3
               where ranking = 1),
            sync_basic_info as --冗余记录整合
             (select fbpid,
                     fuid,
                     fsync_at,
                     fsync_action,
                     fgender,
                     fage,
                     flanguage,
                     fcountry,
                     fcity,
                     ffriends_num,
                     fappfriends_num,
                     fprofession,
                     fbycoin_num,
                     fgamecoin_num,
                     fgoods_num,
                     fpoint_num,
                     finterest,
                     feducation,
                     fbloodtype,
                     fplatform_uid,
                     fip,
                     fphone,
                     fimsi,
                     fimei,
                     ficcid,
                     fmac,
                     flevel,
                     fsignup_at,
                     femail,
                     fstatus,
                     fapp_id,
                     flogin_time,
                     fvip_time,
                     fvip_level,
                     fplatform_type,
                     ficon,
                     ficon_big,
                     fhometown,
                     forg_app,
                     forg_channel,
                     fchannel_id,
                     ftimeout,
                     fbagvol,
                     fispay,
                     ffirst_match,
                     ffast_match,
                     fversion,
                     fis_set,
                     fpartner_info,
                     fidentity,
                     fcid,
                     fsuperior_agent_id,
                     fsuperior_promoter_id,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from (select fbpid,
                             fuid,
                             fsync_at,
                             fsync_action,
                             fgender,
                             fage,
                             flanguage,
                             fcountry,
                             fcity,
                             ffriends_num,
                             fappfriends_num,
                             fprofession,
                             fbycoin_num,
                             fgamecoin_num,
                             fgoods_num,
                             fpoint_num,
                             finterest,
                             feducation,
                             fbloodtype,
                             fplatform_uid,
                             fip,
                             fphone,
                             fimsi,
                             fimei,
                             ficcid,
                             fmac,
                             flevel,
                             fsignup_at,
                             femail,
                             fstatus,
                             fapp_id,
                             flogin_time,
                             fvip_time,
                             fvip_level,
                             fplatform_type,
                             ficon,
                             ficon_big,
                             fhometown,
                             forg_app,
                             forg_channel,
                             fchannel_id,
                             ftimeout,
                             fbagvol,
                             fispay,
                             ffirst_match,
                             ffast_match,
                             fversion,
                             fis_set,
                             fpartner_info,
                             fidentity,
                             fcid,
                             fsuperior_agent_id,
                             fsuperior_promoter_id,
                             fgamefsk,
                             fgamename,
                             fplatformfsk,
                             fplatformname,
                             fhallfsk,
                             fhallname,
                             fterminaltypefsk,
                             fterminaltypename,
                             fversionfsk,
                             fversionname,
                             row_number() over(partition by fuid order by fsync_at desc) as ranking
                        from stage_dfqp.user_async_stg
                       where flongitude is null
                         and dt = '%(statdate)s') m4
               where ranking = 1)
            --当天上报信息汇总整理
            insert overwrite table stage_dfqp.user_async_stg partition (dt='%(statdate)s')
            select fbpid,
                   v1.fuid,
                   fsync_at,
                   fsync_action,
                   fname,
                   fdisplay_name,
                   fgender,
                   fage,
                   flanguage,
                   fcountry,
                   fcity,
                   ffriends_num,
                   fappfriends_num,
                   fprofession,
                   fbycoin_num,
                   fgamecoin_num,
                   fgoods_num,
                   fpoint_num,
                   finterest,
                   feducation,
                   fbloodtype,
                   fplatform_uid,
                   fip,
                   fphone,
                   fimsi,
                   fimei,
                   ficcid,
                   fmac,
                   flevel,
                   fsignup_at,
                   femail,
                   flatitude,
                   flongitude,
                   fstatus,
                   fapp_id,
                   flogin_time,
                   fvip_time,
                   fvip_level,
                   fplatform_type,
                   ficon,
                   ficon_big,
                   fhometown,
                   fidcard,
                   forg_app,
                   forg_channel,
                   fchannel_id,
                   ftimeout,
                   fbagvol,
                   fispay,
                   ffirst_match,
                   ffast_match,
                   fversion,
                   fis_set,
                   fpartner_info,
                   fidentity,
                   v1.fcid,
                   fsuperior_agent_id,
                   fsuperior_promoter_id,
                   fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fhallfsk,
                   fhallname,
                   fterminaltypefsk,
                   fterminaltypename,
                   fversionfsk,
                   fversionname
              from sync_basic_info v1
              left join nickname_info v2
                on v1.fcid = v2.fcid
              left join realname_info v3
                on v1.fcid = v3.fcid
              left join longitude_info v4
                on v1.fuid = v4.fuid
             order by v1.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserAsyncStg(sys.argv[1:])
a()