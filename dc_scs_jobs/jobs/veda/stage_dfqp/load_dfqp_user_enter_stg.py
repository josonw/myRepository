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


class load_dfqp_user_enter_stg(BaseStatModel):

    # ���ط����Ƶ��û���������Ϸ��ˮ�����������
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_enter_stg
        ( 	fbpid				varchar(50) 	comment 'BPID',
			fuid                bigint          comment '�û�UID',
			fplatform_uid       varchar(50)     comment '��Ϸƽ̨uid',
			flts_at             string          comment '����ʱ��',
			fis_first           tinyint         comment '�Ƿ���ʷ�״ν��������Ϸ',
			fgame_id            int             comment '����ϷID',
			fchannel_code       varchar(100)    comment '��������ID',
			fip                 varchar(64)     comment '�û�IP��ַ',
			fentrance_id        bigint          comment '�ƶ��������',
			fversion_info       varchar(50)     comment '�汾��',
			fad_code            varchar(50)     comment '��漤��ID',
			user_gamecoins      bigint          comment '�û����������ϷʱЯ������Ϸ������',
			flang               varchar(64)     comment '�û����������Ϸʱʹ�õ�����',
			fm_dtype            varchar(100)    comment '�ն��豸�ͺ�/�ֻ�����',
			fm_pixel            varchar(100)    comment '�ֻ�������С',
			fm_imei             varchar(100)    comment '�ֻ��豸��',
			fm_os               varchar(100)    comment '�ֻ��豸/�ն˲���ϵͳ',
			fm_network          varchar(100)    comment '�ֻ��豸���뷽ʽ',
			fm_operator         varchar(100)    comment '������Ӫ��',
			fsource_path        varchar(100)    comment '��Դ·��',
			fmobilesms          string          comment '�û��ֻ���',
			fpname              string          comment '����������',
			fsubname            string          comment '����������',
			fgsubname           string          comment '����������',
			fgamefsk			bigint			comment '��ϷID',
			fgamename			string			comment '��Ϸ����',
			fplatformfsk		bigint			comment 'ƽ̨ID',
			fplatformname		string			comment 'ƽ̨����',
			fhallfsk			bigint			comment '����ID',
			fhallname			string			comment '��������',
			fterminaltypefsk	bigint			comment '�ն�ID',
			fterminaltypename	string			comment '�ն�����',
			fversionfsk			bigint			comment '�汾ID',
			fversionname		string			comment '�汾����'

        ) comment '�ط������û���������Ϸ��ˮ'
        partitioned by(dt string comment '����')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_enter_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				fplatform_uid,
				flts_at,
				fis_first,
				fgame_id,
				fchannel_code,
				fip,
				fentrance_id,
				fversion_info,
				fad_code,
				user_gamecoins,
				flang,
				fm_dtype,
				fm_pixel,
				fm_imei,
				fm_os,
				fm_network,
				fm_operator,
				fsource_path,
				fmobilesms,
				fpname,
				fsubname,
				fgsubname,
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
        from stage.user_enter_stg t1
        join dim.bpid_map tt
          on t1.fbpid = tt.fbpid
         and fgamefsk = 4132314431
        where dt = '%(statdate)s'
        distribute by t1.fbpid, fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# ����ͳ��ʵ��
a = load_dfqp_user_enter_stg(sys.argv[1:])
a()
