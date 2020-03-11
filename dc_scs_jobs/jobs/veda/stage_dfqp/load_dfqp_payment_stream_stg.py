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


class load_dfqp_payment_stream_stg(BaseStatModel):

    # ���ط����Ƶ��û��ɹ�����״̬������ˮ�����������
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.payment_stream_stg
        ( 	fbpid					varchar(50)  	comment 'BPID',
			fdate                   string          comment '����ʱ��',
			fplatform_uid           varchar(50)     comment 'ƽ̨uid',
			fis_platform_uid        tinyint         comment '�Ƿ���ƽ̨uid',
			forder_id               varchar(255)    comment '����id',
			fcoins_num              decimal(20,2)   comment 'ԭ�Ҷ��',
			frate                   decimal(20,7)   comment '��Ԫ��ԭ�һ���',
			fm_id                   varchar(256)    comment '֧������id',
			fm_name                 varchar(256)    comment '֧����������',
			fp_id                   varchar(256)    comment '��Ʒid',
			fp_name                 varchar(256)    comment '��Ʒ����',
			fchannel_id             varchar(64)     comment '�û�����id',
			fimei                   varchar(64)     comment '�豸��',
			fsucc_time              string          comment '�ɹ�ʱ��',
			fcallback_time          string          comment '�ص�ʱ��',
			fp_type                 int             comment '��Ʒ����',
			fp_num                  bigint          comment '��Ʒ����',
			fsid                    int             comment 'ƽ̨ID',
			fappid                  int             comment 'Ӧ��ID',
			fpmode                  int             comment '����ID',
			fuid                    bigint          comment '�û�UID',
			fpamount_usd            decimal(20,2)   comment '������',
			fproduct_id             varchar(64)     comment 'ҵ���ϱ�����ƷID',
			fproduct_name           varchar(64)     comment 'ҵ���ϱ�����Ʒ����',
			fip                     varchar(64)     comment '����IP',
			fcid                    string          comment '֧��ʱ���û�cid',
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
        ) comment '�ط������û��ɹ�����״̬������ˮ'
        partitioned by(dt string comment '����')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.payment_stream_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				fdate,
				fplatform_uid,
				fis_platform_uid,
				forder_id,
				fcoins_num,
				frate,
				fm_id,
				fm_name,
				fp_id,
				fp_name,
				fchannel_id,
				fimei,
				fsucc_time,
				fcallback_time,
				fp_type,
				fp_num,
				fsid,
				fappid,
				fpmode,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				fpamount_usd,
				fproduct_id,
				fproduct_name,
				fip,
				fcid,
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
        from stage.payment_stream_stg t1
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
a = load_dfqp_payment_stream_stg(sys.argv[1:])
a()
