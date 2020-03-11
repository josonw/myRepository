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


class load_dfqp_user_generate_order_stg(BaseStatModel):

    # ���ط����Ƶ��û��µ���ˮ�����������
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_generate_order_stg
        ( 	fbpid					varchar(50)  	comment 'BPID',
			fuid                    bigint       	comment '�û�UID',
			fplatform_uid           varchar(50)  	comment 'ƽ̨�û�id',
			fentrance_id            varchar(20)  	comment '�������',
			forder_at               string       	comment '����ʱ��',
			forder_id               varchar(100) 	comment '֧��������',
			fcurrency_type          smallint     	comment '��������',
			fcurrency_num           smallint     	comment '��������',
			fitem_category          smallint     	comment '������Ʒ���',
			fitem_id                bigint       	comment '������Ʒid',
			fitem_num               bigint       	comment '������Ʒ����',
			fbalance                bigint       	comment '�µ�ʱ�û������ж�����Ϸ��',
			fgrade                  int          	comment '�µ�ʱ�û���ǰ�ȼ�',
			fgameparty_pname        varchar(50)  	comment '�µ�ʱ�ƾ�һ������',
			fgameparty_subname      varchar(50)  	comment '�µ�ʱ�ƾֶ�������',
			fgameparty_anto         bigint       	comment '�µ�ʱ��ע��',
			fbankrupt               tinyint      	comment '�µ�ʱ�û��Ƿ����Ʋ�״̬',
			fpay_scene              varchar(100) 	comment '���ѳ���',
			fip                     varchar(100) 	comment '�û��µ�ip��ַ',
			fplatform_order_id      varchar(100) 	comment 'ƽ̨������ˮ��',
			fb_order_id             bigint       	comment 'ҵ���ڲ�������',
			ffee                    decimal(20,2)	comment '��������',
			fpm_name                varchar(100) 	comment '�µ�ʱ���õ�֧����ʽ����',
			fversion_info           varchar(50)  	comment '�µ�����Ϸ�汾��',
			fchannel_code           varchar(100) 	comment '��������ID',
			fgame_id                int          	comment '����ϷID',
			fpartner_info           varchar(32)  	comment '��������',
			fgameparty_gsubname     string       	comment '',
			fpay_scene_type         string       	comment '���ѳ�������',
			faward_type             string       	comment '��������',
			fparty_type             string       	comment '�µ�ʱ��������',
			fpay_scene_extra        string       	comment '���ѳ��������ֶ�',
			fmatch_rule_type        string          comment '��������',
			fmatch_rule_id          string          comment '��������id',
			fpay_scene_text         string          comment '�����ֶγ�������˵��',
			fpromoter_id            string          comment '�ƹ�Աid',
			fgamefsk				bigint			comment '��ϷID',
			fgamename				string			comment '��Ϸ����',
			fplatformfsk			bigint			comment 'ƽ̨ID',
			fplatformname			string			comment 'ƽ̨����',
			fhallfsk				bigint			comment '����ID',
			fhallname				string			comment '��������',
			fterminaltypefsk		bigint			comment '�ն�ID',
			fterminaltypename		string			comment '�ն�����',
			fversionfsk				bigint			comment '�汾ID',
			fversionname			string			comment '�汾����'
        ) comment '�ط������û��µ���ˮ'
        partitioned by(dt string comment '����')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_generate_order_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				fplatform_uid,
				fentrance_id,
				forder_at,
				forder_id,
				fcurrency_type,
				fcurrency_num,
				fitem_category,
				fitem_id,
				fitem_num,
				fbalance,
				fgrade,
				fgameparty_pname,
				fgameparty_subname,
				fgameparty_anto,
				fbankrupt,
				fpay_scene,
				fip,
				fplatform_order_id,
				fb_order_id,
				ffee,
				fpm_name,
				fversion_info,
				fchannel_code,
				fgame_id,
				fpartner_info,
				fgameparty_gsubname,
				fpay_scene_type,
				faward_type,
				fparty_type,
				fpay_scene_extra,
				fmatch_rule_type,
				fmatch_rule_id,
				fpay_scene_text,
				fpromoter_id,
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
        from stage.user_generate_order_stg t1
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
a = load_dfqp_user_generate_order_stg(sys.argv[1:])
a()
