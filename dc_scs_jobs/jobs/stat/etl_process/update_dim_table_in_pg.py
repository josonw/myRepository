#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class update_dim_table_in_pg(BasePGStat):

    def stat(self):

        sql = """INSERT into  analysis.game_activity_rule_dim (fgamefsk,fact_id,frule_id,fdis_name)
        select   a.fgamefsk, a.fact_id, a.frule_id, a.frule_id fdis_name
        from analysis.game_activity_rule_fct a
        left outer join analysis.game_activity_rule_dim b
          on a.fgamefsk = b.fgamefsk
         and a.fact_id = b.fact_id
         and a.frule_id = b.frule_id
        where a.fdate = '%(ld_begin)s'
          and b.frule_id is null
        group by a.fgamefsk, a.fact_id, a.frule_id;
            COMMIT;

        """% self.sql_dict
        self.exe_hql(sql)

        sql = """INSERT into  analysis.game_activity_dim (fgamefsk,fact_id,fact_name,fdis_name)
        select   a.fgamefsk, a.fact_id, max(a.fact_name) fact_name, max(a.fact_name) fdis_name
        from analysis.game_activity_fct a
        left outer join analysis.game_activity_dim b
         on a.fgamefsk=b.fgamefsk
        and a.fact_id=b.fact_id
        where a.fdate = '%(ld_begin)s'
        and  b.fact_id is null
        group by a.fgamefsk, a.fact_id;
        COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """delete from dcnew.event_dim where fdate = '%(ld_begin)s';
                 insert into dcnew.event_dim (fdate,fgamefsk,fet_id,fet_name)
                 select '%(ld_begin)s' fdate,
                        fgamefsk,
                        fet_id,
                        fet_id
                   from (select distinct fgamefsk,fet_id
                           from dcnew.event a
                          where fdate = '%(ld_begin)s'
                            and fet_id is not null ) a
                  where not exists ( select 1
                                      from dcnew.event_dim be
                                      where be.fgamefsk = a.fgamefsk
                                        and be.fet_id = a.fet_id );
        COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """insert into dcnew.currencies_type_dim (fgamefsk,fcointype,fact_type,fact_id,fname,fdate_update)
                 select fgamefsk,
                        fcointype,
                        fact_type,
                        fact_id,
                        fact_id fname,
                        '%(ld_begin)s' fdate_update
                   from (select distinct fgamefsk,fcointype,fact_type,fact_id
                           from dcnew.currencies_detail a
                          where fdate = '%(ld_begin)s'
                            and fcointype is not null ) a
                  where not exists ( select 1
                                      from dcnew.currencies_type_dim be
                                      where be.fgamefsk = a.fgamefsk
                                        and be.fcointype = a.fcointype
                                        and be.fact_type = a.fact_type
                                        and be.fact_id = a.fact_id );
        COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """insert into dcnew.subgame_dim (fsubgamefsk,fsubgamename,fgamefsk,priority)
                 select fsubgamefsk,
                        fsubgamefsk,
                        fgamefsk,
                        0 priority
                   from (select distinct fsubgamefsk,fgamefsk
                           from dcnew.act_user a
                          where fdate = '%(ld_begin)s'
                            and fsubgamefsk <> -21379
                            and fsubgamefsk <> -13658) a
                  where not exists ( select 1
                                      from dcnew.subgame_dim be
                                      where be.fgamefsk = a.fgamefsk
                                        and be.fsubgamefsk = a.fsubgamefsk);
        COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = update_dim_table_in_pg(stat_date)
    a()
