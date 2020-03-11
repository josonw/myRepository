#coding=utf-8

sql_template= {
# 获取roi原始数据
'roi_month_game':"""
                 select fdate "fmonth",
                        fgame_id "fgame_id",
                        game_name_str "fgame",
                        fbelong_group "fbelong_group",
                        fdru_month "fdru_month",
                        max(cost_money) "fcost",
                        max(all_dip) "fdip"
                  from (select to_char(a.fsignup_month, 'yyyy-mm') fdate,
                               b.fgame_id,
                             d.fname game_name_str,
                             c.fbelong_group,
                             round(sum(case when fcost_money > 0 then fcost_money else fdsu_d * a.fprice end), 2) cost_money,
                             0 all_dip,
                             0 fdru_month
                        from analysis.marketing_roi_cost_fct a
                        join analysis.marketing_channel_pkg_info b
                          on a.fchannel_id = to_char(b.fid)
                        left join analysis.marketing_channel_trader_info c
                          on b.ftrader_id = c.fid
                        join analysis.marketing_game_info d
                          on b.fgame_id = d.fid
                       where b.fstate in (1, 2)
                         and a.fsignup_month >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'12 months'
                         and a.fsignup_month <= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'1 months'
                         and (b.fcoop_type like '%%%%CPA%%%%' or fcoop_type like '%%%%CPC%%%%')
                       group by to_char(a.fsignup_month, 'yyyy-mm'),
                                b.fgame_id,
                                d.fname,
                                c.fbelong_group
                      union all
                      select to_char(a.fsignup_month, 'yyyy-mm') fdate,
                             b.fgame_id,
                             d.fname game_name_str,
                             c.fbelong_group,
                             0 cost_money,
                             sum(fdip) all_dip,
                             fdru_month
                        from analysis.marketing_roi_retain_pay a
                        join analysis.marketing_channel_pkg_info b
                          on a.fchannel_id = to_char(b.fid)
                        left join analysis.marketing_channel_trader_info c
                          on b.ftrader_id = c.fid
                        join analysis.marketing_game_info d
                          on b.fgame_id = d.fid
                       where b.fstate in (1, 2)
                         and a.fsignup_month >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'12 months'
                         and a.fsignup_month <= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'1 months'
                         and (b.fcoop_type like '%%%%CPA%%%%' or fcoop_type like '%%%%CPC%%%%')
                       group by to_char(a.fsignup_month, 'yyyy-mm'),
                                b.fgame_id,
                                d.fname,
                                fdru_month,
                                c.fbelong_group) foo
               group by fdate, fgame_id, game_name_str, fdru_month, fbelong_group
              having max(all_dip) > 0
               order by fdru_month
    """,

'roi_month_channel':"""
                    select fsignup_month,
                           fchannel_id,
                           fgame_id,
                           fchannelname,
                           ftradername,
                           fbelong_group,
                           fgame,
                           fdru_month,
                           max(cost_money) fcost,
                           max(all_dip) fdip
                      from (select a.fsignup_month,
                                   a.fchannel_id,
                                   b.fgame_id,
                                   b.fname fchannelname,
                                   c.fname ftradername,
                                   c.fbelong_group,
                                   d.fname fgame,
                                   round(sum(case when fcost_money > 0 then fcost_money else fdsu_d * a.fprice end), 2) cost_money,
                                   0 all_dip,
                                   0 fdru_month
                              from analysis.marketing_roi_cost_fct a
                              join analysis.marketing_channel_pkg_info b
                                on a.fchannel_id = to_char(b.fid)
                               and b.fstate in (1, 2)
                               and (b.fcoop_type like '%%%%CPA%%%%' or fcoop_type like '%%%%CPC%%%%')
                              left join analysis.marketing_channel_trader_info c
                                on b.ftrader_id = c.fid
                              join analysis.marketing_game_info d
                                on b.fgame_id = d.fid
                                join analysis.predict_channel_mid e
                                    on a.fchannel_id = to_char(e.fchannel_id)
                                   and e.fcluster in (4, 6, 3)
                             where a.fsignup_month >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'12 months'
                     and a.fsignup_month <= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'1 months'
                             group by a.fsignup_month,
                                      a.fchannel_id,
                                      b.fgame_id,
                                      b.fname,
                                      c.fname,
                                      c.fbelong_group,
                                      d.fname
                            union all
                            select a.fsignup_month,
                                   a.fchannel_id,
                                   b.fgame_id,
                                   b.fname fchannelname,
                                   c.fname ftradername,
                                   c.fbelong_group,
                                   d.fname fgame,
                                   0 cost_money,
                                   sum(fdip) all_dip,
                                   fdru_month
                              from analysis.marketing_roi_retain_pay a
                              join analysis.marketing_channel_pkg_info b
                                on a.fchannel_id = to_char(b.fid)
                                and b.fstate in (1, 2)
                                and (b.fcoop_type like '%%%%CPA%%%%' or fcoop_type like '%%%%CPC%%%%')
                              left join analysis.marketing_channel_trader_info c
                                on b.ftrader_id = c.fid
                              join analysis.marketing_game_info d
                                on b.fgame_id = d.fid
                                join analysis.predict_channel_mid e
                                    on a.fchannel_id = to_char(e.fchannel_id)
                                   and e.fcluster in (4, 6, 3)
                             where a.fsignup_month >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'12 months'
                     and a.fsignup_month <= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') - interval'1 months'
                             group by a.fsignup_month,
                                      a.fchannel_id,
                                      b.fgame_id,
                                      b.fname,
                                      c.fname,
                                      c.fbelong_group,
                                      d.fname,
                                      fdru_month) foo
                    where ftradername != '收录包'
                     group by fsignup_month,
                              fchannel_id,
                              fgame_id,
                              fchannelname,
                              ftradername,
                              fbelong_group,
                              fgame,
                              fdru_month
                    having max(all_dip) > 0
                     order by fdru_month
""",
}