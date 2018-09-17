#!/usr/bin/env python
# -*- coding=utf-8 -*-

import sys
import os
import time
from optparse import OptionParser
import datetime
import re
# import arrow

# _BEELINE_LOCATION = 'impala-shell'
# _IMPALA_SERVER_ADDR = 'node1.lb.bigdata.dmp.com'
# _IMPALA_DATABASE = 'prod'

_BEELINE_LOCATION = '/data/dmp/cloudera/parcels/CDH/bin/beeline'
_HIVE_SERVER_ADDR = 'node2.ikh.bigdata.dmp.com:10000'
DATE_FORMAT = "%Y%m%d"

if __name__ == '__main__':
    date_str = '20180714'
    while date_str != '20180310':
        date_str = (datetime.datetime.strptime(date_str, DATE_FORMAT) - datetime.timedelta(1)).strftime(DATE_FORMAT)
        # print date_str
        sqlContent = '''
        set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table prod.ads_hive_leopard_md_stats_1d partition (ds)
select a.ds as stat_date
      ,a.app_key
      ,a.module
      ,pv
      ,module_uv
      ,view_uv
      ,click
      ,click_uv
      ,use_time_ms
      ,use_time_uv
      ,channel_use_time
      ,a.ds as ds
  from
      (
          select a.ds as ds
                ,a.app_key as app_key
                ,a.module as module
                ,pv
                ,view_uv
                ,click
                ,click_uv
                ,use_time_ms
                ,use_time_uv
                ,channel_use_time
            from
              (
                  select ds
                        ,app_key
                        ,module
                        ,count( case when event_type = 'view' then 1 end ) as view_uv
                        ,count( case when event_type = 'click' then 1 end ) as click_uv
                        ,count( case when event_type = 'exit' then 1 end ) as use_time_uv
                    from
                      (
                          select ds
                                ,app_key
                                ,module
                                ,event_type
                                ,device_id
                            from
                              (
                                  select ds
                                        ,app_key
                                        ,device_id
                                        ,case when event_type in ('click','play') then 'click'
                                              else event_type
                                          end as event_type
                                        ,module
                                        ,case when event_type = 'exit' then cast(get_json_object(args, '$.use_time_ms') as bigint)
                                              else 0
                                          end as use_time_ms
                                    from prod.ods_hive_pv_event_1d
                                  where ds = '{yyyymmdd}'
                                      and event_type in ('view','click','play','exit')
                                      and content_id > '0'
                                      and event_id <> ''
                                      and device_id <> ''
                                      and app_key > 0
                                      and module > 0
                                  group by ds
                                          ,app_key
                                          ,device_id
                                          ,event_type
                                          ,module
                                          ,args
                              ) a
                          where use_time_ms < 600000
                          group by ds
                                  ,app_key
                                  ,module
                                  ,event_type
                                  ,device_id
                      ) a
                  group by ds
                          ,app_key
                          ,module
              ) a
          full join
              (
                  select ds
                        ,app_key
                        ,module
                        ,sum( case when event_type = 'view' then event_count else 0 end ) as pv
                        ,sum( case when event_type = 'click' then event_count else 0 end ) as click
                        ,sum( case when event_type = 'exit' then use_time_ms else 0 end ) as use_time_ms
                        ,sum( case when event_type = 'channel_exit' then channel_use_time_ms else 0 end ) as channel_use_time
                    from
                      (
                          select ds
                                ,app_key
                                ,module
                                ,event_type
                                ,count(*) as event_count
                                ,sum(channel_use_time_ms) as channel_use_time_ms
                                ,sum(use_time_ms) as use_time_ms
                            from
                              (
                                  select ds
                                        ,app_key
                                        ,device_id
                                        ,event_id
                                        ,case when event_type in ('click','play') then 'click'
                                              when event_type in ('channel_exit','circle_exit') then 'channel_exit'
                                              else event_type
                                          end as event_type
                                        ,module
                                        ,case when event_type = 'exit' then cast(get_json_object(args, '$.use_time_ms') as bigint)
                                              else 0
                                          end as use_time_ms
                                        ,case when event_type in('channel_exit','circle_exit') then cast(get_json_object(args, '$.use_time_ms') as bigint)
                                              else 0
                                          end as channel_use_time_ms
                                    from prod.ods_hive_pv_event_1d
                                  where ds = '{yyyymmdd}'
                                      and event_type in ('view','click','play','exit','channel_exit','circle_exit')
                                      and content_id > '0'
                                      and event_id <> ''
                                      and device_id <> ''
                                      and app_key > 0
                                      and module > 0
                                  group by ds
                                          ,app_key
                                          ,device_id
                                          ,event_id
                                          ,event_type
                                          ,module
                                          ,args
                              ) a
                          where channel_use_time_ms < 600000
                              and use_time_ms < 600000
                          group by ds
                                  ,app_key
                                  ,module
                                  ,event_type
                      ) a
                  group by ds
                          ,app_key
                          ,module
              ) b
            on a.ds = b.ds
              and a.app_key = b.app_key
              and a.module = b.module
      ) a
  right join
      (
          select sum(1) as module_uv
                ,module
                ,app_key
                ,ds
            from prod.dws_hive_log_user_module_uv_1d
           where ds = '{yyyymmdd}'
           group by module
                ,app_key
                ,ds
      ) b
    on a.ds = b.ds
   and a.app_key = b.app_key
   and a.module = b.module
;
        '''.format(yyyymmdd=date_str)
        print date_str


        ret = os.system(
            # _BEELINE_LOCATION + " -i " + _IMPALA_SERVER_ADDR + " -d " + _IMPALA_DATABASE + " -q \"" + sqlContent + "\"")
            _BEELINE_LOCATION + " -u jdbc:hive2://" + _HIVE_SERVER_ADDR + " -n impala --silent=true -e \"" + sqlContent + "\"")
        if ret == 0:
            print date_str + '处理完成'
        else:
            print date_str + '处理失败'

# /var/lib/impala/nohup.out
