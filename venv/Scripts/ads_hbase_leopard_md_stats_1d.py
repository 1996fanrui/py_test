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
    date_str = '20180722'
    while date_str != '20180717':
        date_str = (datetime.datetime.strptime(date_str, DATE_FORMAT) - datetime.timedelta(1)).strftime(DATE_FORMAT)
        # print date_str
        sqlContent = '''
        add jar hdfs://nameservice:8020/user/udf/dmp-function-lib.jar;
create temporary function md5 as 'suishen.hive.udf.MD5';
insert overwrite table prod.ads_hbase_leopard_md_stats_1d
select concat(substr(md5(concat(stat_date,'_',app_key,'_',module)),1,4),'_',stat_date,'_',app_key,'_',module) as key
      ,stat_date
      ,app_key
      ,module
      ,pv
      ,module_uv
      ,view_uv
      ,click
      ,click_uv
      ,use_time_ms
      ,use_time_uv
      ,channel_use_time
  from
      (
        select stat_date
              ,app_key
              ,module
              ,pv
              ,module_uv
              ,view_uv
              ,click
              ,click_uv
              ,use_time_ms
              ,use_time_uv
              ,channel_use_time
          from prod.ads_hive_leopard_md_stats_1d
         where ds = '{yyyymmdd}'
               and app_key > 0
               and module > 0
      ) a
  distribute by rand()
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

