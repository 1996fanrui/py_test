#!/usr/bin/env python
# -*- coding=utf-8 -*-

import sys
import os
import time
from optparse import OptionParser
import datetime
import re
# import arrow

_BEELINE_LOCATION = 'impala-shell'
_IMPALA_SERVER_ADDR = 'node1.lb.bigdata.dmp.com'
_IMPALA_DATABASE = 'prod'
DATE_FORMAT = "%Y%m%d"

if __name__ == '__main__':
    date_str = '20180311'
    while date_str != '20180210':
        date_str = (datetime.datetime.strptime(date_str, DATE_FORMAT) - datetime.timedelta(1)).strftime(DATE_FORMAT)
        # print date_str
        sqlContent = '''insert overwrite table prod.dws_hive_log_user_module_uv_1d partition (ds)
    select device_id
          ,module
          ,app_key
          ,ds
      from prod.ods_hive_pv_event_1d
     where ds = '%s'
           and device_id <> ''
           and module > 0
     group by device_id
          ,module
          ,app_key
          ,ds
    ;
        ''' % (date_str)
        print sqlContent


        ret = os.system(
            _BEELINE_LOCATION + " -i " + _IMPALA_SERVER_ADDR + " -d " + _IMPALA_DATABASE + " -q \"" + sqlContent + "\"")



