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
    table_name = 'dws_hive_user_daily_active_v2_1d'

    date_run = datetime.date(2018,8,1)

    while date_run.strftime(DATE_FORMAT) != '20160601':
        pre_month = date_run - datetime.timedelta(1)
        end_date = date_run.strftime(DATE_FORMAT)
        date_run = datetime.date(pre_month.year, pre_month.month, 1)
        start_date = date_run.strftime(DATE_FORMAT)

        sqlContent = '''
      from prod.{table_name}
        and ds >= '{start_date}'
        and ds < '{end_date}'
        
        '''.format(table_name=table_name + start_date, start_date=start_date, end_date=end_date)
        print sqlContent

#     print table_name + '正在处理。。。'
#     ret = os.system(
#         # _BEELINE_LOCATION + " -i " + _IMPALA_SERVER_ADDR + " -d " + _IMPALA_DATABASE + " -q \"" + sqlContent + "\"")
#         _BEELINE_LOCATION + " -u jdbc:hive2://" + _HIVE_SERVER_ADDR + " -n impala --silent=true -e \"" + sqlContent + "\"")
#     if ret == 0:
#         print table_name + '成功加载到test中间表'
#
#         distcp_context = '''
# hadoop distcp -skipcrccheck -update -m 30 hdfs://nameservice:8020/user/hive/warehouse/test.db/{table_name} hdfs://nn1.hadoop.wnl.dmp.com:8020/user/hive/warehouse/test.db/{table_name}/
#         '''.format(table_name=table_name)
#
#         ret = os.system(distcp_context)
#         if ret == 0:
#             print table_name + 'distcp成功'
#         else:
#             print table_name + 'distcp失败'
#     else:
#         print table_name + '加载到test中间表时失败'

