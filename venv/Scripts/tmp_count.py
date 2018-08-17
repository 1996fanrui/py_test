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
    date_str = '20180724'
    while date_str != '20180310':
        date_str = (datetime.datetime.strptime(date_str, DATE_FORMAT) - datetime.timedelta(1)).strftime(DATE_FORMAT)
        today_str = (datetime.datetime.strptime(date_str, DATE_FORMAT) + datetime.timedelta(1)).strftime(DATE_FORMAT)
        print 'date_str:' + date_str
        print 'today_str:' + today_str
        sqlContent = '''
add jar hdfs://nameservice:8020/user/udf/mongo-hadoop-hive-1.5.1.jar;
add jar hdfs://nameservice:8020/user/udf/mongo-hadoop-core-1.5.1.jar;
add jar hdfs://nameservice:8020/user/udf/mongo-java-driver-3.2.2.jar;
insert into table test.tmp_count 
select a.ds 
      ,active_invite_count
      ,active_attention_count
  from 
    (
        select count(*) as active_invite_count
              ,ds 
          from 
            (
                select a.active_uid as active_invite_uid 
                      ,ds 
                  from 
                    (
                        select uid as active_uid 
                              ,ds 
                          from prod.ods_hive_active_user_1h 
                        where ds = '{yyyymmdd}' 
                        group by uid 
                                ,ds 
                    ) a 
                join 
                    (
                        select invite_uid 
                          from 
                            (
                                select masterId as invite_uid
                                  from business.ods_dim_mongodb_wltt_invite_relation_item
                                where createTime < unix_timestamp( '{today}','yyyyMMdd' )*1000
                                union all 
                                select servantId as invite_uid
                                  from business.ods_dim_mongodb_wltt_invite_relation_item
                                where createTime < unix_timestamp( '{today}','yyyyMMdd' )*1000
                            ) a 
                        group by invite_uid
                    ) b 
                on a.active_uid = b.invite_uid 
            ) a 
        group by ds 
    ) a
left outer join 
    (
        select count(*) as active_attention_count
              ,ds
          from 
            (
                select a.active_uid as active_attention_uid 
                      ,ds 
                  from 
                    (
                        select uid as active_uid 
                              ,ds 
                          from prod.ods_hive_active_user_1h 
                        where ds = '{yyyymmdd}' 
                            and app_key in (91988061, 91988062)
                        group by uid 
                                ,ds 
                    ) a 
                join 
                    (
                        select ssy_uid as attention_uid -- 有关注行为的uid
                          from 
                            (
                                select attention_lizhi_uid -- 有关注行为的lizhi_uid
                                  from 
                                    (
                                        select uid as attention_lizhi_uid
                                          from business.ods_dim_weili_user_relation
                                        where status = 1 
                                            and update_time < unix_timestamp( '{today}','yyyyMMdd' )*1000
                                        union all 
                                        select attention_uid as attention_lizhi_uid
                                          from business.ods_dim_weili_user_relation
                                        where status = 1 
                                            and update_time < unix_timestamp( '{today}','yyyyMMdd' )*1000
                                    ) a 
                                group by attention_lizhi_uid                        
                            ) a 
                        left outer join 
                            (
                                select uid 
                                      ,ssy_uid 
                                  from business.ods_dim_weili_community_uid_devid_relation
                            ) b 
                          on a.attention_lizhi_uid = b.uid 
                    ) b 
                on a.active_uid = b.attention_uid 
            ) a 
        group by ds 
    ) c 
  on a.ds = c.ds 
;
        '''.format(yyyymmdd=date_str,today=today_str)


        # ret = os.system(
        #     # _BEELINE_LOCATION + " -i " + _IMPALA_SERVER_ADDR + " -d " + _IMPALA_DATABASE + " -q \"" + sqlContent + "\"")
        #     _BEELINE_LOCATION + " -u jdbc:hive2://" + _HIVE_SERVER_ADDR + " -n impala --silent=true -e \"" + sqlContent + "\"")
        # if ret == 0:
        #     print date_str + '处理完成'
        # else:
        #     print date_str + '处理失败'
