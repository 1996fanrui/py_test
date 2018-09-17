#!/usr/bin/env python
# -*- coding=utf-8 -*-

import sys
import os
import time
from optparse import OptionParser
import datetime
import logging

# -- 1、新集群建立hbase表(dn节点执行)（手动提前执行）
# create 'hbase_ads_dmp_mult_module_item_pv_1d',{NAME=>'T', VERSIONS=>1, BLOCKCACHE=>true, BLOOMFILTER=>'ROW', COMPRESSION => 'snappy', TTL => '15552000'},{SPLITS => ['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f']}
tableName = 'hbase_ads_dmp_mult_module_item_pv_1d'
# -- 2、新集群上建立一个hive中间表
tmpHiveDDL = '''
create table if not exists test.tmp_hive_{tableName} (
     key         String
    ,stat_date   String
    ,pv          bigint
    ,uv          bigint
    ,clk         bigint
    ,clk_uv      bigint
) stored as orc
;
'''.format(tableName = tableName)

print tmpHiveDDL


hbaseDDL = '''
     key         String
    ,stat_date   String
    ,pv          bigint
    ,uv          bigint
    ,clk         bigint
    ,clk_uv      bigint
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties (
    "hbase.table.name"="hbase_ads_dmp_mult_module_item_pv_1d",
    "hbase.columns.mapping" = ":key,T:stat_date,T:pv,T:uv,T:clk,T:clk_uv"
)
;
'''

# -- 3、新集群上prod建立一个新集群hbase的外部表
prodHbaseDDL = '''
create external table prod.{tableName} ( 
'''.format(tableName = tableName) + hbaseDDL


print prodHbaseDDL

# -- 4、新集群上建立一个老集群hbase的外部表
oldHbaseDDL = '''
set hbase.zookeeper.quorum=10.9.186.166:2181,10.9.189.141:2181,10.9.162.210:2181;
create external table test.{tableName} ( 
'''.format(tableName = tableName) + hbaseDDL


print oldHbaseDDL


# -- 5、数据从hbase外部表导入到hive中间表
hbaseToHive = '''
insert overwrite table test.tmp_hive_{tableName} 
select *
from test.{tableName}
;
'''.format(tableName = tableName)

print hbaseToHive

# -- 6、中间表 到 prod外部结果表
hiveToHbase = '''
insert overwrite table prod.{tableName}
select *
from test.tmp_hive_{tableName} 
;
'''.format(tableName = tableName)

print hiveToHbase
