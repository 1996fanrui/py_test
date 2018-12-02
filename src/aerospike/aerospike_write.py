# coding:utf8
# import time
# import datetime
# import os
# import sys
# import arrow as arrow
# import logging
# from optparse import OptionParser
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import HiveContext
import struct
import aerospike
from aerospike import exception as ex

# import hashlib

local_config = {
    'hosts': [
        ('192.168.30.216', 3000)
    ],
    'policies': {
        'timeout': 10000  # milliseconds
    }
}

online_config = {
    'hosts': [
        ('node5.aerospike.bigdata.wl.com', 3000),
        ('node6.aerospike.bigdata.wl.com', 3000),
        ('node7.aerospike.bigdata.wl.com', 3000),
        ('node8.aerospike.bigdata.wl.com', 3000),
    ],
    'policies': {
        'timeout': 10000  # milliseconds
    }
}
config = local_config


def intToBytes(buffer, n):
    tmp = []
    for i in range(4):
        tmp.insert(0, n & 0xFF)
        n >>= 8

    for value in tmp:
        buffer.append(value)


def longToBytes(buffer, n):
    tmp = []
    for i in range(8):
        tmp.insert(0, n & 0xFF)
        n >>= 8

    for value in tmp:
        buffer.append(value)


def allToBytes(ptId, appkey, md, contentId, date):
    b = bytearray([])  # init
    intToBytes(b, int(ptId))
    intToBytes(b, int(appkey))
    intToBytes(b, int(md))
    longToBytes(b, long(contentId))
    intToBytes(b, int(date))
    return b


rowkey = allToBytes('1', '888888', '1', '123456', '20180101')

try:
    client = aerospike.client(config).connect()
    key = ('test', 'my_test', rowkey)
    bins = {
        'p': 8
    }
    meta = {'ttl': 10}
    policy = {
        'exists': aerospike.POLICY_EXISTS_UPDATE,
        'commit_level ': aerospike.POLICY_COMMIT_LEVEL_MASTER,
        'key': aerospike.POLICY_KEY_SEND,
        'max_retries': 10,
        'sleep_between_retries': 100
    }
    client.put(key, bins, meta, policy)

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)

print 'done'

