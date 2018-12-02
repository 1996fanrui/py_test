# coding:utf8
import time
import datetime
import os
import sys
import arrow as arrow
import logging
from optparse import OptionParser
import aerospike
from aerospike import exception as ex
import hashlib


sys.path.append("/usr/lib64/python2.7/site-packages")


parser = OptionParser()
parser.add_option("-d", help='date format yyyymmdd')
(options, args) = parser.parse_args()
yyyymmdd = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
today = (datetime.date.today()).strftime('%Y%m%d')
if options.d is not None:
    yyyymmdd = options.d

# namespace = 'dmp'
# sets = 'aero_data_api_item_id_stat_nd'
namespace = 'ads'
sets = 'liyue_dsp_statistics'

local_config = {'hosts': [('192.168.30.216', 3000)]}
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
config = online_config

def intToBytes(buffer,n):
    tmp = []
    for i in range(4):
        tmp.insert(0,n & 0xFF)
        n >>= 8

    for value in tmp:
        buffer.append(value)

def longToBytes(buffer,n):
    tmp = []
    for i in range(8):
        tmp.insert(0,n & 0xFF)
        n >>= 8

    for value in tmp:
        buffer.append(value)

def allToBytes(ptId,md,contentId):
    b = bytearray([])  # init
    intToBytes(b,int(ptId))
    intToBytes(b,int(md))
    longToBytes(b,long(contentId))
    return b

def getRowkey(type, date, id):
    b = bytearray([])  # init
    intToBytes(b, int(type))
    intToBytes(b, int(date))
    if int(type) > 0:
        longToBytes(b, long(id))
    return b

# try:
#     client = aerospike.client(config).connect()
#     rowkey = allToBytes(1,12,297940423)
#     key = (namespace, sets, rowkey)
#     (key, meta, bins) = client.get(key)
#     print(key)
#     print('--------------------------')
#     print(meta)
#     print('--------------------------')
#     print(bins)
# except ex.RecordNotFound:
#     print("Record not found:", key)
# except ex.AerospikeError as e:
#     print("Error: {0} [{1}]".format(e.msg, e.code))
#     sys.exit(1)
# finally:
#     client.close()


try:
    client = aerospike.client(config).connect()
    rowkey = getRowkey(1,20180918,80000028)
    key = (namespace, sets, rowkey)
    print key
    (key, meta, bins) = client.get(key)
    print(key)
    print('--------------------------')
    print(meta)
    print('--------------------------')
    print(bins)
except ex.RecordNotFound:
    print("Record not found:", key)
except ex.AerospikeError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)
finally:
    client.close()

print('done')
