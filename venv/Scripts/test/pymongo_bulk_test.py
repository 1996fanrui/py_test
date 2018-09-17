# coding:utf8
import time
import os
import sys
import pymongo
import arrow as arrow
import logging
from optparse import OptionParser
from pymongo import UpdateOne
from pymongo import UpdateMany
from pymongo import InsertOne
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext


# os.environ['SPARK_HOME'] = "/data/dmp/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/spark"
# sys.path.append("/data/dmp/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/spark/python")
# sys.path.append("/data/dmp/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/spark/python/lib/py4j-0.9-src.zip")
# # os.environ['SPARK_HOME'] = "/data/dmp/cloudera/parcels/CDH/lib/spark"
#
#
# parser = OptionParser()
# parser.add_option("-d", help='date format yyyymmdd')
# (options, args) = parser.parse_args()
# biz_date = time.strftime('%Y%m%d', time.localtime(time.time()))
# if options.d is not None:
#     biz_date = options.d
#
# APP_NAME = 'APP_TEST'
#
# conf = (SparkConf()
#          .setAppName(APP_NAME))
# conf.set("spark.sql.crossJoin.enabled", "true")
# sc = SparkContext(conf=conf)
# # sc = SparkContext(appName=APP_NAME)
# sc.setLogLevel('WARN')
#
# sqlContext = HiveContext(sc)

local_conf_dict = {
    'MONGO_HOST': '192.168.30.121'
    ,'MONGO_PORT': 27017
    ,'MONGO_DB': 'suishen_lizhi'
    ,'MONGO_USER': 'dmp'
    ,'MONGO_PASSWD': 'dmp123456'
    ,'MONGO_COLL': 'wlkk_same_account_more_devices'
}

online_conf_dict = {
    'MONGO_HOST': '10.9.170.6'
    ,'MONGO_PORT': 27017
    ,'MONGO_DB': 'suishen_lizhi'
    ,'MONGO_USER': 'dmp'
    ,'MONGO_PASSWD': 'dmp123456'
    ,'MONGO_COLL': 'wlkk_same_account_more_devices'
}

conf_dict = online_conf_dict

def upsert_oper_many(opers):
    """
    upsert_many 文档集合
    :param opers:
    """
    client = None
    try:
        client = pymongo.MongoClient(conf_dict['MONGO_HOST'], conf_dict['MONGO_PORT'])
        db = client[conf_dict['MONGO_DB']]
        if conf_dict['MONGO_USER'] and conf_dict['MONGO_PASSWD']:
            db.authenticate(conf_dict['MONGO_USER'], conf_dict['MONGO_PASSWD'])
        coll = db[conf_dict['MONGO_COLL']]
        coll.bulk_write(opers)
    except Exception as e:
        print(repr(e))
    finally:
        if client:
            client.close()


def find(opers):
    """
    upsert_many 文档集合
    :param opers:
    """
    client = None
    try:
        client = pymongo.MongoClient(conf_dict['MONGO_HOST'], conf_dict['MONGO_PORT'])
        db = client[conf_dict['MONGO_DB']]
        if conf_dict['MONGO_USER'] and conf_dict['MONGO_PASSWD']:
            db.authenticate(conf_dict['MONGO_USER'], conf_dict['MONGO_PASSWD'])
        coll = db[conf_dict['MONGO_COLL']]
        aa = coll.find(opers)
        for a in aa:
            print a
    except Exception as e:
        print(repr(e))
    finally:
        if client:
            client.close()

def make_upsert_opers(uids):
    return [UpdateMany({"uid": uid[0]}, {'$set': {'devices': uid[1], 'createTime': arrow.now().timestamp * 1000}}, upsert=True)
            for uid in uids]


opers = [UpdateOne({"uid": 2069631861000}, {'$set': {'devices': ["073ed5b332dafca1121c986c8fe500d8", "73ad0c7cf206b8356b0d381bb684b009"] }}, upsert=True)
        ,UpdateOne({"uid": 2069635269000}, {'$set': {'devices': ["c3d3783f7e27bfe6a112e2e2e98a2777", "99afff309e3e8d8b64046ae6c7f0a926"] }}, upsert=True)
        ,UpdateOne({"uid": 206916004600},  {'$set': {'devices': ["f3e3111b79e36d248a9fc750ee0556a5", "d68bd26b9598e6d8a25358cb6a7ac3ff"] }}, upsert=True)
]
upsert_oper_many(opers)

opers = [UpdateMany({"createTime": { "$exists": False }}, {'$set': {'createTime': arrow.now().timestamp * 1000 }}, upsert=False)]
upsert_oper_many(opers)


opers = {"uid": 206916004600}
opers = {"createTime": { "$exists": False }}
find(opers)

print 'done'
